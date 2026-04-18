"""
Consolida el estado actual del tracker en un solo JSON que el dashboard consume.
Output: dashboard/data/latest.json (tambien copia con timestamp en data/)

Estructura:
{
  "generated_at": "...",
  "national_snapshot": {...},    # presidencial mas reciente
  "actas_snapshot": {...},
  "candidatos": [...],            # desfase por candidato
  "by_depto": {...},              # {depto_id: {agrup_id: votos}}
  "depto_names": {...},           # {depto_id: nombre}
  "anomalies": [...],             # todas las del snapshot actual
  "historical_desfase": [...],    # evolucion temporal si hay mas de 1 snapshot
  "integrity_hash": "sha256..."   # hash de los campos estructurales
}
"""
import hashlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn

ROOT = Path(__file__).parent
OUT_DASHBOARD = ROOT / "dashboard" / "data" / "latest.json"
OUT_ARCHIVE = ROOT / "data"
ID_ELECCION = 10

DEPTO_NAMES = {
    10000: "AMAZONAS", 20000: "ANCASH", 30000: "APURIMAC", 40000: "AREQUIPA",
    50000: "AYACUCHO", 60000: "CAJAMARCA", 70000: "CALLAO", 80000: "CUSCO",
    90000: "HUANCAVELICA", 100000: "HUANUCO", 110000: "ICA", 120000: "JUNIN",
    130000: "LA LIBERTAD", 140000: "LAMBAYEQUE", 150000: "LIMA", 160000: "LORETO",
    170000: "MADRE DE DIOS", 180000: "MOQUEGUA", 190000: "PASCO", 200000: "PIURA",
    210000: "PUNO", 220000: "SAN MARTIN", 230000: "TACNA", 240000: "TUMBES",
    250000: "UCAYALI", 990000: "EXTRANJERO",
}


def main():
    conn = get_conn()

    # Nacional mas reciente
    nat = conn.execute(
        "SELECT id, captured_at, actas_contabilizadas_pct, contabilizadas, total_actas, "
        "participacion_ciudadana_pct, total_votos_emitidos "
        "FROM snapshots WHERE tipo='presidencial' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not nat:
        print("ERROR: no hay snapshot presidencial"); sys.exit(1)
    nat_sid, nat_captured, nat_pct, nat_contab, nat_total, nat_part, nat_emit = nat

    cands = list(conn.execute(
        "SELECT codigo_agrupacion, nombre_agrupacion, nombre_candidato, "
        "total_votos_validos, pct_votos_validos "
        "FROM candidates WHERE snapshot_id=? ORDER BY total_votos_validos DESC",
        (nat_sid,)
    ))
    nat_by_cod = {c[0]: {"agrupacion": c[1], "candidato": c[2], "votos": c[3], "pct": c[4]} for c in cands}

    # Mejor snapshot completo: actas_ok >= 10000 (smoke tests excluidos),
    # mas reciente primero en caso de empate de tamanio
    act = conn.execute(
        "SELECT id, captured_at, modo, actas_ok, source, source_sha256 FROM actas_snapshots "
        "WHERE id_eleccion=? AND actas_ok >= 10000 "
        "ORDER BY captured_at DESC LIMIT 1", (ID_ELECCION,)
    ).fetchone()
    if not act:
        # Fallback: cualquier snapshot
        act = conn.execute(
            "SELECT id, captured_at, modo, actas_ok, source, source_sha256 FROM actas_snapshots "
            "WHERE id_eleccion=? ORDER BY actas_ok DESC LIMIT 1", (ID_ELECCION,)
        ).fetchone()
    if not act:
        print("ERROR: no hay actas_snapshot"); sys.exit(1)
    act_sid, act_captured, act_modo, act_ok, act_source, act_sha256 = act

    # Suma mesas por candidato
    mesas = {r[0]: r[1] for r in conn.execute(
        "SELECT codigo_agrupacion, SUM(votos) FROM acta_votos "
        "WHERE snapshot_id=? AND id_eleccion=? GROUP BY codigo_agrupacion",
        (act_sid, ID_ELECCION)
    )}

    # Candidatos presentes en ambos
    present = [c[0] for c in cands if mesas.get(c[0], 0) > 0]
    total_mesas = sum(mesas[c] for c in present)

    cmp_rows = []
    for cod in present:
        nv = (nat_by_cod.get(cod) or {}).get("votos", 0) or 0
        mv = mesas.get(cod, 0) or 0
        share = mv / total_mesas * 100 if total_mesas else 0
        cmp_rows.append({
            "codigo_agrupacion": cod,
            "agrupacion": nat_by_cod[cod]["agrupacion"],
            "candidato": nat_by_cod[cod]["candidato"],
            "nacional": nv,
            "mesas": mv,
            "desfase": nv - mv,
            "share_pct": round(share, 3),
        })
    total_abs = sum(abs(r["desfase"]) for r in cmp_rows)
    for r in cmp_rows:
        r["pct_del_desfase"] = round(abs(r["desfase"]) / total_abs * 100, 3) if total_abs else 0
        r["ratio"] = round(r["pct_del_desfase"] / r["share_pct"], 3) if r["share_pct"] else 0
    cmp_rows.sort(key=lambda r: abs(r["desfase"]), reverse=True)

    # Breakdown por depto
    depto_raw = list(conn.execute(
        "SELECT a.id_ubigeo_departamento, v.codigo_agrupacion, SUM(v.votos) "
        "FROM acta_votos v JOIN actas a ON a.snapshot_id=v.snapshot_id "
        "  AND a.codigo=v.codigo AND a.id_eleccion=v.id_eleccion "
        "WHERE v.snapshot_id=? AND v.id_eleccion=? "
        "GROUP BY a.id_ubigeo_departamento, v.codigo_agrupacion",
        (act_sid, ID_ELECCION)
    ))
    by_depto: dict[int, dict[int, int]] = defaultdict(dict)
    for depto, cod, votos in depto_raw:
        by_depto[depto or 0][cod] = votos

    # Anomalias actuales
    anomalies = [
        {
            "id": r[0], "detected_at": r[1], "tipo": r[2], "codigo": r[3],
            "codigo_agrupacion": r[4], "severity": r[5],
            "detalle": json.loads(r[6]) if r[6] else None,
        }
        for r in conn.execute(
            "SELECT id, detected_at, tipo, codigo, codigo_agrupacion, severity, detalle_json "
            "FROM anomalies WHERE snapshot_id=? ORDER BY severity DESC, id DESC",
            (act_sid,)
        )
    ]

    # Tendencia historica: desfase nacional (tracker) evolucion en el tiempo
    hist = list(conn.execute(
        "SELECT captured_at, actas_contabilizadas_pct FROM snapshots "
        "WHERE tipo='presidencial' ORDER BY captured_at"
    ))
    historical_pct = [{"captured_at": r[0], "actas_pct": r[1]} for r in hist]

    totals = conn.execute(
        "SELECT COUNT(*), SUM(total_votos_validos), SUM(total_votos_emitidos), "
        "SUM(CASE WHEN estado='contabilizada' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN estado='jee' THEN 1 ELSE 0 END) "
        "FROM actas WHERE snapshot_id=? AND id_eleccion=?",
        (act_sid, ID_ELECCION)
    ).fetchone()

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "national_snapshot": {
            "id": nat_sid, "captured_at": nat_captured, "actas_pct": nat_pct,
            "contabilizadas": nat_contab, "total_actas": nat_total,
            "participacion_pct": nat_part, "total_votos_emitidos": nat_emit,
        },
        "actas_snapshot": {
            "id": act_sid, "captured_at": act_captured, "modo": act_modo,
            "source": act_source, "source_sha256": act_sha256,
            "n_actas_ok": act_ok,
            "n_actas_en_db": totals[0], "sum_validos": totals[1],
            "sum_emitidos": totals[2], "n_contabilizadas": totals[3], "n_jee": totals[4],
        },
        "sources_all": [
            {"source": r[0], "snapshot_id": r[1], "captured_at": r[2], "actas_ok": r[3]}
            for r in conn.execute(
                "SELECT source, MAX(id), MAX(captured_at), MAX(actas_ok) FROM actas_snapshots "
                "WHERE id_eleccion=? AND source IS NOT NULL GROUP BY source",
                (ID_ELECCION,),
            )
        ],
        "candidatos": cmp_rows,
        "total_abs_desfase": total_abs,
        "by_depto": {str(k): v for k, v in by_depto.items()},
        "depto_names": {str(k): v for k, v in DEPTO_NAMES.items()},
        "anomalies": anomalies,
        "historical_national_pct": historical_pct,
    }

    # Integrity hash sobre los campos criticos
    integrity_source = json.dumps({
        "nat_sid": nat_sid, "act_sid": act_sid,
        "candidatos": cmp_rows,
        "by_depto": payload["by_depto"],
    }, sort_keys=True, ensure_ascii=False)
    payload["integrity_sha256"] = hashlib.sha256(integrity_source.encode()).hexdigest()

    # Escribir destino + archivo historico
    OUT_DASHBOARD.parent.mkdir(parents=True, exist_ok=True)
    OUT_DASHBOARD.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"[out] {OUT_DASHBOARD}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive = OUT_ARCHIVE / f"dashboard_snapshot_{ts}.json"
    archive.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"[archive] {archive}")

    print(f"\n  integrity_sha256: {payload['integrity_sha256'][:16]}...")
    print(f"  anomalias: {len(anomalies)}")
    print(f"  candidatos con desfase: {len(cmp_rows)}")

    conn.close()


if __name__ == "__main__":
    main()
