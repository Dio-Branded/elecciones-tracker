"""
Detector de anomalias sobre el snapshot de actas mas reciente.
Registra cada hallazgo en la tabla `anomalies` (idempotente por tipo+codigo+candidato+snapshot).

Reglas implementadas:
  1. sum_mismatch — SUM(votos_candidatos) + blancos + nulos != total_votos_emitidos (por acta)
  2. electores_exceeded — total_votos_emitidos > electores_habiles (por acta)
  3. disproportionate_delta — candidato con ratio desfase/share > 2.0x a nivel agregado
  4. vote_change — [historical] una acta ya 'contabilizada' cambio su conteo entre snapshots
  5. missing_acta — [historical] acta que existia en snapshot previo ya no aparece
  6. outlier_local — mesa con votos de un partido >= Nσ respecto a su colegio (surge/drop)

Uso:
  python anomalies.py                # solo snapshot actual
  python anomalies.py --historical   # tambien reglas 4 y 5

Salida: data/anomalies_report_YYYYMMDD_HHMMSS.json + stdout resumen
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn, insert_anomaly

OUT_DIR = Path(__file__).parent / "data"
ELECCION_PRESIDENCIAL = 10
RATIO_DISPROPORTIONATE_THRESHOLD = 2.0
SUM_MISMATCH_TOLERANCE = 5  # votos

# outlier_local tuning
OUTLIER_MIN_MESAS_POR_LOCAL = 5      # colegio debe tener >=5 mesas para computar stats
OUTLIER_SIGMA_SURGE = 5.0            # umbral de sigmas para flaggear "surge" (valor alto)
OUTLIER_SIGMA_DROP = 5.0             # idem para "drop" (valor bajo)
OUTLIER_MIN_VOTES_SURGE = 10         # piso absoluto: surge debe tener >= N votos (ignora ruido)
OUTLIER_MIN_MEAN_DROP = 10           # para flaggear drop, la media local debe ser >= N (partido relevante)


def latest_actas_snapshot(conn, eid: int):
    return conn.execute(
        "SELECT id, captured_at, modo FROM actas_snapshots "
        "WHERE id_eleccion=? ORDER BY id DESC LIMIT 1",
        (eid,),
    ).fetchone()


def previous_actas_snapshot(conn, eid: int, before_id: int):
    return conn.execute(
        "SELECT id, captured_at FROM actas_snapshots "
        "WHERE id_eleccion=? AND id<? ORDER BY id DESC LIMIT 1",
        (eid, before_id),
    ).fetchone()


def detect_sum_mismatch_and_electores(conn, snap_id: int, eid: int) -> list[dict]:
    """Actas donde sum(votos_candidatos) + blancos + nulos != emitidos, o emitidos > electores."""
    rows = list(conn.execute(
        """
        SELECT a.codigo, a.total_votos_emitidos, a.total_votos_validos,
               a.electores_habiles, a.votos_blancos, a.votos_nulos,
               COALESCE((SELECT SUM(v.votos) FROM acta_votos v
                         WHERE v.snapshot_id=a.snapshot_id AND v.codigo=a.codigo
                         AND v.id_eleccion=a.id_eleccion), 0) AS sum_cand_votos
        FROM actas a
        WHERE a.snapshot_id=? AND a.id_eleccion=?
        """,
        (snap_id, eid),
    ))
    findings = []
    for codigo, emit, validos, elect, blanc, nul, sum_cand in rows:
        emit = emit or 0; validos = validos or 0; elect = elect or 0
        blanc = blanc or 0; nul = nul or 0; sum_cand = sum_cand or 0
        # Regla 1: suma interna
        #   El CSV de PRIME tiene solo 7 candidatos, entonces sum_cand < validos es esperado
        #   pero validos debe ser consistente con emitidos: validos + blancos + nulos = emitidos
        expected = validos + blanc + nul
        if emit > 0 and abs(expected - emit) > SUM_MISMATCH_TOLERANCE:
            findings.append({
                "tipo": "sum_mismatch",
                "codigo": codigo,
                "detalle": {
                    "validos": validos, "blancos": blanc, "nulos": nul,
                    "sum_esperado": expected, "emitidos": emit,
                    "diff": expected - emit,
                },
                "severity": 2,
            })
        # Regla 2: emitidos > electores
        if elect > 0 and emit > elect:
            findings.append({
                "tipo": "electores_exceeded",
                "codigo": codigo,
                "detalle": {
                    "emitidos": emit, "electores_habiles": elect, "exceso": emit - elect,
                },
                "severity": 3,
            })
    return findings


def detect_disproportionate_delta(conn, snap_id: int, eid: int, tipo_nac: str) -> list[dict]:
    """Agregado: un candidato con ratio desfase/share > THRESHOLD."""
    # Nacional ultimo snapshot
    nat_row = conn.execute(
        "SELECT id FROM snapshots WHERE tipo=? ORDER BY id DESC LIMIT 1",
        (tipo_nac,),
    ).fetchone()
    if not nat_row:
        return []
    nat_sid = nat_row[0]
    nat = {r[0]: r[1] for r in conn.execute(
        "SELECT codigo_agrupacion, total_votos_validos FROM candidates WHERE snapshot_id=?",
        (nat_sid,),
    )}
    mesas = {r[0]: r[1] for r in conn.execute(
        "SELECT codigo_agrupacion, SUM(votos) FROM acta_votos "
        "WHERE snapshot_id=? AND id_eleccion=? GROUP BY codigo_agrupacion",
        (snap_id, eid),
    )}
    # Solo candidatos con mesas>0 y nacional>0
    present = [c for c in nat if nat.get(c, 0) > 0 and mesas.get(c, 0) > 0]
    total_mesas = sum(mesas[c] for c in present)
    total_abs_desfase = sum(abs((nat[c] or 0) - (mesas.get(c, 0) or 0)) for c in present)
    findings = []
    for c in present:
        nat_v = nat[c]; mes_v = mesas.get(c, 0) or 0
        desfase = nat_v - mes_v
        share = mes_v / total_mesas * 100 if total_mesas else 0
        pct_desfase = abs(desfase) / total_abs_desfase * 100 if total_abs_desfase else 0
        ratio = pct_desfase / share if share else 0
        if ratio >= RATIO_DISPROPORTIONATE_THRESHOLD:
            findings.append({
                "tipo": "disproportionate_delta",
                "codigo": None,
                "codigo_agrupacion": c,
                "detalle": {
                    "nacional": nat_v, "mesas": mes_v, "desfase": desfase,
                    "share_pct": round(share, 3), "pct_del_desfase": round(pct_desfase, 3),
                    "ratio": round(ratio, 3),
                    "nacional_snapshot_id": nat_sid,
                },
                "severity": 3,
            })
    return findings


def detect_vote_changes(conn, curr_sid: int, prev_sid: int, eid: int) -> list[dict]:
    """Actas cuyo total_votos_validos cambio entre snapshots consecutivos."""
    rows = list(conn.execute(
        """
        SELECT curr.codigo, curr.total_votos_validos, prev.total_votos_validos
        FROM actas curr
        JOIN actas prev ON prev.codigo=curr.codigo AND prev.id_eleccion=curr.id_eleccion
        WHERE curr.snapshot_id=? AND prev.snapshot_id=? AND curr.id_eleccion=?
          AND curr.total_votos_validos != prev.total_votos_validos
          AND curr.estado='contabilizada' AND prev.estado='contabilizada'
        """,
        (curr_sid, prev_sid, eid),
    ))
    findings = []
    for codigo, curr_v, prev_v in rows:
        findings.append({
            "tipo": "vote_change",
            "codigo": codigo,
            "detalle": {"prev": prev_v, "current": curr_v, "diff": curr_v - prev_v,
                        "prev_snapshot_id": prev_sid},
            "severity": 3,
        })
    return findings


def detect_missing_actas(conn, curr_sid: int, prev_sid: int, eid: int) -> list[dict]:
    """Actas que existian en prev pero no en curr."""
    rows = list(conn.execute(
        """
        SELECT prev.codigo
        FROM actas prev
        LEFT JOIN actas curr ON curr.codigo=prev.codigo AND curr.id_eleccion=prev.id_eleccion
                             AND curr.snapshot_id=?
        WHERE prev.snapshot_id=? AND prev.id_eleccion=? AND curr.codigo IS NULL
        """,
        (curr_sid, prev_sid, eid),
    ))
    return [{"tipo": "missing", "codigo": r[0],
             "detalle": {"prev_snapshot_id": prev_sid}, "severity": 2}
            for r in rows]


def detect_outlier_local(conn, snap_id: int, eid: int) -> list[dict]:
    """Por colegio de votacion (codigoLocalVotacion en raw_json), para cada partido,
    computa media + desviacion tipica de los votos entre las mesas del colegio.
    Flaggea mesas donde un partido tiene:
      * surge: votos >= mean + OUTLIER_SIGMA_SURGE*std  Y  votos >= OUTLIER_MIN_VOTES_SURGE
      * drop:  mean >= OUTLIER_MIN_MEAN_DROP  Y  (mean - votos) >= OUTLIER_SIGMA_DROP*std

    Requiere que el colegio tenga >= OUTLIER_MIN_MESAS_POR_LOCAL mesas contabilizadas.
    """
    # 1. Mapear cada mesa a su codigoLocalVotacion
    mesa_local = {}
    for codigo, local in conn.execute(
        """
        SELECT a.codigo, json_extract(a.raw_json, '$.codigoLocalVotacion')
        FROM actas a
        WHERE a.snapshot_id=? AND a.id_eleccion=? AND a.estado='contabilizada'
          AND a.raw_json IS NOT NULL
        """,
        (snap_id, eid),
    ):
        if local:
            mesa_local[codigo] = str(local)

    if not mesa_local:
        return []

    # 2. Traer votos por (mesa, agrupacion) para todas las mesas relevantes
    # Usamos un solo query y agrupamos en python por local
    local_to_mesas = defaultdict(list)
    for codigo, local in mesa_local.items():
        local_to_mesas[local].append(codigo)

    # Filter locales con suficientes mesas
    locales_validos = {l: ms for l, ms in local_to_mesas.items()
                       if len(ms) >= OUTLIER_MIN_MESAS_POR_LOCAL}
    if not locales_validos:
        return []

    mesas_relevantes = {c for ms in locales_validos.values() for c in ms}

    # Votos por mesa x agrupacion (solo presidencial)
    votos_mesa_agrup = defaultdict(dict)  # {codigo: {agrup: votos}}
    for codigo, agrup, votos in conn.execute(
        "SELECT codigo, codigo_agrupacion, votos FROM acta_votos "
        "WHERE snapshot_id=? AND id_eleccion=?",
        (snap_id, eid),
    ):
        if codigo in mesas_relevantes:
            votos_mesa_agrup[codigo][agrup] = votos or 0

    findings = []
    for local, mesas in locales_validos.items():
        # Conjunto de agrupaciones presentes en este colegio
        agrup_set = set()
        for m in mesas:
            agrup_set.update(votos_mesa_agrup.get(m, {}).keys())

        for agrup in agrup_set:
            values = [votos_mesa_agrup.get(m, {}).get(agrup, 0) for m in mesas]
            n = len(values)
            if n < OUTLIER_MIN_MESAS_POR_LOCAL:
                continue
            total = sum(values)
            total_sq = sum(v * v for v in values)

            for m, v in zip(mesas, values):
                # Leave-one-out: stats del colegio excluyendo la propia mesa.
                # Evita que un outlier contamine su propia referencia.
                n_o = n - 1
                mean = (total - v) / n_o
                var = (total_sq - v * v) / n_o - mean * mean
                std = var ** 0.5 if var > 0 else 0
                # Piso minimo para std (evita z-scores explotando con std≈0)
                std_eff = max(std, 1.0)

                # SURGE
                if v >= OUTLIER_MIN_VOTES_SURGE:
                    z_surge = (v - mean) / std_eff
                    if z_surge >= OUTLIER_SIGMA_SURGE:
                        findings.append({
                            "tipo": "outlier_local",
                            "codigo": m,
                            "codigo_agrupacion": agrup,
                            "detalle": {
                                "subtipo": "surge",
                                "codigo_local_votacion": local,
                                "n_mesas_local": n,
                                "votos_mesa": v,
                                "media_local": round(mean, 2),
                                "std_local": round(std, 2),
                                "z_score": round(z_surge, 2),
                            },
                            "severity": 3,
                        })
                # DROP
                if mean >= OUTLIER_MIN_MEAN_DROP and v < mean:
                    z_drop = (mean - v) / std_eff
                    if z_drop >= OUTLIER_SIGMA_DROP:
                        findings.append({
                            "tipo": "outlier_local",
                            "codigo": m,
                            "codigo_agrupacion": agrup,
                            "detalle": {
                                "subtipo": "drop",
                                "codigo_local_votacion": local,
                                "n_mesas_local": n,
                                "votos_mesa": v,
                                "media_local": round(mean, 2),
                                "std_local": round(std, 2),
                                "z_score": round(z_drop, 2),
                            },
                            "severity": 3,
                        })
    return findings


def persist_findings(conn, snap_id: int, findings: list[dict]):
    for f in findings:
        insert_anomaly(
            conn, f["tipo"],
            snapshot_id=snap_id,
            codigo=f.get("codigo"),
            id_eleccion=ELECCION_PRESIDENCIAL,
            codigo_agrupacion=f.get("codigo_agrupacion"),
            detalle=f.get("detalle"),
            severity=f.get("severity", 2),
        )
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--historical", action="store_true",
                    help="Correr reglas que comparan vs snapshot previo")
    ap.add_argument("--eleccion", type=int, default=ELECCION_PRESIDENCIAL)
    ap.add_argument("--snapshot-id", type=int, default=None,
                    help="Forzar un snapshot_id especifico (default: ultimo full con >=10000 actas)")
    args = ap.parse_args()

    conn = get_conn()
    if args.snapshot_id:
        row = conn.execute(
            "SELECT id, captured_at, modo FROM actas_snapshots WHERE id=? AND id_eleccion=?",
            (args.snapshot_id, args.eleccion),
        ).fetchone()
        actas_row = row
    else:
        # Preferir el ultimo snapshot full con >=10000 actas; caer al latest si no hay
        actas_row = conn.execute(
            "SELECT id, captured_at, modo FROM actas_snapshots "
            "WHERE id_eleccion=? AND actas_ok >= 10000 "
            "ORDER BY id DESC LIMIT 1",
            (args.eleccion,),
        ).fetchone()
        if not actas_row:
            actas_row = latest_actas_snapshot(conn, args.eleccion)
    if not actas_row:
        print("No hay snapshots de actas")
        sys.exit(1)
    snap_id, captured_at, modo = actas_row
    print(f"[actas_snap] id={snap_id} captured_at={captured_at} modo={modo}")

    all_findings = []

    print("\n[1] Detectando sum_mismatch + electores_exceeded...")
    f1 = detect_sum_mismatch_and_electores(conn, snap_id, args.eleccion)
    print(f"     {len(f1)} hallazgos")
    all_findings.extend(f1)

    print("[2] Detectando disproportionate_delta (agregado vs nacional)...")
    tipo_nac = {10: "presidencial", 12: "senadores_nacional", 15: "parlamento_andino"}.get(args.eleccion, "presidencial")
    f2 = detect_disproportionate_delta(conn, snap_id, args.eleccion, tipo_nac)
    print(f"     {len(f2)} hallazgos")
    all_findings.extend(f2)

    print("[3] Detectando outlier_local (votos anomalos vs colegio)...")
    f_ol = detect_outlier_local(conn, snap_id, args.eleccion)
    print(f"     {len(f_ol)} hallazgos")
    all_findings.extend(f_ol)

    if args.historical:
        prev_row = previous_actas_snapshot(conn, args.eleccion, snap_id)
        if prev_row:
            prev_sid, prev_captured = prev_row
            print(f"[4] Detectando vote_change vs snapshot id={prev_sid} ({prev_captured})...")
            f3 = detect_vote_changes(conn, snap_id, prev_sid, args.eleccion)
            print(f"     {len(f3)} hallazgos")
            all_findings.extend(f3)

            print(f"[5] Detectando missing vs snapshot id={prev_sid}...")
            f4 = detect_missing_actas(conn, snap_id, prev_sid, args.eleccion)
            print(f"     {len(f4)} hallazgos")
            all_findings.extend(f4)
        else:
            print("[hist] no hay snapshot previo — skip reglas historicas")

    persist_findings(conn, snap_id, all_findings)

    # Resumen por severity
    by_sev = defaultdict(int); by_tipo = defaultdict(int)
    for f in all_findings:
        by_sev[f.get("severity", 2)] += 1
        by_tipo[f["tipo"]] += 1

    print(f"\n=== RESUMEN ANOMALIAS ===")
    print(f"  Total:    {len(all_findings)}")
    print(f"  Por tipo:     {dict(by_tipo)}")
    print(f"  Por severity: {dict(sorted(by_sev.items(), reverse=True))}")

    # Top 10 criticos
    critical = [f for f in all_findings if f.get("severity", 2) >= 3]
    if critical:
        print(f"\n--- TOP {min(10, len(critical))} CRITICOS ---")
        for f in critical[:10]:
            print(f"  [{f['tipo']}] codigo={f.get('codigo')} {f.get('detalle')}")

    # Guardar JSON
    OUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"anomalies_report_{ts}.json"
    out.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snap_id,
        "snapshot_captured_at": captured_at,
        "by_tipo": dict(by_tipo),
        "by_severity": {str(k): v for k, v in by_sev.items()},
        "findings": all_findings,
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n[out] {out}")

    conn.close()


if __name__ == "__main__":
    main()
