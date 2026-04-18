"""
Analiza el ultimo snapshot de actas vs el snapshot nacional mas reciente.

Reporta:
  1. Suma mesa-nivel por candidato vs total_votos_validos nacional
  2. Desfase absoluto + ratio (%desfase / %voto)
  3. Breakdown por departamento
  4. Tendencia historica (si hay >1 actas_snapshot)
  5. Guarda reporte JSON en data/actas_analysis_YYYYMMDD_HHMMSS.json

Uso:
  python analyze_actas.py                    # ultimo snapshot
  python analyze_actas.py --snapshot 1       # snapshot especifico
  python analyze_actas.py --eleccion 10      # id_eleccion (default 10)
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn

OUT_DIR = Path(__file__).parent / "data"
DEPTO_NAMES = {
    10000: "AMAZONAS", 20000: "ANCASH", 30000: "APURIMAC", 40000: "AREQUIPA",
    50000: "AYACUCHO", 60000: "CAJAMARCA", 70000: "CALLAO", 80000: "CUSCO",
    90000: "HUANCAVELICA", 100000: "HUANUCO", 110000: "ICA", 120000: "JUNIN",
    130000: "LA LIBERTAD", 140000: "LAMBAYEQUE", 150000: "LIMA", 160000: "LORETO",
    170000: "MADRE DE DIOS", 180000: "MOQUEGUA", 190000: "PASCO", 200000: "PIURA",
    210000: "PUNO", 220000: "SAN MARTIN", 230000: "TACNA", 240000: "TUMBES",
    250000: "UCAYALI", 990000: "EXTRANJERO",
}


def latest_actas_snapshot(conn, eid: int):
    row = conn.execute(
        "SELECT id, captured_at, modo, actas_ok FROM actas_snapshots "
        "WHERE id_eleccion=? ORDER BY id DESC LIMIT 1",
        (eid,),
    ).fetchone()
    return row  # (id, captured_at, modo, actas_ok) o None


def latest_national_snapshot(conn, tipo: str):
    row = conn.execute(
        "SELECT id, captured_at, actas_contabilizadas_pct FROM snapshots "
        "WHERE tipo=? ORDER BY id DESC LIMIT 1",
        (tipo,),
    ).fetchone()
    return row


def candidates_national(conn, snapshot_id: int):
    return list(conn.execute(
        "SELECT codigo_agrupacion, nombre_agrupacion, nombre_candidato, "
        "total_votos_validos, pct_votos_validos "
        "FROM candidates WHERE snapshot_id=? "
        "ORDER BY total_votos_validos DESC",
        (snapshot_id,),
    ))


def sum_mesas_per_candidate(conn, actas_snap_id: int, eid: int):
    return list(conn.execute(
        "SELECT codigo_agrupacion, SUM(votos), COUNT(*) "
        "FROM acta_votos "
        "WHERE snapshot_id=? AND id_eleccion=? "
        "GROUP BY codigo_agrupacion "
        "ORDER BY 2 DESC",
        (actas_snap_id, eid),
    ))


def sum_mesas_per_depto_candidate(conn, actas_snap_id: int, eid: int):
    return list(conn.execute(
        "SELECT a.id_ubigeo_departamento, v.codigo_agrupacion, SUM(v.votos) "
        "FROM acta_votos v "
        "JOIN actas a ON a.snapshot_id=v.snapshot_id AND a.codigo=v.codigo AND a.id_eleccion=v.id_eleccion "
        "WHERE v.snapshot_id=? AND v.id_eleccion=? "
        "GROUP BY a.id_ubigeo_departamento, v.codigo_agrupacion "
        "ORDER BY a.id_ubigeo_departamento, 3 DESC",
        (actas_snap_id, eid),
    ))


def actas_totals(conn, actas_snap_id: int, eid: int):
    row = conn.execute(
        "SELECT COUNT(*), SUM(total_votos_validos), SUM(total_votos_emitidos), "
        "SUM(electores_habiles), SUM(votos_blancos), SUM(votos_nulos), "
        "SUM(CASE WHEN estado='contabilizada' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN estado='jee' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN estado='pendiente' THEN 1 ELSE 0 END) "
        "FROM actas WHERE snapshot_id=? AND id_eleccion=?",
        (actas_snap_id, eid),
    ).fetchone()
    return {
        "n_actas": row[0], "sum_validos": row[1], "sum_emitidos": row[2],
        "sum_electores": row[3], "sum_blancos": row[4], "sum_nulos": row[5],
        "n_contabilizadas": row[6], "n_jee": row[7], "n_pendientes": row[8],
    }


def format_table_candidatos(cmp_rows, header: str) -> str:
    lines = [header, "=" * 110]
    lines.append(f"{'CANDIDATO':<40} {'NACIONAL':>12} {'MESAS':>12} {'DESFASE':>10} {'SHARE':>7} {'RATIO':>7}")
    lines.append("-" * 110)
    for r in cmp_rows:
        lines.append(
            f"{(r['candidato'] or r['agrupacion'] or '')[:38]:<40} "
            f"{r['nacional']:>12,} "
            f"{r['mesas']:>12,} "
            f"{r['desfase']:>+10,} "
            f"{r['share_pct']:>6.2f}% "
            f"{r['ratio']:>6.2f}x"
        )
    return "\n".join(lines)


def format_table_deptos(depto_rows, top_desfase_codigos: list, codigo_to_name: dict) -> str:
    lines = ["", "=" * 110, "DESGLOSE POR DEPARTAMENTO — top 3 candidatos con mayor desfase (en absoluto)", "=" * 110]
    hdr = f"{'DEPARTAMENTO':<22}"
    for codigo in top_desfase_codigos:
        nombre = (codigo_to_name.get(codigo, str(codigo)) or "")[:12]
        hdr += f" {nombre:>12}"
    lines.append(hdr)
    lines.append("-" * len(hdr))
    for depto_id, by_candidato in depto_rows.items():
        name = DEPTO_NAMES.get(depto_id, f"ubigeo={depto_id}")[:20]
        row = f"{name:<22}"
        for codigo in top_desfase_codigos:
            v = by_candidato.get(codigo, 0)
            row += f" {v:>12,}"
        lines.append(row)
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=int, default=None)
    ap.add_argument("--eleccion", type=int, default=10)
    args = ap.parse_args()

    conn = get_conn()

    if args.snapshot:
        row = conn.execute(
            "SELECT id, captured_at, modo, actas_ok FROM actas_snapshots WHERE id=?",
            (args.snapshot,),
        ).fetchone()
    else:
        row = latest_actas_snapshot(conn, args.eleccion)
    if not row:
        print(f"No hay snapshots de actas para id_eleccion={args.eleccion}")
        sys.exit(1)
    actas_sid, actas_captured, actas_modo, actas_ok = row
    print(f"[actas]  snapshot_id={actas_sid}  captured_at={actas_captured}  modo={actas_modo}  n={actas_ok:,}")

    tipo_map = {10: "presidencial", 12: "senadores_nacional", 15: "parlamento_andino"}
    tipo = tipo_map.get(args.eleccion, "presidencial")
    nat = latest_national_snapshot(conn, tipo)
    if not nat:
        print(f"No hay snapshot nacional tipo={tipo}")
        sys.exit(1)
    nat_sid, nat_captured, nat_pct = nat
    print(f"[nac]    snapshot_id={nat_sid}  captured_at={nat_captured}  actas={nat_pct}%")

    # Totales de actas
    totals = actas_totals(conn, actas_sid, args.eleccion)
    print(f"\n[totals] actas_count={totals['n_actas']:,}  validos={totals['sum_validos']:,}  "
          f"emitidos={totals['sum_emitidos']:,}  contabilizadas={totals['n_contabilizadas']:,}  "
          f"jee={totals['n_jee']:,}  pendientes={totals['n_pendientes']:,}")

    # Candidatos nacional
    cands = candidates_national(conn, nat_sid)
    nat_by_cod = {c[0]: {"agrupacion": c[1], "candidato": c[2], "votos": c[3], "pct": c[4]} for c in cands}

    # Suma mesas por candidato
    mesas_sums = sum_mesas_per_candidate(conn, actas_sid, args.eleccion)
    mesas_by_cod = {m[0]: {"votos": m[1], "actas_con_voto": m[2]} for m in mesas_sums}

    # Candidatos presentes en AMBAS fuentes (nacional + mesas con voto > 0).
    # Esto coincide con el dataset que PRIME publico (top 7 candidatos).
    present_codigos = [
        c[0] for c in cands
        if (mesas_by_cod.get(c[0]) or {}).get("votos", 0) > 0
    ]
    total_mesas_sum_top = sum(
        (mesas_by_cod.get(c) or {}).get("votos", 0) or 0 for c in present_codigos
    )

    cmp_rows = []
    for codigo in present_codigos:
        nat_v = (nat_by_cod.get(codigo) or {}).get("votos", 0) or 0
        mes_v = (mesas_by_cod.get(codigo) or {}).get("votos", 0) or 0
        share = (mes_v / total_mesas_sum_top * 100) if total_mesas_sum_top else 0
        cmp_rows.append({
            "codigo_agrupacion": codigo,
            "agrupacion": (nat_by_cod.get(codigo) or {}).get("agrupacion"),
            "candidato": (nat_by_cod.get(codigo) or {}).get("candidato"),
            "nacional": nat_v,
            "mesas": mes_v,
            "desfase": nat_v - mes_v,
            "share_pct": share,
            "ratio": 0.0,
        })

    # Candidatos en nacional SIN data en mesas (info-only)
    missing_in_mesas = [
        {
            "codigo_agrupacion": c[0],
            "agrupacion": c[1],
            "candidato": c[2],
            "nacional": c[3],
            "mesas": 0,
        }
        for c in cands if (mesas_by_cod.get(c[0]) or {}).get("votos", 0) == 0 and c[3] > 0
    ]

    # Ratio relativo al desfase total del top-N
    total_abs_desfase = sum(abs(r["desfase"]) for r in cmp_rows)
    for r in cmp_rows:
        r["pct_del_desfase"] = (abs(r["desfase"]) / total_abs_desfase * 100) if total_abs_desfase else 0
        if r["share_pct"] > 0 and total_abs_desfase > 0:
            r["ratio"] = r["pct_del_desfase"] / r["share_pct"]
    cmp_rows.sort(key=lambda r: abs(r["desfase"]), reverse=True)

    display_rows = cmp_rows
    print("\n" + format_table_candidatos(display_rows, "COMPARATIVA ONPE NACIONAL vs SUMA MESA-A-MESA"))
    print(f"\nTotal desfase absoluto (sum |diff|): {total_abs_desfase:,} votos")
    print("Interpretacion: RATIO ~1.0 = proporcional al share. >1.5x = desproporcionado.")

    # Breakdown por departamento
    depto_rows_raw = sum_mesas_per_depto_candidate(conn, actas_sid, args.eleccion)
    depto_rows: dict[int, dict] = defaultdict(dict)
    for depto_id, cod_agrup, votos in depto_rows_raw:
        depto_rows[depto_id or 0][cod_agrup] = votos

    # top 5 candidatos por desfase absoluto para el breakdown
    top_codigos_depto = [r["codigo_agrupacion"] for r in cmp_rows[:5]]
    codigo_to_name = {
        r["codigo_agrupacion"]: (r["candidato"] or r["agrupacion"] or str(r["codigo_agrupacion"])).split()[-1]
        for r in cmp_rows
    }
    print(format_table_deptos(depto_rows, top_codigos_depto, codigo_to_name))

    # Historico: si hay >1 snapshot prime_csv o actas, comparar el mas reciente vs el anterior
    prev_rows = list(conn.execute(
        "SELECT id, captured_at FROM actas_snapshots WHERE id_eleccion=? AND id<? ORDER BY id DESC LIMIT 1",
        (args.eleccion, actas_sid),
    ))
    trend_text = ""
    if prev_rows:
        prev_sid, prev_captured = prev_rows[0]
        prev = dict(sum_mesas_per_candidate(conn, prev_sid, args.eleccion))
        prev = {k: v[0] if isinstance(v, tuple) else v for k, v in prev.items()}
        # re-fetch tuples as dict {codigo: total}
        prev = {m[0]: m[1] for m in sum_mesas_per_candidate(conn, prev_sid, args.eleccion)}
        trend_text = f"\n\nTENDENCIA vs snapshot anterior ({prev_captured}):"
        for r in cmp_rows[:5]:
            cod = r["codigo_agrupacion"]
            prev_v = prev.get(cod, 0) or 0
            delta = r["mesas"] - prev_v
            trend_text += f"\n  {(r['candidato'] or '')[:30]:<32} delta={delta:+,}"
    print(trend_text)

    # JSON out
    OUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"actas_analysis_{ts}.json"
    out_path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "actas_snapshot": {"id": actas_sid, "captured_at": actas_captured, "modo": actas_modo, "n": actas_ok},
        "national_snapshot": {"id": nat_sid, "captured_at": nat_captured, "actas_pct": nat_pct},
        "actas_totals": totals,
        "total_abs_desfase": total_abs_desfase,
        "candidatos": cmp_rows,
        "by_depto": {str(k): v for k, v in depto_rows.items()},
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n[out] {out_path}")

    conn.close()


if __name__ == "__main__":
    main()
