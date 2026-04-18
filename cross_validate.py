"""Compara el ultimo snapshot de cada source para detectar discrepancias
mesa-a-mesa. Si dos fuentes independientes difieren sobre la misma mesa,
es senal roja (tampering, error de sync, o data diff temporal).

Reglas:
  1. Diff por votos agregados por candidato (nivel macro)
  2. Mesas presentes en una fuente pero no en otra
  3. Mesas con votos distintos entre fuentes (nivel micro)

Genera anomalies tipo 'source_mismatch' con detalle_json comparativo.

Uso:
  python cross_validate.py                  # todos los pares de fuentes mas recientes
  python cross_validate.py --eleccion 10
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

from db import get_conn, insert_anomaly

OUT_DIR = Path(__file__).parent / "data"


def latest_snapshot_per_source(conn, id_eleccion: int) -> dict[str, int]:
    """Retorna {source: snapshot_id} del mas reciente por cada source."""
    rows = list(conn.execute(
        "SELECT source, MAX(id) FROM actas_snapshots "
        "WHERE id_eleccion=? AND source IS NOT NULL GROUP BY source",
        (id_eleccion,),
    ))
    return {source: sid for source, sid in rows if source}


def sum_by_agrupacion(conn, snap_id: int, id_eleccion: int) -> dict[int, int]:
    return {r[0]: r[1] for r in conn.execute(
        "SELECT codigo_agrupacion, SUM(votos) FROM acta_votos "
        "WHERE snapshot_id=? AND id_eleccion=? GROUP BY codigo_agrupacion",
        (snap_id, id_eleccion),
    )}


def actas_codes(conn, snap_id: int, id_eleccion: int) -> set[int]:
    return {r[0] for r in conn.execute(
        "SELECT codigo FROM actas WHERE snapshot_id=? AND id_eleccion=?",
        (snap_id, id_eleccion),
    )}


def compare_pair(conn, src_a: str, sid_a: int, src_b: str, sid_b: int, id_eleccion: int) -> dict:
    agg_a = sum_by_agrupacion(conn, sid_a, id_eleccion)
    agg_b = sum_by_agrupacion(conn, sid_b, id_eleccion)
    all_codigos = set(agg_a.keys()) | set(agg_b.keys())
    agg_diff = []
    for cod in sorted(all_codigos):
        va = agg_a.get(cod, 0); vb = agg_b.get(cod, 0)
        if va != vb:
            agg_diff.append({"codigo_agrupacion": cod, "a": va, "b": vb, "diff": va - vb})

    set_a = actas_codes(conn, sid_a, id_eleccion)
    set_b = actas_codes(conn, sid_b, id_eleccion)
    only_a = sorted(set_a - set_b)
    only_b = sorted(set_b - set_a)
    common = set_a & set_b

    # Para reducir carga, samplear diferencias mesa-a-mesa solo en los primeros
    # 5000 codigos comunes
    sample = sorted(common)[:5000]
    mesa_mismatches = []
    if sample:
        ph = ",".join("?" * len(sample))
        # Lee votos de ambos snapshots
        rows_a = dict(conn.execute(
            f"SELECT codigo, codigo_agrupacion||':'||votos "
            f"FROM acta_votos WHERE snapshot_id=? AND id_eleccion=? AND codigo IN ({ph})",
            (sid_a, id_eleccion, *sample),
        ))
        # Agrupar por codigo -> dict
        by_code_a: dict[int, dict[int, int]] = defaultdict(dict)
        for codigo, cod_agrup, votos in conn.execute(
            f"SELECT codigo, codigo_agrupacion, votos FROM acta_votos "
            f"WHERE snapshot_id=? AND id_eleccion=? AND codigo IN ({ph})",
            (sid_a, id_eleccion, *sample),
        ):
            by_code_a[codigo][cod_agrup] = votos
        by_code_b: dict[int, dict[int, int]] = defaultdict(dict)
        for codigo, cod_agrup, votos in conn.execute(
            f"SELECT codigo, codigo_agrupacion, votos FROM acta_votos "
            f"WHERE snapshot_id=? AND id_eleccion=? AND codigo IN ({ph})",
            (sid_b, id_eleccion, *sample),
        ):
            by_code_b[codigo][cod_agrup] = votos

        for codigo in sample:
            ma = by_code_a.get(codigo, {})
            mb = by_code_b.get(codigo, {})
            if ma != mb:
                diffs = {}
                for k in set(ma.keys()) | set(mb.keys()):
                    va = ma.get(k, 0); vb = mb.get(k, 0)
                    if va != vb:
                        diffs[k] = {"a": va, "b": vb}
                if diffs:
                    mesa_mismatches.append({"codigo": codigo, "diffs": diffs})
            if len(mesa_mismatches) >= 200:
                break

    return {
        "pair": {"a": src_a, "sid_a": sid_a, "b": src_b, "sid_b": sid_b},
        "agg_diff_count": len(agg_diff),
        "agg_diff": agg_diff[:20],
        "only_a_count": len(only_a),
        "only_b_count": len(only_b),
        "common_count": len(common),
        "mesa_mismatches_in_sample": len(mesa_mismatches),
        "mesa_mismatches_sample": mesa_mismatches[:10],
    }


def persist_anomalies(conn, pair_result: dict, id_eleccion: int):
    p = pair_result["pair"]
    # Agregado
    if pair_result["agg_diff_count"] > 0:
        insert_anomaly(
            conn, "source_mismatch", snapshot_id=p["sid_a"],
            id_eleccion=id_eleccion,
            detalle={"vs": p["b"], "vs_snapshot_id": p["sid_b"],
                      "agg_diff": pair_result["agg_diff"][:20],
                      "agg_diff_count": pair_result["agg_diff_count"]},
            severity=2,
        )
    # Mesas unicas a una fuente
    if pair_result["only_a_count"] > 0 or pair_result["only_b_count"] > 0:
        insert_anomaly(
            conn, "source_mismatch", snapshot_id=p["sid_a"],
            id_eleccion=id_eleccion,
            detalle={"vs": p["b"], "vs_snapshot_id": p["sid_b"],
                      "only_a_count": pair_result["only_a_count"],
                      "only_b_count": pair_result["only_b_count"]},
            severity=2,
        )
    # Mismatches mesa-nivel (critico si muchos)
    if pair_result["mesa_mismatches_in_sample"] > 0:
        severity = 3 if pair_result["mesa_mismatches_in_sample"] > 50 else 2
        insert_anomaly(
            conn, "source_mismatch", snapshot_id=p["sid_a"],
            id_eleccion=id_eleccion,
            detalle={"vs": p["b"], "vs_snapshot_id": p["sid_b"],
                      "mesa_mismatches_in_sample": pair_result["mesa_mismatches_in_sample"],
                      "sample": pair_result["mesa_mismatches_sample"][:5]},
            severity=severity,
        )
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eleccion", type=int, default=10)
    args = ap.parse_args()

    conn = get_conn()
    latest = latest_snapshot_per_source(conn, args.eleccion)
    if len(latest) < 2:
        print(f"Necesitas >=2 sources con snapshots para cross-validar. "
              f"Actuales: {list(latest.keys())}")
        sys.exit(0)

    print(f"[cross_validate] sources activas: {latest}")
    reports = []
    for (a, sid_a), (b, sid_b) in combinations(latest.items(), 2):
        print(f"\n=== {a} (snap={sid_a}) vs {b} (snap={sid_b}) ===")
        result = compare_pair(conn, a, sid_a, b, sid_b, args.eleccion)
        reports.append(result)
        print(f"  agg_diff: {result['agg_diff_count']} candidatos con totales distintos")
        print(f"  only_{a}: {result['only_a_count']}")
        print(f"  only_{b}: {result['only_b_count']}")
        print(f"  common: {result['common_count']}")
        print(f"  mesa_mismatches (sample 5000): {result['mesa_mismatches_in_sample']}")
        persist_anomalies(conn, result, args.eleccion)

    OUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"cross_validate_{ts}.json"
    out.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "id_eleccion": args.eleccion,
        "sources": latest,
        "pair_reports": reports,
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n[out] {out}")
    conn.close()


if __name__ == "__main__":
    main()
