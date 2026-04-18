"""Serie temporal del desfase de Sanchez Palomino (ag=10) respecto al agregado nacional de la API.

Correr despues de cada scrape incremental. Anade una fila a data/sanchez_timeline.csv
con: timestamp, escrutinio_pct, nacional_votos, mesas_votos, ratio_desfase_share.

Cuando el escrutinio suba de 93% a 100%, la serie mostrara si el ratio 3.96x converge
a 1.0 (bug benigno por mesas faltantes en zonas chavistas) o permanece >=2 (sesgo sistemico).
"""
from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn

OUT = Path(__file__).parent / "data" / "sanchez_timeline.csv"
TARGET_AGRUP = 10  # Sanchez Palomino


def main():
    conn = get_conn()

    # Latest snapshot full completo (modo='full' y actas_ok>=60000 => todas las presidenciales)
    row = conn.execute(
        "SELECT id, captured_at FROM actas_snapshots "
        "WHERE modo='full' AND actas_ok>=60000 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT id, captured_at FROM actas_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if not row:
        print("no hay snapshots"); sys.exit(1)
    snap_id, captured_at = row

    # Nacional agregado reciente
    nat = conn.execute(
        "SELECT id, actas_contabilizadas_pct FROM snapshots "
        "WHERE tipo='presidencial' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not nat:
        print("no hay snapshots nacionales"); sys.exit(1)
    nat_sid, pct_escrutinio = nat

    # Votos de ag=10 en nacional
    nat_votos = conn.execute(
        "SELECT total_votos_validos FROM candidates WHERE snapshot_id=? AND codigo_agrupacion=?",
        (nat_sid, TARGET_AGRUP),
    ).fetchone()
    nat_votos = nat_votos[0] if nat_votos else 0

    # Votos agregados en actas (mesa_search)
    mesas_votos_row = conn.execute(
        "SELECT SUM(votos) FROM acta_votos WHERE snapshot_id=? AND id_eleccion=10 AND codigo_agrupacion=?",
        (snap_id, TARGET_AGRUP),
    ).fetchone()
    mesas_votos = mesas_votos_row[0] or 0

    # Totales (share + desfase)
    total_mesas = conn.execute(
        "SELECT SUM(votos) FROM acta_votos WHERE snapshot_id=? AND id_eleccion=10",
        (snap_id,),
    ).fetchone()[0] or 0
    total_desfase = sum(abs((r[1] or 0) - (r[2] or 0)) for r in conn.execute(
        "SELECT c.codigo_agrupacion, c.total_votos_validos, "
        "COALESCE((SELECT SUM(av.votos) FROM acta_votos av "
        " WHERE av.snapshot_id=? AND av.id_eleccion=10 AND av.codigo_agrupacion=c.codigo_agrupacion),0) "
        "FROM candidates c WHERE c.snapshot_id=?",
        (snap_id, nat_sid),
    )) or 0

    desfase = nat_votos - mesas_votos
    share = mesas_votos / total_mesas * 100 if total_mesas else 0
    pct_del_desfase = abs(desfase) / total_desfase * 100 if total_desfase else 0
    ratio = pct_del_desfase / share if share else 0

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_row = {
        "timestamp": now,
        "snapshot_id": snap_id,
        "nat_snapshot_id": nat_sid,
        "escrutinio_pct": pct_escrutinio,
        "nacional_votos_ag10": nat_votos,
        "mesas_votos_ag10": mesas_votos,
        "desfase": desfase,
        "share_pct": round(share, 4),
        "pct_del_desfase": round(pct_del_desfase, 4),
        "ratio": round(ratio, 4),
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    first = not OUT.exists()
    with OUT.open("a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(new_row.keys()))
        if first:
            w.writeheader()
        w.writerow(new_row)

    print(f"[sanchez] escrutinio={pct_escrutinio}% nat={nat_votos} mesas={mesas_votos} "
          f"desfase={desfase} ratio={ratio:.2f}x -> {OUT}")
    conn.close()


if __name__ == "__main__":
    main()
