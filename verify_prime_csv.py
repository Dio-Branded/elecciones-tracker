"""
Bloque 1 del plan: validar la auditoria de PRIME INSTITUTE re-sumando su CSV
y cruzando con el snapshot nacional vivo del tracker.

Descarga: https://primeinstitute.com/onpe/data.csv (86K filas)
Cruza:   candidates del ultimo snapshot 'presidencial' en onpe.db

Columnas del CSV de PRIME:
  mesa, departamento, provincia, distrito, local,
  electores, emitidos, validos,
  FU, SP, LA, NI, BE, AL, CH,   # votos por candidato (top 7)
  blancos, nulos, estado        # estado: C=contabilizada, P=pendiente, J=JEE

Salidas:
  data/prime_crosscheck_YYYYMMDD_HHMMSS.json
  stdout: tabla comparativa por candidato + por departamento
"""
import csv
import json
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn

PRIME_CSV_URL = "https://primeinstitute.com/onpe/data.csv"
CSV_CACHE = Path(__file__).parent / "data" / "prime_data.csv"
OUT_DIR = Path(__file__).parent / "data"

# Columnas de candidatos del CSV PRIME -> codigo_agrupacion en ONPE (snapshot id=4)
CSV_TO_ONPE = {
    "FU": ("FUERZA POPULAR",                8,  "KEIKO SOFIA FUJIMORI HIGUCHI"),
    "SP": ("JUNTOS POR EL PERU",            10, "ROBERTO HELBERT SANCHEZ PALOMINO"),
    "LA": ("RENOVACION POPULAR",            35, "RAFAEL BERNARDO LOPEZ ALIAGA CAZORLA"),
    "NI": ("PARTIDO DEL BUEN GOBIERNO",     16, "JORGE NIETO MONTESINOS"),
    "BE": ("PARTIDO CIVICO OBRAS",          14, "RICARDO PABLO BELMONT CASSINELLI"),
    "AL": ("PARTIDO PAIS PARA TODOS",       23, "CARLOS GONSALO ALVAREZ LOAYZA"),
    "CH": ("AHORA NACION - AN",             2,  "PABLO ALFONSO LOPEZ CHAU NAVA"),
}


def download_csv(force: bool = False) -> Path:
    if CSV_CACHE.exists() and not force:
        age_min = (datetime.now().timestamp() - CSV_CACHE.stat().st_mtime) / 60
        if age_min < 60:
            print(f"[cache] usando CSV local ({age_min:.1f} min de antiguedad)")
            return CSV_CACHE
    print(f"[download] {PRIME_CSV_URL}")
    CSV_CACHE.parent.mkdir(exist_ok=True)
    req = urllib.request.Request(PRIME_CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        CSV_CACHE.write_bytes(r.read())
    print(f"[download] {CSV_CACHE.stat().st_size // 1024} KB guardados en {CSV_CACHE}")
    return CSV_CACHE


def sum_csv(csv_path: Path) -> dict:
    """Retorna:
      totals = { 'validos': int, 'emitidos': int, 'blancos': int, 'nulos': int,
                 'FU': int, 'SP': int, ... }
      by_depto = { depto: { candidato: int, ... } }
      mesas_estado = { 'C': int, 'P': int, 'J': int, ... }
      by_estado = { 'C': {'FU':..,'SP':..}, 'P': {...}, ... }
    """
    totals = defaultdict(int)
    by_depto = defaultdict(lambda: defaultdict(int))
    by_estado = defaultdict(lambda: defaultdict(int))
    mesas_estado = defaultdict(int)
    mesas_count = 0

    with csv_path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mesas_count += 1
            estado = row.get("estado", "").strip()
            depto = row.get("departamento", "").strip().upper()
            mesas_estado[estado] += 1

            def as_int(k):
                v = row.get(k, "").strip()
                try:
                    return int(v) if v else 0
                except ValueError:
                    return 0

            for col in ("validos", "emitidos", "blancos", "nulos"):
                totals[col] += as_int(col)

            for cand in CSV_TO_ONPE:
                v = as_int(cand)
                totals[cand] += v
                by_depto[depto][cand] += v
                by_estado[estado][cand] += v

    return {
        "mesas_count": mesas_count,
        "mesas_estado": dict(mesas_estado),
        "totals": dict(totals),
        "by_depto": {d: dict(v) for d, v in by_depto.items()},
        "by_estado": {e: dict(v) for e, v in by_estado.items()},
    }


def fetch_onpe_snapshot() -> dict:
    """Del tracker: ultimo snapshot presidencial + candidates."""
    conn = get_conn()
    row = conn.execute(
        "SELECT id, captured_at, actas_contabilizadas_pct, contabilizadas, total_actas, total_votos_emitidos "
        "FROM snapshots WHERE tipo='presidencial' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise RuntimeError("No hay snapshot presidencial en onpe.db — correr scraper.py primero")
    sid, captured_at, pct, contab, total, emitidos = row
    cands = list(conn.execute(
        "SELECT codigo_agrupacion, nombre_agrupacion, nombre_candidato, total_votos_validos, pct_votos_validos "
        "FROM candidates WHERE snapshot_id=?",
        (sid,),
    ))
    conn.close()
    by_codigo = {c[0]: {"agrupacion": c[1], "candidato": c[2], "votos": c[3], "pct": c[4]} for c in cands}
    return {
        "snapshot_id": sid,
        "captured_at": captured_at,
        "actas_pct": pct,
        "contabilizadas": contab,
        "total_actas": total,
        "total_votos_emitidos": emitidos,
        "by_codigo": by_codigo,
    }


def compare(csv_data: dict, onpe_data: dict) -> dict:
    """Empareja candidato-a-candidato y calcula desfase."""
    rows = []
    csv_totals = csv_data["totals"]
    csv_sum_all_cands = sum(csv_totals[c] for c in CSV_TO_ONPE)
    for short, (name_fallback, codigo, cand_name) in CSV_TO_ONPE.items():
        onpe = onpe_data["by_codigo"].get(codigo, {})
        csv_v = csv_totals.get(short, 0)
        onpe_v = onpe.get("votos", 0)
        diff = onpe_v - csv_v
        pct_share = (csv_v / csv_totals["validos"] * 100) if csv_totals["validos"] else 0
        pct_desfase = (abs(diff) / sum_abs_diffs_placeholder(csv_data, onpe_data) * 100) if diff else 0
        rows.append({
            "codigo_agrupacion": codigo,
            "short": short,
            "candidato": onpe.get("candidato") or cand_name,
            "onpe_nacional": onpe_v,
            "csv_suma_mesas": csv_v,
            "desfase_abs": diff,
            "desfase_pct_vs_onpe": (diff / onpe_v * 100) if onpe_v else 0,
            "share_voto_csv_pct": pct_share,
        })
    # Recalcular % desfase relativo al total de desfases
    total_abs_desfase = sum(abs(r["desfase_abs"]) for r in rows)
    for r in rows:
        r["desfase_pct_del_total"] = (abs(r["desfase_abs"]) / total_abs_desfase * 100) if total_abs_desfase else 0
        r["ratio_desfase_vs_share"] = (
            r["desfase_pct_del_total"] / r["share_voto_csv_pct"]
            if r["share_voto_csv_pct"] else 0
        )
    rows.sort(key=lambda r: abs(r["desfase_abs"]), reverse=True)
    return {
        "total_abs_desfase": total_abs_desfase,
        "rows": rows,
    }


def sum_abs_diffs_placeholder(csv_data, onpe_data):
    # Placeholder para la primera pasada; recomputed en compare()
    return 1


def format_table(cmp: dict, csv_data: dict, onpe_data: dict) -> str:
    out = []
    out.append("=" * 100)
    out.append("COMPARATIVA NACIONAL ONPE vs SUMA MESA-POR-MESA (CSV PRIME)")
    out.append(f"  ONPE snapshot:    {onpe_data['captured_at']} ({onpe_data['actas_pct']:.2f}% actas)")
    out.append(f"  PRIME CSV mesas:  {csv_data['mesas_count']:,}  estados: {csv_data['mesas_estado']}")
    out.append(f"  Total desfase:    {cmp['total_abs_desfase']:,} votos (suma de |diff| top 7)")
    out.append("=" * 100)
    header = f"{'CANDIDATO':<40} {'ONPE':>12} {'CSV':>12} {'DESFASE':>10} {'%vs':>6} {'SHARE%':>7} {'RATIO':>6}"
    out.append(header)
    out.append("-" * len(header))
    for r in cmp["rows"]:
        out.append(
            f"{(r['candidato'] or '')[:38]:<40} "
            f"{r['onpe_nacional']:>12,} "
            f"{r['csv_suma_mesas']:>12,} "
            f"{r['desfase_abs']:>+10,} "
            f"{r['desfase_pct_vs_onpe']:>+6.2f} "
            f"{r['share_voto_csv_pct']:>6.2f}% "
            f"{r['ratio_desfase_vs_share']:>5.2f}x"
        )
    out.append("-" * len(header))
    out.append("")
    out.append("INTERPRETACION:")
    out.append("  - DESFASE = ONPE_nacional - SUMA_mesas. Positivo = nacional tiene MAS votos que la suma.")
    out.append("  - RATIO = (% del desfase que aporta) / (% del voto). >1.5x es desproporcionado.")
    out.append("  - Esperado para error tecnico uniforme: RATIO ≈ 1.0 para todos los candidatos.")
    return "\n".join(out)


def main():
    force = "--force-download" in sys.argv
    csv_path = download_csv(force=force)
    print("\n[parse] sumando CSV mesa por mesa...")
    csv_data = sum_csv(csv_path)
    print(f"[parse] {csv_data['mesas_count']:,} mesas procesadas")
    print(f"[parse] estados: {csv_data['mesas_estado']}")
    print(f"[parse] validos={csv_data['totals']['validos']:,}  emitidos={csv_data['totals']['emitidos']:,}")

    print("\n[db] leyendo snapshot ONPE nacional...")
    onpe_data = fetch_onpe_snapshot()
    print(f"[db] snapshot_id={onpe_data['snapshot_id']}  captured_at={onpe_data['captured_at']}  actas={onpe_data['actas_pct']}%")

    print("\n[compare] cruzando...")
    cmp = compare(csv_data, onpe_data)

    table = format_table(cmp, csv_data, onpe_data)
    print("\n" + table)

    # Guardar JSON completo
    OUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"prime_crosscheck_{ts}.json"
    out_path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "csv_source": PRIME_CSV_URL,
        "csv_mesas_count": csv_data["mesas_count"],
        "csv_mesas_estado": csv_data["mesas_estado"],
        "csv_totals": csv_data["totals"],
        "onpe_snapshot": {
            "snapshot_id": onpe_data["snapshot_id"],
            "captured_at": onpe_data["captured_at"],
            "actas_pct": onpe_data["actas_pct"],
        },
        "comparison": cmp,
        "by_depto_csv": csv_data["by_depto"],
        "by_estado_csv": csv_data["by_estado"],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[out] {out_path}")


if __name__ == "__main__":
    main()
