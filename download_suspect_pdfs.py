"""Descarga PDFs escaneados SOLO de mesas con anomalias graves.

En lugar de bajar 87K actas (~20-40 GB), usa el ultimo anomalies_report_*.json
y selecciona mesas prioritarias segun estos criterios (en este orden):
  1. Mesas con surge+drop simultaneos (candidatas a transposicion de filas)
  2. Top N surges por z_score absoluto
  3. Top N drops por z_score absoluto

Deduplica y baja cada mesa una sola vez. Genera reporte HTML con el conteo
por agrupacion de la API, pares sospechosos, y links al PDF.

Uso:
  python download_suspect_pdfs.py                      # default: 169 transposiciones + top 50 surge/drop
  python download_suspect_pdfs.py --max 100            # limitar total
  python download_suspect_pdfs.py --only-transpositions
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn
from validate_actas import (
    PDF_DIR, OUT_DIR, _cookies_and_headers, download_pdfs_for_mesa,
)


def latest_anomalies_report() -> Path | None:
    files = sorted(OUT_DIR.glob("anomalies_report_*.json"))
    return files[-1] if files else None


def select_suspect_mesas(report: Path, top_n_each: int = 50,
                         only_transpositions: bool = False) -> dict:
    """Retorna {codigo: {'reasons': [...], 'priority': int}}."""
    data = json.loads(report.read_text(encoding="utf-8"))
    olocal = [f for f in data["findings"] if f.get("tipo") == "outlier_local"]

    by_mesa = defaultdict(lambda: {"surge": [], "drop": []})
    for f in olocal:
        by_mesa[f["codigo"]][f["detalle"]["subtipo"]].append(f)

    # Categoria 1: surge+drop simultaneos (prioridad 1 - mas sospechosos)
    transpositions = {m: fl for m, fl in by_mesa.items() if fl["surge"] and fl["drop"]}
    out: dict[int, dict] = {}
    for m, fl in transpositions.items():
        best_s = max(fl["surge"], key=lambda x: x["detalle"]["z_score"])
        best_d = max(fl["drop"], key=lambda x: x["detalle"]["z_score"])
        out[m] = {
            "priority": 1,
            "reasons": ["transposition"],
            "surge": {"agrup": best_s["codigo_agrupacion"],
                      "votos": best_s["detalle"]["votos_mesa"],
                      "z": best_s["detalle"]["z_score"]},
            "drop":  {"agrup": best_d["codigo_agrupacion"],
                      "votos": best_d["detalle"]["votos_mesa"],
                      "mean":  best_d["detalle"]["media_local"],
                      "z": best_d["detalle"]["z_score"]},
            "score": best_s["detalle"]["z_score"] + best_d["detalle"]["z_score"],
        }
    if only_transpositions:
        return out

    # Categoria 2: top N surges puros
    pure_surges = [(m, max(fl["surge"], key=lambda x: x["detalle"]["z_score"]))
                   for m, fl in by_mesa.items() if fl["surge"] and not fl["drop"]]
    pure_surges.sort(key=lambda t: t[1]["detalle"]["z_score"], reverse=True)
    for m, s in pure_surges[:top_n_each]:
        if m in out:
            continue
        out[m] = {"priority": 2, "reasons": ["surge"],
                  "surge": {"agrup": s["codigo_agrupacion"],
                            "votos": s["detalle"]["votos_mesa"],
                            "z": s["detalle"]["z_score"]},
                  "score": s["detalle"]["z_score"]}

    # Categoria 3: top N drops puros
    pure_drops = [(m, max(fl["drop"], key=lambda x: x["detalle"]["z_score"]))
                  for m, fl in by_mesa.items() if fl["drop"] and not fl["surge"]]
    pure_drops.sort(key=lambda t: t[1]["detalle"]["z_score"], reverse=True)
    for m, d in pure_drops[:top_n_each]:
        if m in out:
            continue
        out[m] = {"priority": 3, "reasons": ["drop"],
                  "drop": {"agrup": d["codigo_agrupacion"],
                           "votos": d["detalle"]["votos_mesa"],
                           "mean":  d["detalle"]["media_local"],
                           "z": d["detalle"]["z_score"]},
                  "score": d["detalle"]["z_score"]}
    return out


async def _download_all(codigos: list[int]) -> list[dict]:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    jar, headers = await _cookies_and_headers()
    import aiohttp
    all_results = []
    async with aiohttp.ClientSession(
            cookie_jar=jar, headers=headers,
            timeout=aiohttp.ClientTimeout(total=60)) as session:
        for i, codigo in enumerate(codigos, 1):
            # skip si ya fue descargada
            padded = f"{codigo:06d}"
            existing = list(PDF_DIR.glob(f"{padded}_*.pdf"))
            if existing:
                print(f"[{i}/{len(codigos)}] mesa {padded} ya descargada ({len(existing)} pdfs)")
                for p in existing:
                    all_results.append({"codigo": codigo, "pdf_path": str(p),
                                        "descripcion": p.stem.split("_", 2)[-1],
                                        "size_kb": p.stat().st_size // 1024,
                                        "cached": True})
                continue
            print(f"[{i}/{len(codigos)}] mesa {padded} descargando...")
            res = await download_pdfs_for_mesa(session, codigo, 10)
            for r in res:
                if "error" in r:
                    print(f"  ERR {r['error']}")
                else:
                    print(f"  OK  {r['descripcion']} -> {r['size_kb']} KB")
            all_results.extend(res)
    return all_results


def render_report(suspects: dict, download_results: list[dict], conn) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"suspect_report_{ts}.html"

    # Map agrup -> nombre
    nom_map = {r[0]: r[1] for r in conn.execute(
        "SELECT codigo_agrupacion, nombre_agrupacion FROM candidates WHERE snapshot_id="
        "(SELECT MAX(id) FROM snapshots WHERE tipo='presidencial')"
    )}

    # Traer conteos API por mesa
    latest_full = conn.execute(
        "SELECT id FROM actas_snapshots WHERE source='mesa_search' AND actas_ok>=10000 "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not latest_full:
        latest_full = conn.execute(
            "SELECT id FROM actas_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
    snap_id = latest_full[0]

    pdfs_by_mesa = defaultdict(list)
    for r in download_results:
        if r.get("pdf_path"):
            pdfs_by_mesa[r["codigo"]].append(r)

    sorted_mesas = sorted(suspects.items(),
                          key=lambda kv: (kv[1]["priority"], -kv[1]["score"]))
    html = [
        '<!doctype html><html lang="es"><head><meta charset="utf-8">',
        '<title>Mesas sospechosas — auditoria ONPE</title>',
        '<style>body{background:#0d1117;color:#e6edf3;font-family:sans-serif;margin:20px}',
        '.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;margin:12px 0}',
        'h1{color:#58a6ff}h2{color:#f0883e;margin:0 0 8px}',
        'table{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}',
        'th{background:#21262d;color:#58a6ff;text-align:left;padding:6px 10px}',
        'td{padding:5px 10px;border-bottom:1px solid #21262d}',
        '.p1{border-left:4px solid #f85149}.p2{border-left:4px solid #f0883e}.p3{border-left:4px solid #ffd33d}',
        '.surge{color:#f85149}.drop{color:#58a6ff}',
        '.tag{background:#21262d;padding:2px 8px;border-radius:4px;font-size:11px;margin-right:6px}',
        'a{color:#58a6ff}.pdf{background:#3fb95022;color:#3fb950;padding:3px 10px;border-radius:4px}',
        '</style></head><body>',
        f'<h1>Mesas sospechosas — {len(suspects)} mesas</h1>',
        f'<p>Generado {datetime.now(timezone.utc).isoformat(timespec="seconds")} desde snapshot {snap_id}.</p>',
        '<p>Prioridad 1 (rojo): surge+drop en la misma mesa (posible transposicion de filas). '
        'Prioridad 2 (naranja): surge puro. Prioridad 3 (amarillo): drop puro.</p>',
    ]

    for codigo, info in sorted_mesas:
        padded = f"{codigo:06d}"
        # Traer raw_json para ubicacion
        raw = conn.execute(
            "SELECT raw_json FROM actas WHERE snapshot_id=? AND codigo=? AND id_eleccion=10",
            (snap_id, codigo),
        ).fetchone()
        ubic = ""
        if raw and raw[0]:
            try:
                d = json.loads(raw[0])
                ubic = f"{d.get('nombreLocalVotacion','')} (local {d.get('codigoLocalVotacion','')})"
            except Exception:
                pass

        html.append(f'<div class="card p{info["priority"]}">')
        html.append(f'<h2>Mesa {padded}</h2>')
        html.append(f'<div class="text-sm">{ubic}</div>')
        html.append(f'<div class="mt-2">')
        for r in info["reasons"]:
            html.append(f'<span class="tag">{r}</span>')
        if "surge" in info:
            s = info["surge"]
            nombre_s = nom_map.get(s["agrup"], f'agrup={s["agrup"]}')
            html.append(f'<span class="tag surge">surge: {nombre_s} '
                        f'votos={s["votos"]} z={s["z"]}</span>')
        if "drop" in info:
            d = info["drop"]
            nombre_d = nom_map.get(d["agrup"], f'agrup={d["agrup"]}')
            html.append(f'<span class="tag drop">drop: {nombre_d} '
                        f'votos={d["votos"]} (media colegio {d["mean"]}) z={d["z"]}</span>')
        html.append('</div>')

        # Mostrar TODOS los votos de la mesa segun API
        votes = list(conn.execute(
            "SELECT codigo_agrupacion, votos FROM acta_votos "
            "WHERE snapshot_id=? AND codigo=? AND id_eleccion=10 ORDER BY votos DESC",
            (snap_id, codigo),
        ))
        if votes:
            html.append('<table><thead><tr><th>agrup</th><th>Partido</th><th>Votos (API)</th></tr></thead><tbody>')
            for agrup, v in votes[:10]:
                html.append(f'<tr><td>{agrup}</td><td>{nom_map.get(agrup, "")}</td><td>{v}</td></tr>')
            html.append('</tbody></table>')

        # Links PDFs
        if codigo in pdfs_by_mesa:
            html.append('<div class="mt-2">')
            for p in pdfs_by_mesa[codigo]:
                rel = Path(p["pdf_path"]).name
                cached = " (cache)" if p.get("cached") else ""
                html.append(f'<a class="pdf" href="actas_pdfs/{rel}" target="_blank">'
                            f'{p["descripcion"]} ({p["size_kb"]} KB){cached}</a> ')
            html.append('</div>')
        else:
            html.append('<div class="mt-2"><em>(PDFs no descargados)</em></div>')

        html.append('</div>')

    html.append('</body></html>')
    out.write_text("\n".join(html), encoding="utf-8")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-per-category", type=int, default=50,
                    help="Top N por categoria surge-puro y drop-puro (default 50)")
    ap.add_argument("--max", type=int, default=None,
                    help="Max total de mesas a descargar (default: todas las seleccionadas)")
    ap.add_argument("--only-transpositions", action="store_true",
                    help="Solo surge+drop simultaneos (ignora puros)")
    ap.add_argument("--no-download", action="store_true",
                    help="Solo listar mesas seleccionadas, no descargar")
    args = ap.parse_args()

    report = latest_anomalies_report()
    if not report:
        print("No hay anomalies_report. Corre python anomalies.py primero.")
        sys.exit(1)
    print(f"[report] {report.name}")

    suspects = select_suspect_mesas(
        report,
        top_n_each=args.top_per_category,
        only_transpositions=args.only_transpositions,
    )
    if args.max and len(suspects) > args.max:
        sorted_items = sorted(suspects.items(),
                              key=lambda kv: (kv[1]["priority"], -kv[1]["score"]))
        suspects = dict(sorted_items[:args.max])

    by_pri = defaultdict(int)
    for info in suspects.values():
        by_pri[info["priority"]] += 1
    print(f"[suspects] total={len(suspects)}")
    for p in sorted(by_pri):
        print(f"  priority {p}: {by_pri[p]}")

    if args.no_download:
        for c, info in sorted(suspects.items(), key=lambda kv: (kv[1]["priority"], -kv[1]["score"]))[:30]:
            print(f"  mesa {c:06d} p{info['priority']} score={info['score']:.1f} reasons={info['reasons']}")
        return

    codigos = [c for c, _ in sorted(suspects.items(),
                                     key=lambda kv: (kv[1]["priority"], -kv[1]["score"]))]
    results = asyncio.run(_download_all(codigos))
    print(f"\n[done] {len(results)} archivos totales")

    conn = get_conn()
    out = render_report(suspects, results, conn)
    print(f"[report] {out}")
    conn.close()


if __name__ == "__main__":
    main()
