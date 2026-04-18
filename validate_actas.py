"""Valida mesas con mismatches descargando sus actas escaneadas.

Cuando cross_validate detecta mesas con votos distintos entre fuentes
(p.ej. PRIME CSV vs nuestro scrape ONPE direct), baja los PDFs escaneados
del acta original para que un humano pueda verificar los numeros.

Flow de descarga (cada mesa):
  1. GET /actas/buscar/mesa?codigoMesa=NNNNNN -> obtener `id` del acta
  2. GET /actas/{id} -> obtener lista `archivos` con IDs MongoDB
  3. GET /actas/file?id=<mongo_id> -> obtener URL S3 firmada
  4. GET <signed_url> -> descargar el PDF

Salida:
  data/actas_pdfs/{codigo}_{tipo}.pdf
  data/validation_report_YYYYMMDD_HHMMSS.html
     Tabla por mesa con: PRIME vs API (votos por candidato) + link al PDF

Uso:
  python validate_actas.py                        # todos los mismatches pendientes
  python validate_actas.py --limit 10             # solo primeros 10
  python validate_actas.py --codigo 1,100,1000    # mesas especificas
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn

ONPE_BASE = "https://resultadoelectoral.onpe.gob.pe"
OUT_DIR = Path(__file__).parent / "data"
PDF_DIR = OUT_DIR / "actas_pdfs"


async def _cookies_and_headers():
    """Bootstrap Playwright (async) para cookies + headers CORS."""
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True)
        ctx = await b.new_context()
        page = await ctx.new_page()
        await page.goto(f"{ONPE_BASE}/main/presidenciales", wait_until="networkidle", timeout=45000)
        cookies = await ctx.cookies()
        await b.close()
    import aiohttp
    jar = aiohttp.CookieJar()
    for c in cookies:
        jar.update_cookies({c["name"]: c["value"]},
                           response_url=aiohttp.helpers.URL("https://" + c["domain"].lstrip(".")))
    headers = {
        "User-Agent": "Mozilla/5.0 AppleWebKit/537.36 Chrome/124.0.0.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": ONPE_BASE,
        "Referer": f"{ONPE_BASE}/main/presidenciales",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }
    return jar, headers


async def download_pdfs_for_mesa(session, codigo: int, id_eleccion: int = 10) -> list[dict]:
    """Para un codigo, retorna lista de {tipo, descripcion, pdf_path, acta_id, archivo_id}.
    Baja los PDFs a PDF_DIR/{codigo}_{tipo}_{sufijo}.pdf
    """
    padded = f"{codigo:06d}"
    results = []

    # 1. buscarMesa
    url = f"{ONPE_BASE}/presentacion-backend/actas/buscar/mesa?codigoMesa={padded}"
    async with session.get(url) as r:
        if r.status != 200:
            return [{"error": f"buscarMesa status={r.status}"}]
        j = await r.json()
    actas = (j.get("data") or [])
    # Filtrar solo la eleccion pedida
    actas = [a for a in actas if a.get("idEleccion") == id_eleccion]
    if not actas:
        return [{"error": "no actas para esta eleccion"}]

    for acta in actas:
        acta_id = acta.get("id")
        # 2. detalle con archivos
        async with session.get(f"{ONPE_BASE}/presentacion-backend/actas/{acta_id}") as r:
            if r.status != 200:
                continue
            jd = await r.json()
        archivos = (jd.get("data") or {}).get("archivos") or []
        for archivo in archivos:
            mongo_id = archivo.get("id")
            tipo = archivo.get("tipo")
            desc = archivo.get("descripcion") or f"tipo_{tipo}"
            # 3. file -> URL S3 firmada
            async with session.get(f"{ONPE_BASE}/presentacion-backend/actas/file?id={mongo_id}") as r:
                if r.status != 200:
                    continue
                signed_url = (await r.json()).get("data")
            if not signed_url:
                continue
            # 4. descargar PDF desde S3 (sin cookies ONPE, es otro dominio)
            try:
                import aiohttp
                async with aiohttp.ClientSession() as s3:
                    async with s3.get(signed_url) as r:
                        if r.status != 200:
                            results.append({"codigo": codigo, "acta_id": acta_id,
                                            "tipo": tipo, "descripcion": desc,
                                            "error": f"s3 status={r.status}"})
                            continue
                        pdf_bytes = await r.read()
            except Exception as e:
                results.append({"codigo": codigo, "acta_id": acta_id,
                                "error": f"s3 exception: {e}"})
                continue

            if not pdf_bytes.startswith(b"%PDF"):
                results.append({"codigo": codigo, "acta_id": acta_id,
                                "error": "respuesta S3 no es PDF"})
                continue
            safe_desc = re.sub(r"[^a-zA-Z0-9_-]", "_", desc)[:40]
            pdf_path = PDF_DIR / f"{padded}_{tipo}_{safe_desc}.pdf"
            pdf_path.write_bytes(pdf_bytes)
            results.append({"codigo": codigo, "acta_id": acta_id, "archivo_id": mongo_id,
                            "tipo": tipo, "descripcion": desc,
                            "pdf_path": str(pdf_path),
                            "size_kb": len(pdf_bytes) // 1024})
    return results


def get_mismatch_codes_from_cross_validate(conn, limit: int | None = None) -> list[int]:
    """Extrae los codigos con mesa_mismatch del mas reciente reporte cross_validate."""
    files = sorted(OUT_DIR.glob("cross_validate_*.json"))
    if not files:
        print("No hay reportes cross_validate. Corre python cross_validate.py primero.")
        return []
    latest = files[-1]
    data = json.loads(latest.read_text(encoding="utf-8"))
    codes = []
    for pair in data.get("pair_reports", []):
        for s in pair.get("mesa_mismatches_sample", []):
            cod = s.get("codigo")
            if cod and cod not in codes:
                codes.append(cod)
    if limit:
        codes = codes[:limit]
    print(f"Codigos con mismatch en {latest.name}: {len(codes)}")
    return codes


async def _main(codigos: list[int], id_eleccion: int) -> list[dict]:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    jar, headers = await _cookies_and_headers()
    import aiohttp
    all_results = []
    async with aiohttp.ClientSession(cookie_jar=jar, headers=headers,
                                      timeout=aiohttp.ClientTimeout(total=60)) as session:
        for i, codigo in enumerate(codigos, 1):
            print(f"[{i}/{len(codigos)}] mesa {codigo:06d}")
            res = await download_pdfs_for_mesa(session, codigo, id_eleccion)
            for r in res:
                if "error" in r:
                    print(f"  ERR {r['error']}")
                else:
                    print(f"  OK  {r['descripcion']} -> {r['pdf_path']} ({r['size_kb']} KB)")
            all_results.extend(res)
    return all_results


def render_html_report(results: list[dict], conn) -> Path:
    """Genera un reporte HTML con tabla comparativa por mesa."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"validation_report_{ts}.html"

    # Agrupar resultados por codigo
    by_codigo: dict[int, list[dict]] = {}
    for r in results:
        c = r.get("codigo")
        if c is None:
            continue
        by_codigo.setdefault(c, []).append(r)

    # Para cada codigo: traer los votos de ambas fuentes (PRIME + mesa_search)
    rows = []
    for codigo in sorted(by_codigo):
        files = [f for f in by_codigo[codigo] if "pdf_path" in f]
        errors = [f for f in by_codigo[codigo] if "error" in f]
        # PRIME snapshot (id=1) vs mesa_search ultimo
        prime_votes = {r[0]: r[1] for r in conn.execute(
            "SELECT codigo_agrupacion, votos FROM acta_votos "
            "WHERE snapshot_id=1 AND codigo=? AND id_eleccion=10", (codigo,)
        )}
        latest_sid = conn.execute(
            "SELECT MAX(id) FROM actas_snapshots WHERE source='mesa_search' AND actas_ok>0"
        ).fetchone()[0]
        ms_votes = {r[0]: r[1] for r in conn.execute(
            "SELECT codigo_agrupacion, votos FROM acta_votos "
            "WHERE snapshot_id=? AND codigo=? AND id_eleccion=10", (latest_sid, codigo)
        )}
        # Map codigo_agrupacion -> nombre (aprovecho candidates)
        nom = {r[0]: r[1] for r in conn.execute(
            "SELECT codigo_agrupacion, nombre_agrupacion FROM candidates WHERE snapshot_id=?",
            (conn.execute("SELECT MAX(id) FROM snapshots WHERE tipo='presidencial'").fetchone()[0],)
        )}
        all_cods = set(prime_votes) | set(ms_votes)
        compare_rows = []
        for cod in sorted(all_cods):
            p = prime_votes.get(cod, 0); m = ms_votes.get(cod, 0)
            if p != m:
                compare_rows.append({
                    "codigo_agrupacion": cod,
                    "nombre": nom.get(cod, f"agrup={cod}"),
                    "prime": p, "mesa_search": m, "diff": m - p,
                })
        rows.append({
            "codigo": codigo,
            "files": files,
            "errors": errors,
            "compare_rows": compare_rows,
        })

    # HTML simple con Tailwind CDN
    html = [
        '<!doctype html><html lang="es"><head><meta charset="utf-8"><title>Validacion actas</title>',
        '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css">',
        '<style>body{background:#0d1117;color:#e6edf3;font-family:sans-serif}',
        '.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;margin:12px 0}',
        'table{border-collapse:collapse;width:100%;font-size:13px}',
        'th{background:#21262d;color:#58a6ff;text-align:left;padding:6px 10px}',
        'td{padding:6px 10px;border-bottom:1px solid #21262d}',
        '.badge-red{background:#f8514922;color:#f85149;padding:2px 8px;border-radius:4px;font-size:11px}',
        '.badge-green{background:#3fb95022;color:#3fb950;padding:2px 8px;border-radius:4px;font-size:11px}',
        'a{color:#58a6ff}</style></head><body>',
        f'<div class="container mx-auto p-6 max-w-5xl">',
        '<h1 class="text-2xl font-bold mb-2">Validacion de actas — PRIME CSV vs ONPE direct</h1>',
        f'<p class="text-sm mb-4">Generado {datetime.now(timezone.utc).isoformat(timespec="seconds")}. '
        f'{len(rows)} mesas validadas. Click en el PDF para abrir el acta escaneada.</p>',
    ]
    for row in rows:
        html.append(f'<div class="card"><h2 class="text-lg font-bold">Mesa {row["codigo"]:06d}</h2>')
        if row["files"]:
            html.append('<div class="mb-2">')
            for f in row["files"]:
                rel = Path(f["pdf_path"]).name
                html.append(f'<a class="badge-green mr-2" href="actas_pdfs/{rel}" target="_blank">'
                            f'{f["descripcion"]} ({f["size_kb"]} KB)</a>')
            html.append('</div>')
        if row["errors"]:
            for e in row["errors"]:
                html.append(f'<div class="badge-red">error: {e.get("error")}</div>')
        if row["compare_rows"]:
            html.append('<table><thead><tr><th>Candidato/Partido</th>'
                        '<th class="text-right">PRIME</th><th class="text-right">ONPE direct</th>'
                        '<th class="text-right">Diff</th></tr></thead><tbody>')
            for c in row["compare_rows"]:
                diff_cls = "badge-red" if c["diff"] != 0 else ""
                html.append(f'<tr><td>{c["nombre"]}</td>'
                            f'<td class="text-right">{c["prime"]}</td>'
                            f'<td class="text-right">{c["mesa_search"]}</td>'
                            f'<td class="text-right"><span class="{diff_cls}">{c["diff"]:+}</span></td></tr>')
            html.append('</tbody></table>')
        else:
            html.append('<p class="text-sm">(no hay diferencias registradas en esta mesa)</p>')
        html.append('</div>')
    html.append('</div></body></html>')
    out.write_text("\n".join(html), encoding="utf-8")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=10,
                    help="Maximo de mesas a validar (default 10)")
    ap.add_argument("--codigo", default=None,
                    help="Codigos especificos separados por coma (sobreescribe mismatches)")
    ap.add_argument("--eleccion", type=int, default=10)
    args = ap.parse_args()

    conn = get_conn()
    if args.codigo:
        codigos = [int(x) for x in args.codigo.split(",")]
    else:
        codigos = get_mismatch_codes_from_cross_validate(conn, limit=args.limit)
    if not codigos:
        print("No hay codigos a validar.")
        sys.exit(0)

    print(f"Validando {len(codigos)} mesas: {codigos[:20]}...")
    results = asyncio.run(_main(codigos, args.eleccion))
    print(f"\n{len(results)} archivos obtenidos")

    report = render_html_report(results, conn)
    print(f"\nReporte: {report}")

    conn.close()


if __name__ == "__main__":
    main()
