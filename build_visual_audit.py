"""Visual audit report — crops + API values for priority mesas.

Estrategia: OCR automatico de digitos manuscritos es caro y ruidoso (TrOCR
handwritten <70%, PaddleOCR bloqueado por protobuf, Tesseract ~50-70%).
En lugar de fallar al automatizar, genera un REPORTE VISUAL donde el
auditor ve lado-a-lado:
  * crop de la columna de votos del acta escaneada (x=0.28..0.45, y=0.20..0.87)
  * tabla con lo que dice la API

Un auditor humano revisa cada mesa en ~20 seg. 228 mesas = ~75 min. Factible.

Uso:
  python build_visual_audit.py                   # default: todas del ultimo report
  python build_visual_audit.py --max 50
  python build_visual_audit.py --codigo 54938,55452
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np

from db import get_conn
from ocr_trocr import pdf_to_image

PDF_DIR = Path(__file__).parent / "data" / "actas_pdfs"
OUT_DIR = Path(__file__).parent / "data"
CROPS_DIR = OUT_DIR / "audit_crops"
GEO_CACHE_PATH = OUT_DIR / "geo_cache.json"
ID_ELECCION = 10


def load_geo_cache() -> dict:
    if GEO_CACHE_PATH.exists():
        return json.loads(GEO_CACHE_PATH.read_text(encoding="utf-8"))
    return {}

# Crop window for vote column (calibrated 2026-04-18 by visual inspection)
# X: column "TOTAL DE VOTOS" is ~0.42..0.56 of image width
# Y: table (parties + 4 totals) spans 0.20..0.88
CROP_X0 = 0.410
CROP_X1 = 0.580
CROP_Y0 = 0.200
CROP_Y1 = 0.880


def find_pdf(codigo: int) -> Path | None:
    padded = f"{codigo:06d}"
    c = sorted(PDF_DIR.glob(f"{padded}_1_ACTA_DE_ESCRUTINIO.pdf"))
    if c:
        return c[0]
    c = sorted(PDF_DIR.glob(f"{padded}_*.pdf"))
    return c[0] if c else None


def extract_vote_column(pdf: Path) -> np.ndarray:
    img = pdf_to_image(pdf)
    H, W = img.shape[:2]
    x0 = int(W * CROP_X0)
    x1 = int(W * CROP_X1)
    y0 = int(H * CROP_Y0)
    y1 = int(H * CROP_Y1)
    return img[y0:y1, x0:x1]


def load_anomaly_hits() -> dict[int, list[dict]]:
    files = sorted(OUT_DIR.glob("anomalies_report_*.json"))
    if not files:
        return {}
    data = json.loads(files[-1].read_text(encoding="utf-8"))
    out = defaultdict(list)
    for f in data["findings"]:
        if f.get("tipo") == "outlier_local" and f.get("codigo") is not None:
            out[f["codigo"]].append(f)
    return out


def prioritize(codigos: list[int], anomaly_hits: dict[int, list[dict]]):
    """Returns list of (codigo, priority, max_z) sorted by priority then z desc."""
    out = []
    for c in codigos:
        hits = anomaly_hits.get(c, [])
        has_surge = any(h["detalle"]["subtipo"] == "surge" for h in hits)
        has_drop = any(h["detalle"]["subtipo"] == "drop" for h in hits)
        if has_surge and has_drop:
            pri = 1
        elif has_surge:
            pri = 2
        elif has_drop:
            pri = 3
        else:
            pri = 4
        max_z = max((h["detalle"]["z_score"] for h in hits), default=0)
        out.append((c, pri, max_z))
    out.sort(key=lambda t: (t[1], -t[2]))
    return out


def render_mesa_block(codigo: int, hits: list[dict], conn, snap_id: int,
                       nom_map: dict[int, str], geo_cache: dict) -> tuple[str, Path | None]:
    """Genera un bloque HTML para una mesa. Guarda crop en CROPS_DIR."""
    padded = f"{codigo:06d}"
    pdf = find_pdf(codigo)
    crop_path = None
    if pdf is not None:
        try:
            col = extract_vote_column(pdf)
            # Resize to max 500 wide for HTML
            W = col.shape[1]
            if W > 450:
                scale = 450 / W
                col = cv2.resize(col, (450, int(col.shape[0] * scale)),
                                 interpolation=cv2.INTER_AREA)
            crop_path = CROPS_DIR / f"{padded}_votes.jpg"
            cv2.imwrite(str(crop_path), col, [cv2.IMWRITE_JPEG_QUALITY, 85])
        except Exception as e:
            crop_path = None

    # Geo info: depart / prov / dist from geo_cache (enrich_geo.py), local from raw_json
    geo = geo_cache.get(str(codigo), {})
    raw_row = conn.execute(
        "SELECT raw_json FROM actas WHERE snapshot_id=? AND codigo=? AND id_eleccion=10",
        (snap_id, codigo),
    ).fetchone()
    local_name = geo.get("local") or ""
    local_cod = geo.get("codigo_local") or ""
    if not local_name and raw_row and raw_row[0]:
        try:
            d = json.loads(raw_row[0])
            local_name = d.get("nombreLocalVotacion") or ""
            local_cod = d.get("codigoLocalVotacion") or ""
        except Exception:
            pass
    depart = geo.get("departamento") or ""
    prov = geo.get("provincia") or ""
    dist = geo.get("distrito") or ""
    direccion = geo.get("direccion") or ""

    # API votes + totals
    votos = list(conn.execute(
        "SELECT codigo_agrupacion, votos FROM acta_votos "
        "WHERE snapshot_id=? AND codigo=? AND id_eleccion=10 ORDER BY codigo_agrupacion",
        (snap_id, codigo),
    ))
    acta = conn.execute(
        "SELECT total_votos_validos, total_votos_emitidos, electores_habiles, "
        "votos_blancos, votos_nulos FROM actas "
        "WHERE snapshot_id=? AND codigo=? AND id_eleccion=10",
        (snap_id, codigo),
    ).fetchone()

    # Flag which agrups are anomalous
    surge_agrups = {h["codigo_agrupacion"] for h in hits if h["detalle"]["subtipo"] == "surge"}
    drop_agrups = {h["codigo_agrupacion"] for h in hits if h["detalle"]["subtipo"] == "drop"}

    html = [f'<div class="card">']
    html.append(f'<h2>Mesa {padded}</h2>')
    # Geographic info
    geo_parts = []
    if depart:
        geo_parts.append(depart)
    if prov and prov != depart:
        geo_parts.append(prov)
    if dist and dist != prov:
        geo_parts.append(dist)
    geo_line = " / ".join(geo_parts)
    if geo_line:
        html.append(f'<div class="geo">{geo_line}</div>')
    if local_name:
        html.append(f'<div class="ubic"><b>{local_name}</b>'
                    + (f' <span class="local-code">(local {local_cod})</span>' if local_cod else '')
                    + '</div>')
    if direccion:
        html.append(f'<div class="direccion">{direccion}</div>')

    # Anomaly tags
    html.append('<div class="tags">')
    for h in hits:
        sub = h["detalle"]["subtipo"]
        agr = h["codigo_agrupacion"]
        z = h["detalle"]["z_score"]
        v = h["detalle"]["votos_mesa"]
        mn = h["detalle"]["media_local"]
        cls = "tag surge" if sub == "surge" else "tag drop"
        name = nom_map.get(agr, f"agrup_{agr}")
        html.append(f'<span class="{cls}">{sub} ag={agr} {name[:25]} v={v} mean={mn} z={z}</span>')
    html.append('</div>')

    # Layout: image left, table right
    html.append('<div class="row">')

    # Image
    html.append('<div class="col-img">')
    if crop_path:
        rel = crop_path.name
        html.append(f'<img src="audit_crops/{rel}" alt="votos mesa {padded}"/>')
    else:
        html.append('<div class="nopdf">(PDF no disponible)</div>')
    html.append('</div>')

    # Table
    html.append('<div class="col-table"><table>')
    html.append('<thead><tr><th>agrup</th><th>Partido</th><th>API votos</th></tr></thead><tbody>')
    for agrup, v in votos:
        cls = ""
        if agrup in surge_agrups:
            cls = "highlight-surge"
        elif agrup in drop_agrups:
            cls = "highlight-drop"
        name = nom_map.get(agrup, "")
        html.append(f'<tr class="{cls}"><td>{agrup}</td><td>{name[:45]}</td><td>{v}</td></tr>')
    if acta:
        v_val, v_emi, v_ele, v_bla, v_nul = acta
        html.append(f'<tr class="subt"><td></td><td><b>V\u00e1lidos</b></td><td>{v_val}</td></tr>')
        html.append(f'<tr class="subt"><td></td><td><b>Blancos</b></td><td>{v_bla}</td></tr>')
        html.append(f'<tr class="subt"><td></td><td><b>Nulos</b></td><td>{v_nul}</td></tr>')
        html.append(f'<tr class="subt"><td></td><td><b>Emitidos</b></td><td>{v_emi}</td></tr>')
        html.append(f'<tr class="subt"><td></td><td><b>Electores</b></td><td>{v_ele}</td></tr>')
    html.append('</tbody></table>')

    # Link to full PDF
    if pdf:
        html.append(f'<div class="pdf-link"><a href="actas_pdfs/{pdf.name}" target="_blank">Ver PDF completo</a></div>')
    html.append('</div>')  # col-table
    html.append('</div>')  # row
    html.append('</div>')  # card
    return "\n".join(html), crop_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codigo", default=None, help="Codigos separados por coma")
    ap.add_argument("--max", type=int, default=None)
    args = ap.parse_args()

    CROPS_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_conn()
    snap_id = conn.execute(
        "SELECT id FROM actas_snapshots WHERE modo='full' AND actas_ok>=60000 "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]
    nsid = conn.execute(
        "SELECT MAX(id) FROM snapshots WHERE tipo='presidencial'"
    ).fetchone()[0]
    nom_map = {r[0]: (r[1] or "") for r in conn.execute(
        "SELECT codigo_agrupacion, nombre_agrupacion FROM candidates WHERE snapshot_id=?",
        (nsid,),
    )}

    geo_cache = load_geo_cache()
    anomaly_hits = load_anomaly_hits()
    if args.codigo:
        codigos = [int(x) for x in args.codigo.split(",")]
    else:
        codigos = sorted(anomaly_hits.keys())
    if args.max:
        codigos = codigos[:args.max]

    prioritized = prioritize(codigos, anomaly_hits)
    total_with_pdf = sum(1 for c, *_ in prioritized if find_pdf(c))
    print(f"[audit] {len(prioritized)} mesas ({total_with_pdf} con PDF)")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_html = OUT_DIR / f"visual_audit_{ts}.html"

    header = """<!doctype html><html lang="es"><head><meta charset="utf-8">
<title>Auditoria visual ONPE</title>
<style>
body{background:#0d1117;color:#e6edf3;font-family:sans-serif;margin:20px;max-width:1400px;margin:auto}
h1{color:#58a6ff}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;margin:16px 0}
.card.p1{border-left:5px solid #f85149}
.card.p2{border-left:5px solid #f0883e}
.card.p3{border-left:5px solid #ffd33d}
.card.p4{border-left:5px solid #8b949e}
h2{color:#f0883e;margin:0 0 8px}
.geo{font-size:13px;color:#3fb950;margin-bottom:3px;font-weight:bold}
.ubic{font-size:13px;color:#c9d1d9;margin-bottom:3px}
.local-code{color:#8b949e;font-weight:normal}
.direccion{font-size:12px;color:#8b949e;margin-bottom:6px;font-style:italic}
.tags{margin:6px 0 10px}
.tag{background:#21262d;padding:3px 9px;border-radius:4px;font-size:11px;margin-right:6px;display:inline-block;margin-bottom:4px}
.tag.surge{color:#f85149;background:#f8514922}
.tag.drop{color:#58a6ff;background:#58a6ff22}
.row{display:flex;gap:16px;align-items:flex-start}
.col-img{flex:0 0 460px}
.col-img img{width:100%;border:1px solid #30363d;border-radius:4px;background:white}
.col-table{flex:1}
.col-table table{border-collapse:collapse;width:100%;font-size:12px}
.col-table th{background:#21262d;color:#58a6ff;text-align:left;padding:5px 8px}
.col-table td{padding:4px 8px;border-bottom:1px solid #21262d}
tr.highlight-surge{background:#f8514922;font-weight:bold}
tr.highlight-drop{background:#58a6ff22;font-weight:bold}
tr.subt td{border-top:1px solid #30363d}
a{color:#58a6ff}
.pdf-link{margin-top:8px;font-size:12px}
.nopdf{background:#30363d;padding:20px;text-align:center;border-radius:4px;color:#8b949e}
.summary{background:#161b22;padding:12px;border-radius:6px;margin-bottom:16px}
</style></head><body>
<h1>Auditoria visual ONPE 2026 — mesas con outlier_local</h1>
<div class="summary">
  <p>Cada tarjeta muestra la <b>columna de votos del acta escaneada</b> (imagen) y los <b>valores reportados por la API</b> (tabla). Las filas resaltadas en rojo (surge) o azul (drop) son las que el detector outlier_local flaggeo como anomalas.</p>
  <p>Para cada mesa, verifica manualmente si los numeros manuscritos en la imagen coinciden con los valores de la API. Si no coinciden, esa mesa tiene una discrepancia real entre papel y sistema.</p>
  <p><span class="tag" style="background:#f8514922;color:#f85149">Priority 1</span> mesas con surge+drop (posible transposicion)
     <span class="tag" style="background:#f0883e22;color:#f0883e">Priority 2</span> surge puro
     <span class="tag" style="background:#ffd33d22;color:#ffd33d">Priority 3</span> drop puro</p>
</div>
"""
    parts = [header]
    parts.append(f'<p>Generado: {datetime.now(timezone.utc).isoformat(timespec="seconds")}. '
                 f'{len(prioritized)} mesas (snap {snap_id}).</p>')

    for i, (codigo, pri, z) in enumerate(prioritized, 1):
        if i % 20 == 0:
            print(f"  [{i}/{len(prioritized)}] procesando...")
        hits = anomaly_hits.get(codigo, [])
        block, _ = render_mesa_block(codigo, hits, conn, snap_id, nom_map, geo_cache)
        # Inject priority class
        block = block.replace('<div class="card">', f'<div class="card p{pri}">', 1)
        parts.append(block)

    parts.append('</body></html>')
    out_html.write_text("\n".join(parts), encoding="utf-8")
    print(f"\n[out] {out_html}")
    conn.close()


if __name__ == "__main__":
    main()
