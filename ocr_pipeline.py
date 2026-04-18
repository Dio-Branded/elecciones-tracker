"""OCR pipeline: compara lo que dice el acta escaneada vs la API ONPE.

Para cada mesa sospechosa (segun anomalies_report), lee SOLO las celdas que importan:
  - La fila del agrup flaggeada por outlier_local (surge/drop)
  - 2-3 filas "control" de los partidos con mas votos segun la API (para calibrar OCR)
  - Las 4 filas de totales (blancos, nulos, impugnados, emitidos)

No lee las 38 filas completas — eso seria costoso y ruidoso.

Output:
  data/ocr_pipeline_YYYYMMDD_HHMMSS.json
  data/ocr_cells/{codigo}_{rowtype}.png   (una por celda leida, para el reporte HTML)

Uso:
  python ocr_pipeline.py                                   # todas las mesas del report
  python ocr_pipeline.py --codigo 54938,55452,1            # mesas especificas
  python ocr_pipeline.py --max 20                          # primeras 20 sospechosas
  python ocr_pipeline.py --debug-rows 54938                # corre OCR sobre TODAS las filas de 1 mesa (debug)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np

from db import get_conn
from ocr_trocr import pdf_to_image

PDF_DIR = Path(__file__).parent / "data" / "actas_pdfs"
OUT_DIR = Path(__file__).parent / "data"
CELLS_DIR = OUT_DIR / "ocr_cells"
ID_ELECCION = 10

# Calibrated for scanned ONPE 2026 acta template (~3280x5080 jpeg).
# Verified visually 2026-04-18 that rows align with party cells when plotted.
PROP_Y0_PARTIDOS = 0.210
PROP_Y1_PARTIDOS = 0.777
PROP_Y0_TOTALES = 0.790
PROP_Y1_TOTALES = 0.872
PROP_X0_VOTOS = 0.300
PROP_X1_VOTOS = 0.395
PAD_Y = 8


def build_rows(img: np.ndarray) -> list[tuple[int, int, str]]:
    H = img.shape[0]
    rows = []
    y0 = int(H * PROP_Y0_PARTIDOS)
    y1 = int(H * PROP_Y1_PARTIDOS)
    h = (y1 - y0) / 38
    for i in range(38):
        rows.append((int(y0 + i * h), int(y0 + (i + 1) * h), f"partido_{i+1}"))
    ty0 = int(H * PROP_Y0_TOTALES)
    ty1 = int(H * PROP_Y1_TOTALES)
    th = (ty1 - ty0) / 4
    for i, lab in enumerate(["blancos", "nulos", "impugnados", "emitidos"]):
        rows.append((int(ty0 + i * th), int(ty0 + (i + 1) * th), lab))
    return rows


def crop_cell(img: np.ndarray, y0: int, y1: int) -> np.ndarray:
    H, W = img.shape[:2]
    x0 = int(W * PROP_X0_VOTOS)
    x1 = int(W * PROP_X1_VOTOS)
    return img[max(0, y0 - PAD_Y):min(H, y1 + PAD_Y), x0:x1]


_DIGITS_RE = re.compile(r"\d+")


def parse_digits(text: str) -> int | None:
    m = _DIGITS_RE.search(text.replace("O", "0").replace("o", "0"))
    if not m:
        return None
    return int(m.group(0))


def _load_ocr():
    """Load Tesseract (cheap) + TrOCR-base-printed (1GB, higher quality).

    Tesseract handles printed digits well; TrOCR-printed handles semi-cursive digits.
    Ensemble logic in ocr_cell: if both agree, high confidence; if not, lower.
    """
    import os
    os.environ.setdefault("USE_TF", "0")
    os.environ.setdefault("USE_TORCH", "1")
    import torch
    from transformers import TrOCRProcessor, VisionEncoderDecoderModel
    print("[trocr] cargando trocr-base-printed (~1GB primera vez)...")
    name = "microsoft/trocr-base-printed"
    processor = TrOCRProcessor.from_pretrained(name)
    model = VisionEncoderDecoderModel.from_pretrained(name)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device).eval()
    print(f"[trocr] cargado en {device}")
    return {"trocr": (processor, model, device), "torch": torch}


def ocr_tesseract(cell: np.ndarray) -> tuple[int | None, float, str]:
    """Tesseract with digits-only whitelist. Returns (value, conf, raw)."""
    import pytesseract
    # Preprocess: grayscale + upscale + binarize
    H = cell.shape[0]
    if H < 96:
        scale = 96 / H
        cell = cv2.resize(cell, (int(cell.shape[1] * scale), 96),
                          interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(cell, cv2.COLOR_BGR2GRAY)
    # Config: PSM 7 = single line, only digits
    cfg = "--psm 7 -c tessedit_char_whitelist=0123456789"
    try:
        data = pytesseract.image_to_data(gray, config=cfg,
                                          output_type=pytesseract.Output.DICT)
    except Exception:
        return None, 0.0, ""
    best_text, best_conf = "", -1
    for i, t in enumerate(data.get("text", [])):
        conf = float(data["conf"][i]) if data["conf"][i] != "-1" else -1
        if t.strip() and conf > best_conf:
            best_text, best_conf = t.strip(), conf
    if best_conf < 0:
        return None, 0.0, ""
    val = parse_digits(best_text)
    # Tesseract conf is 0-100; normalize to 0-1
    return val, best_conf / 100.0, best_text


def ocr_trocr_printed(cell: np.ndarray, bundle: dict) -> tuple[int | None, float, str]:
    """TrOCR printed model."""
    from PIL import Image
    processor, model, device = bundle["trocr"]
    torch = bundle["torch"]
    H = cell.shape[0]
    if H < 96:
        scale = 96 / H
        cell = cv2.resize(cell, (int(cell.shape[1] * scale), 96),
                          interpolation=cv2.INTER_CUBIC)
    rgb = cv2.cvtColor(cell, cv2.COLOR_BGR2RGB)
    pil = Image.fromarray(rgb)
    pixel_values = processor(images=pil, return_tensors="pt").pixel_values.to(device)
    with torch.no_grad():
        gen = model.generate(pixel_values, max_length=16,
                             output_scores=True, return_dict_in_generate=True)
    text = processor.batch_decode(gen.sequences, skip_special_tokens=True)[0].strip()
    conf = 0.0
    if gen.scores:
        # Mean of top-token probabilities
        conf = sum(s.softmax(dim=-1).max(dim=-1).values.item()
                   for s in gen.scores) / max(len(gen.scores), 1)
    val = parse_digits(text)
    return val, float(conf), text


def ocr_cell(ocr, cell: np.ndarray) -> tuple[int | None, float, str]:
    """Ensemble: Tesseract + TrOCR-printed.

    - If both read same number -> high confidence (avg of two)
    - If only one reads -> that one, with halved confidence
    - If both differ -> lower of the two, confidence halved
    """
    t_val, t_conf, t_raw = ocr_tesseract(cell)
    tr_val, tr_conf, tr_raw = ocr_trocr_printed(cell, ocr)
    raw = f"tess='{t_raw}' trocr='{tr_raw}'"
    if t_val is not None and tr_val is not None:
        if t_val == tr_val:
            return t_val, min(1.0, (t_conf + tr_conf) / 2 + 0.1), raw
        # Disagree: pick the higher-confidence one, but reduce confidence
        if tr_conf > t_conf:
            return tr_val, tr_conf * 0.5, raw
        return t_val, t_conf * 0.5, raw
    if t_val is not None:
        return t_val, t_conf * 0.6, raw
    if tr_val is not None:
        return tr_val, tr_conf * 0.6, raw
    return None, 0.0, raw


def get_api_data(conn, codigo: int, snap_id: int) -> dict:
    acta = conn.execute(
        "SELECT total_votos_validos, total_votos_emitidos, electores_habiles, "
        "votos_blancos, votos_nulos, raw_json "
        "FROM actas WHERE snapshot_id=? AND codigo=? AND id_eleccion=?",
        (snap_id, codigo, ID_ELECCION),
    ).fetchone()
    if not acta:
        return {}
    votos = {r[0]: r[1] for r in conn.execute(
        "SELECT codigo_agrupacion, votos FROM acta_votos "
        "WHERE snapshot_id=? AND codigo=? AND id_eleccion=?",
        (snap_id, codigo, ID_ELECCION),
    )}
    # impugnados puede estar en raw_json
    impugnados = None
    try:
        rj = json.loads(acta[5] or "{}")
        impugnados = rj.get("votosImpugnados") or rj.get("impugnados")
    except Exception:
        pass
    return {
        "votos": votos,
        "validos": acta[0], "emitidos": acta[1], "electores": acta[2],
        "blancos": acta[3], "nulos": acta[4], "impugnados": impugnados,
    }


# Map agrup (codigo_agrupacion ONPE) -> fila fisica del acta (1-indexed)
# Vinculo agrup <-> fila se deriva del orden nacional de agrupaciones.
# Para el template ONPE 2026 hay que mapear por inspeccion de un acta muestra.
# Aqui asumimos que la fila fisica == codigo_agrupacion cuando ambos estan en [1,38].
# TODO: confirmar este mapping con un acta escaneada leyendo OCR de nombres.
def agrup_to_row_idx(agrup: int) -> int | None:
    """Devuelve el indice 0-based dentro de la lista build_rows()."""
    if 1 <= agrup <= 38:
        return agrup - 1
    return None


def select_cells_to_read(codigo: int, anomaly_hits: list[dict],
                          api_data: dict) -> list[tuple[str, int | None, int]]:
    """Returns list of (label, api_value, row_idx) to OCR for this mesa."""
    cells = []
    # 1. Celdas flaggeadas por outlier_local
    for hit in anomaly_hits:
        agrup = hit.get("codigo_agrupacion")
        if agrup is None:
            continue
        row_idx = agrup_to_row_idx(agrup)
        if row_idx is None:
            continue
        api_v = api_data["votos"].get(agrup, 0)
        cells.append((f"agrup_{agrup}_{hit['detalle']['subtipo']}", api_v, row_idx))
    # 2. Celdas control: top 3 agrups con mas votos en la API (no ya incluidos)
    included_agrups = {int(c[0].split("_")[1]) for c in cells if c[0].startswith("agrup_")}
    top_by_votes = sorted(api_data["votos"].items(), key=lambda kv: kv[1] or 0, reverse=True)
    added_ctrl = 0
    for agrup, v in top_by_votes:
        if agrup in included_agrups or agrup_to_row_idx(agrup) is None:
            continue
        cells.append((f"ctrl_{agrup}", v, agrup_to_row_idx(agrup)))
        added_ctrl += 1
        if added_ctrl >= 3:
            break
    # 3. Totales (los 4 de abajo: rows 38-41 en 0-indexed)
    totales = [
        ("total_blancos", api_data["blancos"], 38),
        ("total_nulos", api_data["nulos"], 39),
        ("total_impugnados", api_data["impugnados"], 40),
        ("total_emitidos", api_data["emitidos"], 41),
    ]
    cells.extend(totales)
    return cells


def find_pdf(codigo: int) -> Path | None:
    padded = f"{codigo:06d}"
    candidates = sorted(PDF_DIR.glob(f"{padded}_1_ACTA_DE_ESCRUTINIO.pdf"))
    if candidates:
        return candidates[0]
    # Fallback: cualquier PDF con ese codigo
    any_candidates = sorted(PDF_DIR.glob(f"{padded}_*.pdf"))
    return any_candidates[0] if any_candidates else None


def process_mesa(conn, ocr, codigo: int, anomaly_hits: list[dict], snap_id: int,
                 save_cells: bool = True) -> dict:
    pdf = find_pdf(codigo)
    if pdf is None:
        return {"codigo": codigo, "error": "no pdf"}
    api = get_api_data(conn, codigo, snap_id)
    if not api:
        return {"codigo": codigo, "error": "no api data"}

    img = pdf_to_image(pdf)
    rows = build_rows(img)

    cells_plan = select_cells_to_read(codigo, anomaly_hits, api)
    records = []
    for label, api_v, row_idx in cells_plan:
        y0, y1, _ = rows[row_idx]
        cell_img = crop_cell(img, y0, y1)
        if save_cells:
            out_png = CELLS_DIR / f"{codigo:06d}_{label}.png"
            cv2.imwrite(str(out_png), cell_img)
        ocr_val, conf, raw = ocr_cell(ocr, cell_img)
        # Decision logic
        if ocr_val is None:
            match = "unknown"
        elif api_v is None:
            match = "unknown"
        elif conf < 0.5:
            match = "unknown"
        elif ocr_val == api_v:
            match = "ok"
        else:
            match = "discrepancy"
        records.append({
            "label": label, "row_idx": row_idx,
            "api_value": api_v, "ocr_value": ocr_val,
            "ocr_confidence": round(conf, 3), "ocr_raw": raw,
            "match": match,
            "cell_path": str(out_png) if save_cells else None,
        })
    return {"codigo": codigo, "pdf": str(pdf), "records": records}


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codigo", default=None,
                    help="Codigos separados por coma (sobrescribe seleccion automatica)")
    ap.add_argument("--max", type=int, default=None,
                    help="Max mesas a procesar")
    ap.add_argument("--debug-rows", type=int, default=None,
                    help="Codigo a procesar leyendo TODAS las 42 filas (debug)")
    args = ap.parse_args()

    CELLS_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    snap_id = conn.execute(
        "SELECT id FROM actas_snapshots WHERE modo='full' AND actas_ok>=60000 "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]

    ocr = _load_ocr()

    if args.debug_rows:
        codigo = args.debug_rows
        pdf = find_pdf(codigo)
        if not pdf:
            print(f"no pdf para {codigo}"); sys.exit(1)
        api = get_api_data(conn, codigo, snap_id)
        img = pdf_to_image(pdf)
        rows = build_rows(img)
        print(f"\nmesa {codigo:06d} — {len(rows)} filas")
        print(f"{'row':>4} {'label':>15} {'api':>5} {'ocr':>5} {'conf':>5} {'raw':>10}")
        for i, (y0, y1, label) in enumerate(rows):
            cell = crop_cell(img, y0, y1)
            out = CELLS_DIR / f"debug_{codigo:06d}_{i:02d}.png"
            cv2.imwrite(str(out), cell)
            ocr_v, conf, raw = ocr_cell(ocr, cell)
            if i < 38:
                api_v = api["votos"].get(i + 1)  # asumiendo agrup = fila
            else:
                api_v = [api["blancos"], api["nulos"], api["impugnados"], api["emitidos"]][i - 38]
            print(f"{i:>4} {label:>15} {str(api_v):>5} {str(ocr_v):>5} {conf:>5.2f} {raw[:10]:>10}")
        conn.close()
        return

    anomaly_hits = load_anomaly_hits()
    if args.codigo:
        codigos = [int(x) for x in args.codigo.split(",")]
    else:
        codigos = sorted(anomaly_hits.keys())
    if args.max:
        codigos = codigos[:args.max]

    print(f"[pipeline] procesando {len(codigos)} mesas sobre snapshot {snap_id}")
    results = []
    for i, c in enumerate(codigos, 1):
        print(f"[{i}/{len(codigos)}] mesa {c:06d}")
        hits = anomaly_hits.get(c, [])
        res = process_mesa(conn, ocr, c, hits, snap_id)
        if "error" in res:
            print(f"  ERR {res['error']}")
        else:
            ok = sum(1 for r in res["records"] if r["match"] == "ok")
            disc = sum(1 for r in res["records"] if r["match"] == "discrepancy")
            unk = sum(1 for r in res["records"] if r["match"] == "unknown")
            print(f"  {len(res['records'])} celdas: ok={ok} discrepancy={disc} unknown={unk}")
        results.append(res)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"ocr_pipeline_{ts}.json"
    out.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snap_id,
        "n_mesas": len(results),
        "results": results,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[out] {out}")
    conn.close()


if __name__ == "__main__":
    main()
