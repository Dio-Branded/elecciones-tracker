"""
TrOCR handwritten pipeline para actas ONPE.

Estrategia:
  1. Extraer imagen del PDF (ya tenemos 20 PDFs en data/actas_pdfs/)
  2. Detectar cuadrícula del acta usando OpenCV (líneas horizontales)
  3. Recortar la columna de votos manuscritos (x~900-1200)
  4. Por cada fila (partido): pasar la celda recortada por TrOCR
  5. Comparar OCR vs API (que ya tenemos en DB)
  6. Reportar accuracy + discrepancias

Uso:
  python ocr_trocr.py                # procesa las 10 actas de escrutinio que tenemos
  python ocr_trocr.py --codigo 1     # solo mesa 1
  python ocr_trocr.py --debug        # guarda celdas recortadas a /tmp/ocr_debug/
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

# Force transformers to use only PyTorch (evita conflicto protobuf con TF)
os.environ["USE_TF"] = "0"
os.environ["USE_TORCH"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"

import cv2
import numpy as np
from PIL import Image

from db import get_conn

PDF_DIR = Path(__file__).parent / "data" / "actas_pdfs"
DEBUG_DIR = Path("C:/tmp/ocr_debug")
ID_ELECCION = 10


def load_trocr():
    """Carga el modelo TrOCR handwritten. Descarga ~1GB la primera vez."""
    print("[init] cargando TrOCR-base-handwritten (puede descargar ~1GB la primera vez)...")
    import torch
    from transformers import TrOCRProcessor, VisionEncoderDecoderModel

    name = "microsoft/trocr-base-handwritten"
    processor = TrOCRProcessor.from_pretrained(name)
    model = VisionEncoderDecoderModel.from_pretrained(name)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device).eval()
    print(f"[init] modelo cargado en {device}")
    return processor, model, device


def pdf_to_image(pdf_path: Path) -> np.ndarray:
    """Extrae la imagen del acta de un PDF.

    Estrategia:
      1. pdfimages -j para extraer imagenes embebidas. Si la mas grande es >=2000px
         de ancho (acta escaneada como JPEG unico), usarla.
      2. Si no hay imagen grande (PDF con tiles pequenos o PDF renderizado como
         texto+vectores), rasterizar con pdf2image a 300 DPI.
    """
    import subprocess
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        prefix = Path(td) / "page"
        subprocess.run(
            ["pdfimages", "-j", "-p", str(pdf_path), str(prefix)],
            check=True, capture_output=True,
        )
        imgs = sorted(Path(td).glob("*.jpg")) + sorted(Path(td).glob("*.png"))
        # Elegir la imagen mas grande (por area)
        biggest = None
        biggest_area = 0
        for p in imgs:
            im = cv2.imread(str(p))
            if im is None:
                continue
            area = im.shape[0] * im.shape[1]
            if area > biggest_area:
                biggest_area = area
                biggest = im
        if biggest is not None and biggest.shape[1] >= 2000:
            return biggest
    # Fallback: rasterizar con pdf2image
    from pdf2image import convert_from_path
    pages = convert_from_path(str(pdf_path), dpi=300, first_page=1, last_page=1)
    if not pages:
        raise RuntimeError(f"no se pudo renderizar {pdf_path}")
    rgb = np.array(pages[0])
    return cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)


def detect_rows(img: np.ndarray) -> list[tuple[int, int, str]]:
    """Retorna filas hardcodeadas del template ONPE 2026.

    Basado en inspeccion de la imagen 3292x5104:
    - Partidos: y=0.236 .. y=0.725 (38 partidos, ~66 px cada uno)
    - Totales (blancos/nulos/impugnados/emitidos): y=0.725 .. y=0.790 (~4 rows)
    """
    H = img.shape[0]
    rows = []
    # 38 partidos
    y0_partidos = int(H * 0.236)
    y1_partidos = int(H * 0.725)
    row_h = (y1_partidos - y0_partidos) / 38
    for i in range(38):
        y0 = int(y0_partidos + i * row_h)
        y1 = int(y0_partidos + (i + 1) * row_h)
        rows.append((y0, y1, f"partido_{i+1}"))
    # 4 totales
    y0_totales = int(H * 0.725)
    y1_totales = int(H * 0.790)
    row_h_t = (y1_totales - y0_totales) / 4
    labels = ["blancos", "nulos", "impugnados", "emitidos"]
    for i, label in enumerate(labels):
        y0 = int(y0_totales + i * row_h_t)
        y1 = int(y0_totales + (i + 1) * row_h_t)
        rows.append((y0, y1, label))
    return rows


def crop_votes_column(img: np.ndarray, y0: int, y1: int) -> np.ndarray:
    """Recorta la zona de numeros manuscritos de una fila.

    La columna 'TOTAL DE VOTOS' en el template ONPE esta aprox en x=0.30..0.39 del ancho.
    En la imagen 3292x3292: x=988..1284
    """
    H, W = img.shape[:2]
    x0 = int(W * 0.300)
    x1 = int(W * 0.395)
    yp = 3
    return img[max(0, y0 - yp):min(H, y1 + yp), x0:x1]


def ocr_cell(cell: np.ndarray, processor, model, device) -> tuple[str, float]:
    """Pasa una celda por TrOCR. Retorna (texto, confidence_aprox)."""
    import torch
    # convert BGR->RGB, PIL Image
    rgb = cv2.cvtColor(cell, cv2.COLOR_BGR2RGB)
    pil = Image.fromarray(rgb)
    # resize to at least 384 alto para que TrOCR lo procese bien
    if pil.height < 96:
        scale = 96 / pil.height
        pil = pil.resize((int(pil.width * scale), 96), Image.LANCZOS)
    pixel_values = processor(images=pil, return_tensors="pt").pixel_values.to(device)
    with torch.no_grad():
        gen = model.generate(pixel_values, max_length=16,
                              output_scores=True, return_dict_in_generate=True)
    text = processor.batch_decode(gen.sequences, skip_special_tokens=True)[0]
    # confidence aproximada: exp del score promedio de los tokens
    if gen.scores:
        mean_logprob = sum(
            s.softmax(dim=-1).max(dim=-1).values.item() for s in gen.scores
        ) / max(len(gen.scores), 1)
        conf = float(mean_logprob)
    else:
        conf = 0.0
    return text.strip(), conf


_DIGITS_RE = re.compile(r"[0-9]+")


def parse_number(ocr_text: str) -> int | None:
    """Extrae el primer grupo de dígitos del texto OCR. Devuelve None si no hay."""
    m = _DIGITS_RE.search(ocr_text)
    if not m:
        return None
    return int(m.group(0))


def get_api_votes(conn, codigo: int) -> dict:
    """Retorna {codigo_agrupacion: {nombre, votos}, '_blancos': int, '_nulos': int,
    '_emitidos': int, '_validos': int, '_electores': int, '_posicion': {codigo_agrupacion: num_orden}}"""
    acta = conn.execute(
        "SELECT total_votos_validos, total_votos_emitidos, electores_habiles, "
        "votos_blancos, votos_nulos "
        "FROM actas WHERE snapshot_id=4 AND codigo=? AND id_eleccion=?",
        (codigo, ID_ELECCION),
    ).fetchone()
    if not acta:
        return {}
    votos = {r[0]: r[1] for r in conn.execute(
        "SELECT codigo_agrupacion, votos FROM acta_votos "
        "WHERE snapshot_id=4 AND codigo=? AND id_eleccion=?",
        (codigo, ID_ELECCION),
    )}
    return {
        "votos": votos,
        "validos": acta[0], "emitidos": acta[1], "electores": acta[2],
        "blancos": acta[3], "nulos": acta[4],
    }


def process_acta(codigo: int, processor, model, device, conn, debug: bool = False):
    pdf = PDF_DIR / f"{codigo:06d}_1_ACTA_DE_ESCRUTINIO.pdf"
    if not pdf.exists():
        print(f"[{codigo}] PDF no existe")
        return None
    img = pdf_to_image(pdf)
    rows = detect_rows(img)
    if debug:
        DEBUG_DIR.mkdir(exist_ok=True)
        vis = img.copy()
        x0 = int(img.shape[1] * 0.300)
        x1 = int(img.shape[1] * 0.395)
        for y0, y1, label in rows:
            cv2.rectangle(vis, (x0, y0), (x1, y1), (0, 255, 0), 3)
            cv2.putText(vis, label, (x0 - 400, y0 + 40),
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
        cv2.imwrite(str(DEBUG_DIR / f"{codigo:06d}_rows.jpg"), vis)

    api = get_api_votes(conn, codigo)
    if not api:
        print(f"[{codigo}] no data en API")
        return None

    # OCR row by row
    results = []
    for i, (y0, y1, label) in enumerate(rows):
        cell = crop_votes_column(img, y0, y1)
        if debug and (i < 5 or label in ("blancos", "nulos", "emitidos")):
            cv2.imwrite(str(DEBUG_DIR / f"{codigo:06d}_{i:02d}_{label}.jpg"), cell)
        txt, conf = ocr_cell(cell, processor, model, device)
        num = parse_number(txt)
        results.append({"row_idx": i, "label": label,
                        "ocr_text": txt, "ocr_num": num, "conf": conf})

    return {"codigo": codigo, "rows_detected": len(rows), "results": results, "api": api}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codigo", type=int, default=None,
                    help="solo una mesa especifica")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    processor, model, device = load_trocr()
    conn = get_conn()

    codigos = [args.codigo] if args.codigo else list(range(1, 11))
    all_results = []
    t0 = time.time()
    for c in codigos:
        t = time.time()
        r = process_acta(c, processor, model, device, conn, debug=args.debug)
        if r:
            all_results.append(r)
            print(f"[{c:06d}] rows={r['rows_detected']} tiempo={time.time()-t:.1f}s")
    total_s = time.time() - t0
    print(f"\n[done] {len(all_results)} actas procesadas en {total_s:.1f}s "
          f"({total_s/max(len(all_results),1):.1f}s por acta)")

    # Reporte: cuantas lecturas OCR matchean alguno de los numeros API
    print("\n=== ANALISIS ===")
    print(f"{'mesa':>6} {'rows_det':>9} {'ocr_validos_hits':>18} {'detalle':<40}")
    for r in all_results:
        codigo = r["codigo"]
        api = r["api"]
        # Set de todos los numeros que DEBERIAN aparecer en el acta
        api_numbers = set(api["votos"].values()) | {api["blancos"], api["nulos"],
                                                      api["validos"], api["emitidos"],
                                                      api["electores"]}
        api_numbers.discard(0)
        # Numeros que OCR extrajo
        ocr_numbers = [x["ocr_num"] for x in r["results"] if x["ocr_num"] is not None]
        hits = [n for n in ocr_numbers if n in api_numbers]
        print(f"{codigo:>6} {r['rows_detected']:>9} "
              f"{len(hits):>4}/{len(ocr_numbers):<4} ({len(hits)/max(len(ocr_numbers),1)*100:.0f}%)   "
              f"{'API expects: '+str(sorted(api_numbers))[:60]}")

    # Detalle mesa 1 (muestra)
    if all_results:
        r = all_results[0]
        print(f"\n--- DETALLE mesa {r['codigo']:06d} ---")
        api = r["api"]
        print(f"API dice: validos={api['validos']} emitidos={api['emitidos']} "
              f"blancos={api['blancos']} nulos={api['nulos']}")
        # Mapping posicion acta -> codigo_agrupacion. En las actas ONPE, el orden
        # de las 38 filas coincide con la posicion de la agrupacion en el sorteo.
        # Para comparar OCR vs API por fila necesitamos saber que codigo_agrupacion
        # va en cada posicion. Por ahora, buscamos en la API si existe algun voto
        # que matchee el numero leido.
        expected_numbers = set(api["votos"].values()) | {api["blancos"], api["nulos"],
                                                          api["validos"], api["emitidos"],
                                                          api["electores"]}
        expected_numbers.discard(0)

        print("\nFila-por-fila OCR vs numero esperado:")
        for row in r["results"]:
            label = row["label"]
            ocr_num = row["ocr_num"]
            match = ""
            if ocr_num is not None and ocr_num in expected_numbers:
                match = f" -> MATCH {ocr_num} en API"
            elif label in ("blancos", "nulos", "emitidos"):
                api_val = api.get(label, api.get("votos_emitidos") if label == "emitidos" else None)
                if label == "emitidos":
                    api_val = api["emitidos"]
                match = f" -> API dice {api_val}"
            print(f"  {label:<14}: ocr='{row['ocr_text'][:40]}' -> {ocr_num} (conf={row['conf']:.2f}){match}")

    conn.close()


if __name__ == "__main__":
    main()
