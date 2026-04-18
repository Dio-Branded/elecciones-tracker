"""Calibrate ONPE acta template coordinates.

Takes 3 sample PDFs, extracts the image, draws overlays for the 42 detected
rows (38 parties + 4 totals) and the vote column, and saves them to
C:\\tmp\\layout_debug\\ for visual inspection.

Usage:
  python calibrate_acta_layout.py                   # default: 3 pilot mesas
  python calibrate_acta_layout.py --codigo 1,100,54938
"""
from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path

import cv2
import numpy as np

from ocr_trocr import pdf_to_image


def crop_votes_column(img: np.ndarray, y0: int, y1: int, pad_y: int = 8) -> np.ndarray:
    """Crop the votes column for one row with vertical padding for OCR slack."""
    H, W = img.shape[:2]
    x0 = int(W * 0.300)
    x1 = int(W * 0.395)
    return img[max(0, y0 - pad_y):min(H, y1 + pad_y), x0:x1]


def detect_rows_adaptive(img: np.ndarray) -> list[tuple[int, int, str]]:
    """Detect the 42 rows (38 parties + 4 totals) adaptively using horizontal lines.

    Strategy:
      1. Find the vote column zone (right side of the table, ~x=0.30..0.40)
      2. Convert to grayscale, binarize, and detect horizontal lines via morphology
      3. Keep lines that span the vote column and are stable in the middle section
      4. Cluster / deduplicate lines, then pick the 43 that split into 42 rows
      5. Fall back to hardcoded proportions if detection fails
    """
    H, W = img.shape[:2]
    # Vote column zone x range
    x0 = int(W * 0.28)
    x1 = int(W * 0.40)
    zone = img[:, x0:x1]
    gray = cv2.cvtColor(zone, cv2.COLOR_BGR2GRAY)
    # Binarize inverted (lines become white)
    bw = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
                                cv2.THRESH_BINARY_INV, 15, -2)
    # Extract horizontal lines using a wide horizontal kernel
    hk = cv2.getStructuringElement(cv2.MORPH_RECT, (zone.shape[1] // 2, 1))
    hlines = cv2.morphologyEx(bw, cv2.MORPH_OPEN, hk)
    # Row-sum projection: each y with many white pixels is a line
    row_sum = hlines.sum(axis=1)
    # Threshold: rows with >= 60% of zone width are lines
    line_thresh = zone.shape[1] * 255 * 0.60
    line_ys = np.where(row_sum >= line_thresh)[0]
    # Merge consecutive ys (lines have thickness)
    merged = []
    for y in line_ys:
        if not merged or y - merged[-1][-1] > 3:
            merged.append([y])
        else:
            merged[-1].append(y)
    ys = [int(np.mean(m)) for m in merged]

    # Expect 43 lines (42 rows between them). If <20, fallback to proportions.
    if len(ys) < 30:
        return _fallback_proportions(img)

    # Keep only the main contiguous block: filter to the zone y~[0.15, 0.80]
    y_min = int(H * 0.12)
    y_max = int(H * 0.82)
    ys = [y for y in ys if y_min <= y <= y_max]
    if len(ys) < 30:
        return _fallback_proportions(img)

    # Find the largest sequential run where consecutive diffs are similar
    diffs = np.diff(ys)
    # Typical row height ~ median of all diffs
    median_diff = np.median(diffs)
    # Keep lines where the gap to the next one is within [0.5, 2.0] * median
    rows = []
    for i in range(len(ys) - 1):
        y0 = ys[i]
        y1 = ys[i + 1]
        gap = y1 - y0
        if 0.5 * median_diff <= gap <= 2.0 * median_diff:
            rows.append((y0, y1))
    # Take the longest consecutive run
    if not rows:
        return _fallback_proportions(img)

    # The first 38 rows are parties, next 4 are totals
    labels = []
    for i in range(min(len(rows), 38)):
        labels.append(f"partido_{i+1}")
    tot_labels = ["blancos", "nulos", "impugnados", "emitidos"]
    for i, tlabel in enumerate(tot_labels):
        idx = 38 + i
        if idx < len(rows):
            labels.append(tlabel)
    out = [(r[0], r[1], lab) for r, lab in zip(rows[:42], labels)]
    if len(out) < 38:
        return _fallback_proportions(img)
    return out


def _fallback_proportions(img: np.ndarray) -> list[tuple[int, int, str]]:
    """Hardcoded proportions calibrated via visual inspection of 3280x5080 overlay.

    Partidos: y=0.175..0.655 (38 rows equidistantes, row_h ~0.0126 = ~64 px)
    Totales:  y=0.660..0.720 (4 rows)
    """
    H = img.shape[0]
    rows = []
    y0_partidos = int(H * 0.175)
    y1_partidos = int(H * 0.655)
    row_h = (y1_partidos - y0_partidos) / 38
    for i in range(38):
        y0 = int(y0_partidos + i * row_h)
        y1 = int(y0_partidos + (i + 1) * row_h)
        rows.append((y0, y1, f"partido_{i+1}"))
    y0_totales = int(H * 0.660)
    y1_totales = int(H * 0.720)
    row_h_t = (y1_totales - y0_totales) / 4
    labels = ["blancos", "nulos", "impugnados", "emitidos"]
    for i, label in enumerate(labels):
        y0 = int(y0_totales + i * row_h_t)
        y1 = int(y0_totales + (i + 1) * row_h_t)
        rows.append((y0, y1, label))
    return rows


# Use proportional fallback only (adaptive detector is fragile on this template)
detect_rows = _fallback_proportions

DEBUG_DIR = Path("C:/tmp/layout_debug")
PDF_DIR = Path(__file__).parent / "data" / "actas_pdfs"


def overlay_rows(img: np.ndarray, rows: list[tuple[int, int, str]]) -> np.ndarray:
    """Draws row boundaries + vote-column rectangle on a copy of img."""
    out = img.copy()
    H, W = img.shape[:2]
    x0 = int(W * 0.300)
    x1 = int(W * 0.395)
    # Vote column outline
    cv2.rectangle(out, (x0, rows[0][0]), (x1, rows[-1][1]), (0, 255, 0), 3)
    # Each row line + label
    for i, (y0, y1, label) in enumerate(rows):
        color = (0, 0, 255) if "partido" in label else (255, 128, 0)
        cv2.line(out, (x0 - 40, y0), (x1 + 40, y0), color, 1)
        cv2.putText(out, label, (max(0, x0 - 200), y0 + 18),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1)
    # Close bottom of last row
    cv2.line(out, (x0 - 40, rows[-1][1]), (x1 + 40, rows[-1][1]), (255, 128, 0), 1)
    return out


def calibrate_one(codigo: int, debug_dir: Path) -> dict:
    padded = f"{codigo:06d}"
    pdf = PDF_DIR / f"{padded}_1_ACTA_DE_ESCRUTINIO.pdf"
    if not pdf.exists():
        return {"codigo": codigo, "error": "no ACTA DE ESCRUTINIO"}

    img = pdf_to_image(pdf)
    H, W = img.shape[:2]
    rows = detect_rows(img)

    # Overlay
    overlayed = overlay_rows(img, rows)
    out_overlay = debug_dir / f"{padded}_overlay.jpg"
    # Resize for viewing (orig ~3292 wide is huge)
    if W > 1600:
        scale = 1600 / W
        overlayed_small = cv2.resize(overlayed, (int(W * scale), int(H * scale)))
        cv2.imwrite(str(out_overlay), overlayed_small,
                    [cv2.IMWRITE_JPEG_QUALITY, 80])
    else:
        cv2.imwrite(str(out_overlay), overlayed, [cv2.IMWRITE_JPEG_QUALITY, 80])

    # Save a few sample crops for the vote column
    sample_rows = [0, 14, 32, 35, 38, 41]  # partido_1, partido_15, partido_33, partido_36, total_blancos, total_emitidos
    sample_crops_dir = debug_dir / f"{padded}_cells"
    sample_crops_dir.mkdir(exist_ok=True)
    for idx in sample_rows:
        if idx >= len(rows):
            continue
        y0, y1, label = rows[idx]
        cell = crop_votes_column(img, y0, y1)
        cv2.imwrite(str(sample_crops_dir / f"row_{idx:02d}_{label}.png"), cell)

    return {
        "codigo": codigo,
        "pdf": str(pdf),
        "image_shape": (H, W),
        "rows_detected": len(rows),
        "overlay": str(out_overlay),
        "sample_crops_dir": str(sample_crops_dir),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codigo", default="54938,55452,1",
                    help="Mesas to calibrate, comma-separated (default: 54938,55452,1)")
    args = ap.parse_args()

    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    codigos = [int(x) for x in args.codigo.split(",")]

    print(f"[calibrate] {len(codigos)} mesas -> {DEBUG_DIR}")
    for c in codigos:
        result = calibrate_one(c, DEBUG_DIR)
        if "error" in result:
            print(f"  mesa {c:06d}: ERR {result['error']}")
        else:
            H, W = result["image_shape"]
            print(f"  mesa {c:06d}: img={W}x{H} rows={result['rows_detected']} overlay={result['overlay']}")
            print(f"    cell crops -> {result['sample_crops_dir']}")


if __name__ == "__main__":
    main()
