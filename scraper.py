"""
Scraper ONPE — abre la SPA con Playwright, intercepta los XHR JSON
y guarda totales + lista de participantes por elección en SQLite.

Elecciones capturadas (ids descubiertos en recon.py):
  10 = Presidencial
  12 = Senadores Nacional
  13 = Diputados
  14 = Senadores (otra vista)
  15 = Parlamento Andino
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

from db import get_conn, insert_snapshot

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

ELECCIONES_TARGET = {10: "presidencial", 12: "senadores_nacional",
                     15: "parlamento_andino"}
URL_ONPE = "https://resultadoelectoral.onpe.gob.pe/"
NAV_TIMEOUT_MS = 60000
POST_NAV_WAIT_S = 6


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with (LOG_DIR / "scraper.log").open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def capture():
    """Launch Chromium, visit ONPE, return dict {idEleccion: {"totales": {}, "participantes": []}}."""
    captured = {eid: {"totales": None, "participantes": None} for eid in ELECCIONES_TARGET}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 900},
        )
        page = ctx.new_page()

        def on_response(resp):
            url = resp.url
            if "presentacion-backend/resumen-general/" not in url:
                return
            if "tipoFiltro=eleccion" not in url:
                return  # solo vista nacional "eleccion", no distrital
            try:
                body = resp.json()
            except Exception:
                return
            if not body.get("success"):
                return
            # parse idEleccion from query
            id_eleccion = None
            for part in url.split("?", 1)[-1].split("&"):
                if part.startswith("idEleccion="):
                    try:
                        id_eleccion = int(part.split("=", 1)[1])
                    except ValueError:
                        return
                    break
            if id_eleccion is None or id_eleccion not in ELECCIONES_TARGET:
                return
            if "/totales" in url:
                captured[id_eleccion]["totales"] = body["data"]
            elif "/participantes" in url:
                captured[id_eleccion]["participantes"] = body["data"]

        page.on("response", on_response)

        log(f"GET {URL_ONPE}")
        try:
            page.goto(URL_ONPE, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)
        except PwTimeout:
            log("networkidle timeout — sigo igual")

        # La SPA sólo pide idEleccion=10 en el landing. Navegamos a cada vista
        # para forzar las peticiones de Senado, Diputados y Parlamento Andino.
        deeplinks = {
            12: "/main/senadores",
            15: "/main/parlamento-andino",
        }
        for eid, path in deeplinks.items():
            if captured[eid]["totales"] and captured[eid]["participantes"]:
                continue
            try:
                log(f"Navegando a {path} (idEleccion={eid})")
                page.goto(URL_ONPE.rstrip("/") + path, wait_until="networkidle", timeout=30000)
                time.sleep(2)
            except PwTimeout:
                log(f"timeout cargando {path}")

        time.sleep(POST_NAV_WAIT_S)
        browser.close()

    return captured


def main():
    captured_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        data = capture()
    except Exception as e:
        log(f"ERROR captura: {e}")
        sys.exit(1)

    conn = get_conn()
    saved = 0
    for eid, tipo in ELECCIONES_TARGET.items():
        t = data[eid]["totales"]
        parts = data[eid]["participantes"] or []
        if not t:
            log(f"  [{tipo}] sin totales — skip")
            continue
        sid = insert_snapshot(conn, captured_at, eid, tipo, t, parts)
        pct = t.get("actasContabilizadas")
        log(f"  [{tipo}] snapshot id={sid} actas={pct}% candidatos={len(parts)}")
        saved += 1
    conn.close()
    log(f"OK — guardados {saved}/{len(ELECCIONES_TARGET)} snapshots")


if __name__ == "__main__":
    main()
