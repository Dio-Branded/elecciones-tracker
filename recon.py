"""
Reconocimiento: abre ONPE, captura todos los XHR/fetch calls, guarda URLs + payloads.
Solo se corre una vez para entender la API real.
"""
import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent / "logs" / "recon.jsonl"
OUT.parent.mkdir(exist_ok=True)

def main():
    captured = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        )
        page = ctx.new_page()

        def on_response(resp):
            url = resp.url
            if "presentacion-backend" not in url and "onpe" not in url:
                return
            ctype = resp.headers.get("content-type", "")
            if "json" not in ctype and "presentacion-backend" not in url:
                return
            try:
                body = resp.text()
            except Exception as e:
                body = f"<error: {e}>"
            entry = {
                "url": url,
                "status": resp.status,
                "method": resp.request.method,
                "content_type": ctype,
                "body_preview": body[:400],
                "body_len": len(body),
            }
            captured.append(entry)
            print(f"[{resp.status}] {resp.request.method} {url[:110]} ({len(body)}b)")

        page.on("response", on_response)

        print("Navigating to ONPE...")
        page.goto("https://resultadoelectoral.onpe.gob.pe/", wait_until="networkidle", timeout=60000)
        print("Waiting 5s for background XHRs...")
        time.sleep(5)

        browser.close()

    with OUT.open("w", encoding="utf-8") as f:
        for e in captured:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"\nSaved {len(captured)} requests to {OUT}")

if __name__ == "__main__":
    main()
