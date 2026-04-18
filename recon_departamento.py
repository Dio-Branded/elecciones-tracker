"""
Recon avanzado: descubrir el endpoint para desagregar presidencial por
departamento/region. Navega por la SPA a la vista nacional y luego fuerza
selecciones de region via la UI / URL path.
"""
import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent / "logs" / "recon_departamento.jsonl"
OUT.parent.mkdir(exist_ok=True)
URL = "https://resultadoelectoral.onpe.gob.pe/"


def main():
    captured = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        )
        page = ctx.new_page()

        def on_response(resp):
            u = resp.url
            if "presentacion-backend" not in u:
                return
            try:
                body = resp.text()
            except Exception:
                body = ""
            captured.append({
                "url": u,
                "status": resp.status,
                "method": resp.request.method,
                "body_len": len(body),
                "body_preview": body[:500],
            })
            print(f"[{resp.status}] {u[:140]}")

        page.on("response", on_response)

        print("=== 1) Landing ===")
        page.goto(URL, wait_until="networkidle", timeout=60000)
        time.sleep(3)

        print("\n=== 2) Presidenciales (nacional) ===")
        page.goto(URL + "main/presidenciales", wait_until="networkidle", timeout=30000)
        time.sleep(3)

        # Try to explore URL patterns
        trial_paths = [
            "main/presidenciales/departamento/1",
            "main/presidenciales/departamento/AMAZONAS",
            "main/presidenciales/region/1",
            "main/presidenciales/distrito/15",
            "main/presidenciales/ubigeo/140000",
            "main/presidenciales/departamentos",
        ]
        for path in trial_paths:
            print(f"\n=== Trying /{path} ===")
            try:
                page.goto(URL + path, wait_until="networkidle", timeout=20000)
                time.sleep(2)
            except Exception as e:
                print(f"  err: {e}")

        # Explore API with different tipoFiltro values
        print("\n=== 3) Probando tipoFiltro alternativos via fetch ===")
        trials = [
            "/presentacion-backend/resumen-general/participantes?idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento=010000",
            "/presentacion-backend/resumen-general/participantes?idEleccion=10&tipoFiltro=departamento&idDepartamento=1",
            "/presentacion-backend/resumen-general/participantes?idEleccion=10&tipoFiltro=region&idRegion=1",
            "/presentacion-backend/resumen-general/participantes?idEleccion=10&tipoFiltro=ubigeo&idUbigeo=010000",
            "/presentacion-backend/resumen-general/participantes?idEleccion=10&tipoFiltro=ubigeoDepartamento&idUbigeoDepartamento=010000",
            "/presentacion-backend/resumen-general/participantes?idAmbitoGeografico=2&idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento=010000",
            "/presentacion-backend/resumen-general/participantes?idAmbitoGeografico=3&idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento=010000",
            "/presentacion-backend/departamento/departamentos",
            "/presentacion-backend/ubigeo/departamentos",
            "/presentacion-backend/ambito/departamentos",
        ]
        for t in trials:
            try:
                r = page.evaluate(
                    """async (url) => {
                        const x = await fetch(url, {credentials: 'same-origin'});
                        const txt = await x.text();
                        return {status: x.status, body: txt.slice(0, 500), len: txt.length};
                    }""",
                    t,
                )
                print(f"[{r['status']}] len={r['len']} {t}")
                if r['status'] == 200 and r['len'] > 30:
                    print(f"    PREVIEW: {r['body'][:200]}")
            except Exception as e:
                print(f"  err {t}: {e}")

        browser.close()

    OUT.write_text("\n".join(json.dumps(c, ensure_ascii=False) for c in captured), encoding="utf-8")
    print(f"\nSaved {len(captured)} calls to {OUT}")


if __name__ == "__main__":
    main()
