"""
Verifica la denuncia de @primeinstitute_ del 17-abr-2026:
  "8,358 votos de Sanchez Palomino aparecen en el nacional de ONPE
   pero no existen en ninguna de las 26 regiones"

Cruza, por cada eleccion (Presidencial=10, Senadores=12, Diputados=13,
Senadores-distrito=14, Parlamento-Andino=15):
  total_nacional (tipoFiltro=eleccion)  vs  SUM(26 distritos electorales)

Reporta la diferencia por agrupacion y por candidato. Si hay discrepancia
>0 para un candidato con apellido "Sanchez Palomino", la denuncia se
replica con esta metodologia.
"""
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

URL_ONPE = "https://resultadoelectoral.onpe.gob.pe/"
OUT = Path(__file__).parent / "data" / f"verify_prime_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
OUT.parent.mkdir(exist_ok=True)

ELECCIONES = {
    10: ("presidencial", "/main/presidenciales"),
    12: ("senadores_nacional", "/main/senadores"),
    15: ("parlamento_andino", "/main/parlamento-andino"),
}
NUM_DISTRITOS = 26


def main():
    results = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        )
        page = ctx.new_page()

        captured = {}  # (eid, scope, distrito_id?) -> data

        def on_response(resp):
            u = resp.url
            if "presentacion-backend/resumen-general/participantes" not in u:
                return
            try:
                body = resp.json()
            except Exception:
                return
            if not body.get("success"):
                return
            q = dict(part.split("=", 1) for part in u.split("?", 1)[-1].split("&") if "=" in part)
            eid = int(q.get("idEleccion", 0))
            tipo = q.get("tipoFiltro", "")
            did = int(q.get("idDistritoElectoral", 0)) if q.get("idDistritoElectoral") else None
            if tipo == "eleccion":
                captured[(eid, "nacional", None)] = body["data"]
            elif tipo == "distrito_electoral" and did:
                captured[(eid, "distrito", did)] = body["data"]

        page.on("response", on_response)

        # 1) Landing — fuerza nacional para idEleccion=10
        print(f"[{datetime.now():%H:%M:%S}] GET landing")
        try:
            page.goto(URL_ONPE, wait_until="networkidle", timeout=60000)
        except PwTimeout:
            pass
        time.sleep(3)

        # 2) Navegar a cada eleccion para capturar el nacional
        for eid, (tipo_label, path) in ELECCIONES.items():
            if (eid, "nacional", None) in captured:
                continue
            try:
                print(f"[{datetime.now():%H:%M:%S}] GET nacional idEleccion={eid}")
                page.goto(URL_ONPE.rstrip("/") + path, wait_until="networkidle", timeout=45000)
                time.sleep(2)
            except PwTimeout:
                pass

        # 3) Para cada eleccion, navegar a los 26 distritos
        #    El SPA construye URLs tipo /main/<algo>/distrito/<id>
        #    Estrategia alternativa: llamar la API via page.evaluate fetch (hereda cookies/CSP del origen)
        for eid in ELECCIONES:
            for did in range(1, NUM_DISTRITOS + 1):
                if (eid, "distrito", did) in captured:
                    continue
                api = (f"/presentacion-backend/resumen-general/participantes"
                       f"?idAmbitoGeografico=1&idEleccion={eid}&tipoFiltro=distrito_electoral&idDistritoElectoral={did}")
                print(f"[{datetime.now():%H:%M:%S}] fetch eid={eid} distrito={did}")
                try:
                    resp_text = page.evaluate(
                        """async (url) => {
                            const r = await fetch(url, {credentials: 'same-origin', headers: {'Accept':'application/json'}});
                            return { status: r.status, body: await r.text() };
                        }""",
                        api,
                    )
                    if resp_text["status"] != 200:
                        print(f"  -> HTTP {resp_text['status']}")
                        continue
                    data = json.loads(resp_text["body"])
                    if data.get("success"):
                        captured[(eid, "distrito", did)] = data["data"]
                    else:
                        print(f"  -> success=false: {data.get('message','')[:100]}")
                except Exception as e:
                    print(f"  -> ERROR: {e}")
                    time.sleep(1)

        browser.close()

    # 4) Analizar: nacional vs sum(distritos) por agrupacion/candidato
    analysis = {}
    for eid, (tipo_label, _) in ELECCIONES.items():
        nacional = captured.get((eid, "nacional", None))
        if not nacional:
            print(f"\n[{tipo_label}] SIN nacional — skip")
            continue

        nat_votos = {}  # key=(codigo, candidato_dni) -> votos
        for p in nacional:
            key = (p.get("codigoAgrupacionPolitica"), p.get("dniCandidato") or "")
            nat_votos[key] = {
                "agrupacion": p.get("nombreAgrupacionPolitica"),
                "candidato": p.get("nombreCandidato"),
                "nacional": p.get("totalVotosValidos", 0),
                "regional_sum": 0,
                "distritos_con_datos": 0,
            }

        # Agregar distritos
        distritos_disp = []
        for did in range(1, NUM_DISTRITOS + 1):
            d_data = captured.get((eid, "distrito", did))
            if not d_data:
                continue
            distritos_disp.append(did)
            for p in d_data:
                key = (p.get("codigoAgrupacionPolitica"), p.get("dniCandidato") or "")
                if key not in nat_votos:
                    nat_votos[key] = {
                        "agrupacion": p.get("nombreAgrupacionPolitica"),
                        "candidato": p.get("nombreCandidato"),
                        "nacional": 0,
                        "regional_sum": 0,
                        "distritos_con_datos": 0,
                    }
                nat_votos[key]["regional_sum"] += p.get("totalVotosValidos", 0) or 0
                nat_votos[key]["distritos_con_datos"] += 1

        # Diferencia
        rows = []
        for key, v in nat_votos.items():
            diff = v["nacional"] - v["regional_sum"]
            rows.append({
                "agrupacion": v["agrupacion"],
                "candidato": v["candidato"],
                "nacional": v["nacional"],
                "regional_sum": v["regional_sum"],
                "diff_nacional_menos_regional": diff,
                "distritos_con_datos": v["distritos_con_datos"],
            })
        rows.sort(key=lambda r: abs(r["diff_nacional_menos_regional"]), reverse=True)

        analysis[tipo_label] = {
            "eid": eid,
            "distritos_disponibles": distritos_disp,
            "num_distritos": len(distritos_disp),
            "rows": rows,
        }

        print(f"\n=== {tipo_label.upper()} (idEleccion={eid}) — distritos capturados: {len(distritos_disp)}/26 ===")
        print(f"{'AGRUPACION':<45} {'CANDIDATO':<35} {'NAT':>10} {'REG_SUM':>10} {'DIFF':>8}")
        for r in rows[:15]:
            print(f"{(r['agrupacion'] or '')[:43]:<45} {(r['candidato'] or '')[:33]:<35} "
                  f"{r['nacional']:>10} {r['regional_sum']:>10} {r['diff_nacional_menos_regional']:>8}")

        # Flag: cualquier Sanchez Palomino
        for r in rows:
            name = (r["candidato"] or "").upper()
            if "SANCHEZ PALOMINO" in name or "SÁNCHEZ PALOMINO" in name:
                print(f"\n*** MATCH PRIME *** {name} | NAT={r['nacional']} REG_SUM={r['regional_sum']} DIFF={r['diff_nacional_menos_regional']}")

    OUT.write_text(json.dumps({
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "analysis": analysis,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nResultado JSON: {OUT}")


if __name__ == "__main__":
    main()
