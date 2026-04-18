"""Strategy: iterar `/presentacion-backend/actas/buscar/mesa?...` por cada codigo.

Probamos ~20 variantes de parametros/headers porque investigaciones previas
dejaron tests sin agotar. Si alguna retorna data, iteramos codigo=1..999999.

Plan:
  1. probe(): establecer sesion Playwright, probar cada variante con codigos
     conocidos (1, 100, 50000, 86000 — que existen en el CSV de PRIME).
  2. Si al menos una variante retorna 200 con JSON no-vacio, el strategy
     queda 'available'.
  3. download(): iterar todos los codigos en paralelo con throttling.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from typing import Callable

from ._common import ActasStrategy, ProbeResult, ID_ELECCION_PRESIDENCIAL

ONPE_BASE = "https://resultadoelectoral.onpe.gob.pe"
INIT_URL = f"{ONPE_BASE}/main/presidenciales"

# Codigos conocidos validos segun CSV de PRIME (mesa=1..86000 todos existen ahi)
KNOWN_VALID_CODES = [1, 100, 1000, 50000, 86000]


def _variant_definitions():
    """Lista de (nombre_variante, method, path_template, body_template, headers_extra).

    Las templates usan {code} para el codigo numerico.
    """
    return [
        ("GET_q_codigo",           "GET",  "/presentacion-backend/actas/buscar/mesa?codigo={code}",       None, {}),
        ("GET_q_codigoMesa",       "GET",  "/presentacion-backend/actas/buscar/mesa?codigoMesa={code}",   None, {}),
        ("GET_q_codigoMesa_eid",   "GET",  "/presentacion-backend/actas/buscar/mesa?codigoMesa={code}&idEleccion=10", None, {}),
        ("GET_q_codigoMesa_amb",   "GET",  "/presentacion-backend/actas/buscar/mesa?codigoMesa={code}&idAmbitoGeografico=1", None, {}),
        ("GET_q_padded",           "GET",  "/presentacion-backend/actas/buscar/mesa?codigoMesa={code:06d}", None, {}),
        ("GET_path",               "GET",  "/presentacion-backend/actas/buscar/mesa/{code}",              None, {}),
        ("POST_body_codigoMesa",   "POST", "/presentacion-backend/actas/buscar/mesa",                     '{{"codigoMesa":{code}}}', {}),
        ("POST_body_codigo",       "POST", "/presentacion-backend/actas/buscar/mesa",                     '{{"codigo":{code}}}', {}),
        ("POST_body_padded",       "POST", "/presentacion-backend/actas/buscar/mesa",                     '{{"codigoMesa":"{code:06d}"}}', {}),
        # Variant v2 header
        ("GET_accept_v2",          "GET",  "/presentacion-backend/actas/buscar/mesa?codigoMesa={code}",   None, {"Accept": "application/vnd.onpe.v2+json"}),
    ]


async def _probe_via_playwright() -> list[dict]:
    """Retorna lista de resultados por (variante x codigo)."""
    from playwright.async_api import async_playwright

    variants = _variant_definitions()
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36")
        )
        page = await ctx.new_page()
        await page.goto(INIT_URL, wait_until="networkidle", timeout=45000)
        await asyncio.sleep(2)

        for name, method, path_tpl, body_tpl, hdrs in variants:
            for code in KNOWN_VALID_CODES:
                url = path_tpl.format(code=code)
                body = body_tpl.format(code=code) if body_tpl else None
                js = """async (args) => {
                    const opts = {
                        method: args.method,
                        credentials: 'same-origin',
                        headers: Object.assign({'Accept':'application/json','Content-Type':'application/json'}, args.hdrs),
                    };
                    if (args.body !== null) opts.body = args.body;
                    const r = await fetch(args.url, opts);
                    const t = await r.text();
                    return {status: r.status, len: t.length, body: t.slice(0, 1200)};
                }"""
                try:
                    r = await page.evaluate(js, {"method": method, "url": url, "body": body, "hdrs": hdrs})
                except Exception as e:
                    r = {"status": 0, "len": 0, "body": f"err: {e}"}
                results.append({"variant": name, "code": code, "method": method, "url": url, **r})

        await browser.close()
    return results


class MesaSearchStrategy(ActasStrategy):
    """Placeholder — solo activo si la probe encuentra una variante funcional."""
    name = "mesa_search"
    priority = 20  # preferido sobre prime_csv si funciona

    def __init__(self):
        self._working_variant: dict | None = None

    def probe(self) -> ProbeResult:
        try:
            results = asyncio.run(_probe_via_playwright())
        except Exception as e:
            return ProbeResult(False, f"playwright error: {e}")

        # Una variante se considera 'working' si al menos un codigo retorna 200
        # con body JSON no-vacio que parezca de acta (contiene 'votos' o 'codigoMesa').
        per_variant: dict[str, list] = {}
        for r in results:
            per_variant.setdefault(r["variant"], []).append(r)

        for vname, rs in per_variant.items():
            successes = [
                r for r in rs
                if r["status"] == 200 and r["len"] > 80
                and ("votos" in r["body"].lower() or "codigomesa" in r["body"].lower())
            ]
            if successes:
                self._working_variant = {"variant": vname, "samples": successes}
                return ProbeResult(True, f"variant {vname} works ({len(successes)} samples)",
                                   {"variant": vname, "first_body": successes[0]["body"][:800]})

        # Si ninguna variante funcionó, reportar detalle
        status_counts = {v: [r["status"] for r in rs] for v, rs in per_variant.items()}
        return ProbeResult(False, "ninguna variante devolvio data",
                           {"status_counts": status_counts})

    def download(self, conn, id_eleccion: int = ID_ELECCION_PRESIDENCIAL,
                 code_from: int = 1, code_to: int = 999999,
                 concurrency: int = 8, delay_ms: int = 80,
                 batch_db: int = 500, progress_every: int = 500):
        """Itera codigo 000001..999999 via la variante GET_q_padded.

        - concurrency: tareas asyncio paralelas (cuidar rate limits)
        - delay_ms: delay entre requests por cada worker
        - batch_db: flush cada N actas a SQLite
        """
        if self._working_variant is None:
            probe = self.probe()
            if not probe.ok:
                print(f"[{self.name}] probe FAIL: {probe.message}")
                return None

        return asyncio.run(_scrape_full(conn, id_eleccion, self.name,
                                         code_from, code_to,
                                         concurrency, delay_ms,
                                         batch_db, progress_every))


# ---------- Scraper implementation ----------

async def _scrape_full(conn, id_eleccion: int, source_name: str,
                       code_from: int, code_to: int,
                       concurrency: int, delay_ms: int,
                       batch_db: int, progress_every: int) -> int:
    """Descarga paralela con aiohttp.

    Playwright se usa SOLO para obtener cookies iniciales (1 request).
    Despues, aiohttp hace todas las llamadas con headers de Origin/Referer
    que el servidor exige para devolver JSON (sin ellos devuelve HTML shell).
    """
    import aiohttp
    from playwright.async_api import async_playwright
    from db import open_actas_snapshot, close_actas_snapshot, insert_acta_batch

    captured_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    snap_id = open_actas_snapshot(
        conn, captured_at, id_eleccion,
        modo="full", rango_desde=code_from, rango_hasta=code_to,
        source=source_name,
    )
    print(f"[scrape] snapshot_id={snap_id} range={code_from}..{code_to}")

    # 1) Bootstrap session via Playwright para obtener cookies
    print("[scrape] bootstrap Playwright para cookies...")
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True)
        ctx = await b.new_context()
        page = await ctx.new_page()
        await page.goto(INIT_URL, wait_until="networkidle", timeout=45000)
        cookies = await ctx.cookies()
        await b.close()

    jar = aiohttp.CookieJar()
    for c in cookies:
        jar.update_cookies({c["name"]: c["value"]},
                           response_url=aiohttp.helpers.URL("https://" + c["domain"].lstrip(".")))

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://resultadoelectoral.onpe.gob.pe",
        "Referer": "https://resultadoelectoral.onpe.gob.pe/main/presidenciales",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }

    stats = {"codigos_consultados": 0, "actas_ok": 0, "no_content": 0, "errores": 0}
    actas_buf, votos_buf = [], []
    lock = asyncio.Lock()
    t0 = time.time()

    async with aiohttp.ClientSession(cookie_jar=jar, headers=HEADERS,
                                      timeout=aiohttp.ClientTimeout(total=30)) as session:
        queue: asyncio.Queue[int] = asyncio.Queue()
        for c in range(code_from, code_to + 1):
            queue.put_nowait(c)

        async def worker():
            while True:
                try:
                    code = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                padded = f"{code:06d}"
                url = (f"{ONPE_BASE}/presentacion-backend/actas/buscar/mesa"
                       f"?codigoMesa={padded}")
                try:
                    async with session.get(url) as r:
                        status = r.status
                        text = await r.text()
                    async with lock:
                        stats["codigos_consultados"] += 1

                    if status == 200 and not text.startswith("<"):
                        try:
                            j = json.loads(text)
                        except json.JSONDecodeError:
                            async with lock:
                                stats["errores"] += 1
                            continue
                        if j.get("success"):
                            data = j.get("data") or []
                            if not data:
                                async with lock:
                                    stats["no_content"] += 1
                            else:
                                for acta in data:
                                    parsed = _parse_acta(acta, id_eleccion)
                                    if parsed:
                                        async with lock:
                                            actas_buf.append(parsed[0])
                                            votos_buf.extend(parsed[1])
                                            stats["actas_ok"] += 1
                    elif status == 204:
                        async with lock:
                            stats["no_content"] += 1
                    else:
                        async with lock:
                            stats["errores"] += 1

                    # Flush periodic
                    should_flush = False
                    async with lock:
                        if len(actas_buf) >= batch_db:
                            to_a = actas_buf[:]; to_v = votos_buf[:]
                            actas_buf.clear(); votos_buf.clear()
                            should_flush = True
                    if should_flush:
                        insert_acta_batch(conn, snap_id, to_a, to_v)

                    # Progress
                    if stats["codigos_consultados"] % progress_every == 0:
                        elapsed = time.time() - t0
                        rps = stats["codigos_consultados"] / max(elapsed, 0.01)
                        pending = queue.qsize()
                        eta_min = pending / rps / 60 if rps > 0 else -1
                        print(f"  [{stats['codigos_consultados']:>6}/{code_to-code_from+1}] "
                              f"ok={stats['actas_ok']:>5} empty={stats['no_content']:>5} "
                              f"err={stats['errores']:>3} {rps:.1f} req/s "
                              f"eta={eta_min:.1f}min")

                    if delay_ms > 0:
                        await asyncio.sleep(delay_ms / 1000.0)
                except Exception as e:
                    async with lock:
                        stats["errores"] += 1
                    if stats["errores"] < 5:
                        print(f"  [err code={code}] {e}")

        await asyncio.gather(*[worker() for _ in range(concurrency)])

        # Final flush
        if actas_buf:
            insert_acta_batch(conn, snap_id, actas_buf, votos_buf)

    stats["duracion_s"] = round(time.time() - t0, 2)
    close_actas_snapshot(conn, snap_id, stats)
    print(f"[scrape] done: actas_ok={stats['actas_ok']:,} empty={stats['no_content']:,} "
          f"err={stats['errores']:,} duracion={stats['duracion_s']}s "
          f"({stats['codigos_consultados']/max(stats['duracion_s'], 0.01):.1f} req/s)")
    return snap_id


def _parse_acta(acta: dict, id_eleccion_param: int) -> tuple[dict, list[tuple]] | None:
    """Convierte un dict de la API al shape de nuestras tablas (actas + acta_votos).

    Usa el `idEleccion` del JSON de la API, no el parametro, para que las multiples
    elecciones por codigoMesa (presidencial, senadores, diputados, parlamento andino)
    no colisionen por PK en la tabla actas.
    """
    try:
        codigo = int(acta.get("idMesa") or int(acta.get("codigoMesa") or "0"))
        if not codigo:
            return None
        id_eleccion = acta.get("idEleccion") or id_eleccion_param
        ubi = acta.get("idUbigeo") or 0
        # ubigeo en la API viene como int 5-6 digitos (ej 10101 = Amazonas/Chachapoyas/Chachapoyas)
        # Extraer depto: primeros 2 digitos * 10000
        ubi_str = str(ubi).zfill(6)
        id_depto = int(ubi_str[:2]) * 10000 if ubi_str[:2].isdigit() else None
        id_prov = int(ubi_str[:4] + "00") if ubi_str[:4].isdigit() else None
        id_dist = ubi if ubi else None

        estado_code = (acta.get("codigoEstadoActa") or "").strip().upper()
        estado = {"C": "contabilizada", "E": "jee", "P": "pendiente",
                  "J": "jee", "N": "pendiente"}.get(estado_code, "desconocido")

        acta_row = {
            "codigo": codigo,
            "id_eleccion": id_eleccion,
            "id_ubigeo_departamento": id_depto,
            "id_ubigeo_provincia": id_prov,
            "id_ubigeo_distrito": id_dist,
            "id_distrito_electoral": None,
            "estado": estado,
            "total_votos_validos": acta.get("totalVotosValidos") or 0,
            "total_votos_emitidos": acta.get("totalVotosEmitidos") or 0,
            "electores_habiles": acta.get("totalElectoresHabiles") or 0,
            "votos_blancos": 0,  # buscar en detalle
            "votos_nulos": 0,
            "raw_json": {
                "source": "mesa_search",
                "idMesa": acta.get("idMesa"),
                "codigoMesa": acta.get("codigoMesa"),
                "numeroCopia": acta.get("numeroCopia"),
                "nombreLocalVotacion": acta.get("nombreLocalVotacion"),
                "codigoLocalVotacion": acta.get("codigoLocalVotacion"),
                "descripcionEstadoActa": acta.get("descripcionEstadoActa"),
            },
        }

        votos = []
        for detalle in acta.get("detalle") or []:
            cod_agrup = detalle.get("adAgrupacionPolitica")
            v = detalle.get("adVotos") or 0
            descr = (detalle.get("adDescripcion") or "").upper()
            # Capturar blancos/nulos en columnas dedicadas
            if "BLANCO" in descr:
                acta_row["votos_blancos"] = v
            elif "NULO" in descr:
                acta_row["votos_nulos"] = v
            elif cod_agrup and v:
                votos.append((codigo, id_eleccion, cod_agrup, v))

        return acta_row, votos
    except Exception as e:
        print(f"  [parse err] {e} acta_id={acta.get('idMesa')}")
        return None


if __name__ == "__main__":
    s = MesaSearchStrategy()
    if "--probe" in sys.argv or len(sys.argv) == 1:
        r = s.probe()
        print(f"probe: ok={r.ok} msg={r.message}")
        if r.sample:
            print(json.dumps(r.sample, indent=2, ensure_ascii=False)[:2000])
    elif "--download" in sys.argv:
        from db import get_conn
        # Parse --from N --to M
        args = sys.argv
        code_from = int(args[args.index("--from") + 1]) if "--from" in args else 1
        code_to = int(args[args.index("--to") + 1]) if "--to" in args else 999999
        concurrency = int(args[args.index("--concurrency") + 1]) if "--concurrency" in args else 8
        conn = get_conn()
        sid = s.download(conn, code_from=code_from, code_to=code_to, concurrency=concurrency)
        conn.close()
        print(f"snapshot_id={sid}")
    else:
        print("Uso: python -m sources.mesa_search [--probe | --download [--from N --to M --concurrency K]]")


if __name__ == "__main__":
    s = MesaSearchStrategy()
    if "--probe" in sys.argv or len(sys.argv) == 1:
        r = s.probe()
        print(f"probe: ok={r.ok} msg={r.message}")
        if r.sample:
            print(json.dumps(r.sample, indent=2, ensure_ascii=False)[:2000])
    else:
        print("Uso: python -m sources.mesa_search --probe")
