"""Enrich mesas with geographic names (departamento/provincia/distrito).

The scraper stores ubigeo numeric codes but the detail endpoint /actas/{id}
returns ubigeoNivel01 (departamento), ubigeoNivel02 (provincia),
ubigeoNivel03 (distrito) as strings. We cache them in data/geo_cache.json
keyed by codigo (integer).

Cached data is used by build_visual_audit.py to show "LIMA / LIMA / SAN JUAN
DE MIRAFLORES" instead of just the local name.

Usage:
  python enrich_geo.py                      # enrich all mesas in latest anomalies report
  python enrich_geo.py --codigo 54938,1     # specific mesas
  python enrich_geo.py --all                # every mesa in snapshot
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn
from validate_actas import _cookies_and_headers, ONPE_BASE

OUT_DIR = Path(__file__).parent / "data"
CACHE = OUT_DIR / "geo_cache.json"


def load_cache() -> dict:
    if CACHE.exists():
        return json.loads(CACHE.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict):
    CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


async def enrich_one(session, codigo: int) -> dict | None:
    padded = f"{codigo:06d}"
    url = f"{ONPE_BASE}/presentacion-backend/actas/buscar/mesa?codigoMesa={padded}"
    async with session.get(url) as r:
        if r.status != 200:
            return None
        j = await r.json()
    actas = [a for a in (j.get("data") or []) if a.get("idEleccion") == 10]
    if not actas:
        return None
    acta_id = actas[0].get("id")
    async with session.get(f"{ONPE_BASE}/presentacion-backend/actas/{acta_id}") as r:
        if r.status != 200:
            return None
        jd = await r.json()
    a = jd.get("data") or {}
    return {
        "departamento": a.get("ubigeoNivel01"),
        "provincia": a.get("ubigeoNivel02"),
        "distrito": a.get("ubigeoNivel03"),
        "local": a.get("nombreLocalVotacion"),
        "codigo_local": a.get("codigoLocalVotacion"),
        "direccion": a.get("direccionLocal") or a.get("direccion"),
    }


async def run(codigos: list[int], concurrency: int = 20):
    import aiohttp
    cache = load_cache()
    to_fetch = [c for c in codigos if str(c) not in cache]
    print(f"[geo] cached={len(cache)} new={len(to_fetch)}")
    if not to_fetch:
        return cache

    jar, headers = await _cookies_and_headers()
    sem = asyncio.Semaphore(concurrency)

    async def worker(s, c):
        async with sem:
            try:
                return c, await enrich_one(s, c)
            except Exception as e:
                return c, None

    async with aiohttp.ClientSession(
            cookie_jar=jar, headers=headers,
            timeout=aiohttp.ClientTimeout(total=60)) as session:
        tasks = [worker(session, c) for c in to_fetch]
        done = 0
        for coro in asyncio.as_completed(tasks):
            c, data = await coro
            done += 1
            if data:
                cache[str(c)] = data
            if done % 100 == 0 or done == len(to_fetch):
                print(f"  [{done}/{len(to_fetch)}] cached")
                save_cache(cache)
    save_cache(cache)
    return cache


def load_anomaly_codigos() -> list[int]:
    files = sorted(OUT_DIR.glob("anomalies_report_*.json"))
    if not files:
        return []
    data = json.loads(files[-1].read_text(encoding="utf-8"))
    codes = set()
    for f in data["findings"]:
        if f.get("tipo") == "outlier_local" and f.get("codigo") is not None:
            codes.add(f["codigo"])
    return sorted(codes)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codigo", default=None)
    ap.add_argument("--all", action="store_true", help="enrich all mesas in snapshot")
    args = ap.parse_args()

    if args.codigo:
        codigos = [int(x) for x in args.codigo.split(",")]
    elif args.all:
        conn = get_conn()
        snap_id = conn.execute(
            "SELECT id FROM actas_snapshots WHERE modo='full' AND actas_ok>=60000 "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        codigos = [r[0] for r in conn.execute(
            "SELECT codigo FROM actas WHERE snapshot_id=? AND id_eleccion=10",
            (snap_id,),
        )]
        conn.close()
    else:
        codigos = load_anomaly_codigos()
        if not codigos:
            print("no anomalies_report encontrado"); sys.exit(1)

    print(f"[geo] {len(codigos)} mesas a enriquecer")
    cache = asyncio.run(run(codigos))
    print(f"[geo] cache final: {len(cache)} entradas en {CACHE}")


if __name__ == "__main__":
    main()
