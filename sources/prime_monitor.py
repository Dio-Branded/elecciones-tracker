"""Strategy: CSV de PRIME INSTITUTE (https://primeinstitute.com/onpe/data.csv).

Re-ingesta si el CSV cambio (ETag / Last-Modified / SHA-256). Permite serie
temporal desde PRIME mientras esta sea la unica fuente disponible.
"""
from __future__ import annotations

import csv
import hashlib
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ._common import (
    ActasStrategy, ProbeResult,
    CSV_COL_TO_AGRUPACION, ESTADO_MAP, ID_ELECCION_PRESIDENCIAL,
    depto_to_ubigeo,
)

PRIME_CSV_URL = "https://primeinstitute.com/onpe/data.csv"
CACHE_DIR = Path(__file__).resolve().parents[1] / "data"
CSV_CACHE = CACHE_DIR / "prime_data.csv"
BATCH_SIZE = 2000


class PrimeCsvMirrorStrategy(ActasStrategy):
    """Descarga el CSV publico de PRIME. Idempotente por SHA-256 del contenido."""
    name = "prime_csv"
    priority = 50  # fallback; prefieren strategies que obtienen data directa

    def __init__(self):
        self.url = PRIME_CSV_URL

    def probe(self) -> ProbeResult:
        try:
            req = urllib.request.Request(self.url, method="HEAD",
                                          headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                if r.status == 200:
                    return ProbeResult(True, f"HEAD {r.status}",
                                       {"content_length": r.headers.get("content-length"),
                                        "etag": r.headers.get("etag"),
                                        "last_modified": r.headers.get("last-modified")})
                return ProbeResult(False, f"HEAD {r.status}")
        except Exception as e:
            return ProbeResult(False, f"HEAD failed: {e}")

    def _download_csv(self) -> tuple[Path, str, str | None]:
        CACHE_DIR.mkdir(exist_ok=True)
        req = urllib.request.Request(self.url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read()
            etag = r.headers.get("etag") or r.headers.get("last-modified")
        CSV_CACHE.write_bytes(body)
        sha = hashlib.sha256(body).hexdigest()
        return CSV_CACHE, sha, etag

    def _already_ingested(self, conn, sha: str) -> int | None:
        row = conn.execute(
            "SELECT id FROM actas_snapshots WHERE source=? AND source_sha256=? LIMIT 1",
            (self.name, sha),
        ).fetchone()
        return row[0] if row else None

    def download(self, conn, id_eleccion: int = ID_ELECCION_PRESIDENCIAL) -> int | None:
        # Importado aqui para evitar circular si este archivo se usa como module dentro de db
        from db import open_actas_snapshot, close_actas_snapshot, insert_acta_batch

        print(f"[{self.name}] descargando {self.url}")
        csv_path, sha, etag = self._download_csv()
        print(f"[{self.name}] size={csv_path.stat().st_size // 1024} KB sha={sha[:12]}...")

        existing_sid = self._already_ingested(conn, sha)
        if existing_sid:
            print(f"[{self.name}] ya ingestado como snapshot_id={existing_sid} — skip")
            return existing_sid

        captured_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        snap_id = open_actas_snapshot(
            conn, captured_at, id_eleccion,
            modo="prime_csv", rango_desde=1, rango_hasta=999999,
            source=self.name, source_etag=etag, source_sha256=sha,
        )
        print(f"[{self.name}] abierto snapshot_id={snap_id}")

        t0 = time.time()
        actas_batch, votos_batch = [], []
        stats = {"codigos_consultados": 0, "actas_ok": 0, "no_content": 0, "errores": 0}

        with csv_path.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats["codigos_consultados"] += 1
                try:
                    codigo = int(row["mesa"])
                    ubigeo = depto_to_ubigeo(row.get("departamento", ""))

                    def ai(k):
                        v = (row.get(k) or "").strip()
                        try:
                            return int(v) if v else 0
                        except ValueError:
                            return 0

                    estado = ESTADO_MAP.get((row.get("estado") or "").strip().upper(), "desconocido")

                    actas_batch.append({
                        "codigo": codigo,
                        "id_eleccion": id_eleccion,
                        "id_ubigeo_departamento": ubigeo,
                        "id_ubigeo_provincia": None,
                        "id_ubigeo_distrito": None,
                        "id_distrito_electoral": None,
                        "estado": estado,
                        "total_votos_validos": ai("validos"),
                        "total_votos_emitidos": ai("emitidos"),
                        "electores_habiles": ai("electores"),
                        "votos_blancos": ai("blancos"),
                        "votos_nulos": ai("nulos"),
                        "raw_json": {"source": "prime_csv", "etag": etag,
                                     "provincia": row.get("provincia"),
                                     "distrito": row.get("distrito"),
                                     "local": row.get("local")},
                    })
                    for csv_col, cod_agrup in CSV_COL_TO_AGRUPACION.items():
                        v = ai(csv_col)
                        if v:
                            votos_batch.append((codigo, id_eleccion, cod_agrup, v))
                    stats["actas_ok"] += 1

                    if len(actas_batch) >= BATCH_SIZE:
                        insert_acta_batch(conn, snap_id, actas_batch, votos_batch)
                        actas_batch.clear()
                        votos_batch.clear()
                except Exception as e:
                    stats["errores"] += 1
                    print(f"  [err] mesa={row.get('mesa')}: {e}")

        if actas_batch:
            insert_acta_batch(conn, snap_id, actas_batch, votos_batch)

        stats["duracion_s"] = round(time.time() - t0, 2)
        close_actas_snapshot(conn, snap_id, stats)
        print(f"[{self.name}] done: actas_ok={stats['actas_ok']:,} "
              f"errores={stats['errores']} duracion={stats['duracion_s']}s")
        return snap_id


# CLI probe
if __name__ == "__main__":
    s = PrimeCsvMirrorStrategy()
    if "--probe" in sys.argv:
        r = s.probe()
        print(f"probe: ok={r.ok} msg={r.message} sample={r.sample}")
    elif "--download" in sys.argv:
        from db import get_conn
        conn = get_conn()
        sid = s.download(conn)
        conn.close()
        print(f"snapshot_id={sid}")
    else:
        print("Uso: python -m sources.prime_monitor [--probe | --download]")
