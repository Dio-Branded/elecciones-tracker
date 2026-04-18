"""Strategy: monitor de datosabiertos.gob.pe para el dataset oficial ONPE EG2026.

ONPE historicamente publica datasets post-electorales en el portal nacional
de datos abiertos. Mientras no aparezca, este strategy devuelve probe=False.
Cuando aparezca, lo detecta, descarga el CSV/JSON y lo ingesta.

URL base del portal: https://datosabiertos.gob.pe/
Query: buscar 'elecciones generales 2026' o 'resultados por mesa 2026'

TODO: cuando el dataset exista, ajustar DATASET_SEARCH_URL y el parser del
schema (probablemente distinto al de PRIME).
"""
from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request

from ._common import ActasStrategy, ProbeResult, ID_ELECCION_PRESIDENCIAL

# CKAN-style package search API del portal
PORTAL_API = "https://datosabiertos.gob.pe/api/3/action/package_search"
QUERY = "elecciones generales 2026 por mesa"


class DatosAbiertosStrategy(ActasStrategy):
    """Monitor del dataset oficial ONPE en datosabiertos.gob.pe."""
    name = "onpe_oficial"
    priority = 10  # highest priority: cuando exista, preferir sobre mesa_search

    def __init__(self):
        self._dataset_url: str | None = None

    def probe(self) -> ProbeResult:
        try:
            url = f"{PORTAL_API}?q={urllib.parse.quote(QUERY)}&rows=5"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                body = json.loads(r.read().decode("utf-8"))
        except Exception as e:
            return ProbeResult(False, f"portal fetch failed: {e}")

        if not body.get("success"):
            return ProbeResult(False, "portal devolvio success=false")
        results = body.get("result", {}).get("results", [])
        if not results:
            return ProbeResult(False, "no hay dataset que matchee la query")

        # Heuristica: buscar un dataset cuyo title mencione 2026 + mesa
        for ds in results:
            title = (ds.get("title") or "").lower()
            if "2026" in title and ("mesa" in title or "acta" in title):
                resources = ds.get("resources") or []
                csv_or_json = next((r for r in resources
                                     if (r.get("format") or "").upper() in ("CSV", "JSON")), None)
                if csv_or_json:
                    self._dataset_url = csv_or_json.get("url")
                    return ProbeResult(
                        True, f"dataset encontrado: {ds.get('title')}",
                        {"id": ds.get("id"), "url": self._dataset_url,
                         "format": csv_or_json.get("format")}
                    )

        return ProbeResult(False, f"{len(results)} datasets encontrados pero ninguno matchea")

    def download(self, conn, id_eleccion: int = ID_ELECCION_PRESIDENCIAL):
        if self._dataset_url is None:
            probe = self.probe()
            if not probe.ok:
                print(f"[{self.name}] no disponible: {probe.message}")
                return None

        # TODO: cuando el dataset exista, implementar parser segun su schema.
        # Por ahora alertar y no hacer nada.
        print(f"[{self.name}] dataset detectado: {self._dataset_url}")
        print(f"[{self.name}] parser NO implementado — schema desconocido hasta que se publique.")
        return None


if __name__ == "__main__":
    s = DatosAbiertosStrategy()
    r = s.probe()
    print(f"probe: ok={r.ok} msg={r.message}")
    if r.sample:
        print(json.dumps(r.sample, indent=2, ensure_ascii=False))
