"""Constantes y utilidades compartidas por los strategies de fuentes de actas."""
from __future__ import annotations

import unicodedata
from abc import ABC, abstractmethod
from dataclasses import dataclass

ID_ELECCION_PRESIDENCIAL = 10

# CSV columna (PRIME) -> codigo_agrupacion en ONPE (validado contra snapshot presidencial id=4)
CSV_COL_TO_AGRUPACION = {
    "FU": 8,    # Fuerza Popular (Fujimori)
    "SP": 10,   # Juntos por el Peru (Sanchez Palomino)
    "LA": 35,   # Renovacion Popular (Lopez Aliaga)
    "NI": 16,   # Partido del Buen Gobierno (Nieto)
    "BE": 14,   # Partido Civico Obras (Belmont)
    "AL": 23,   # Partido Pais para Todos (Alvarez)
    "CH": 2,    # Ahora Nacion - AN (Lopez Chau)
}

# Ubigeo department code (6 digits, only departamento prefix relevant)
DEPTO_UBIGEO = {
    "AMAZONAS":    10000, "ANCASH":     20000, "APURIMAC":   30000,
    "AREQUIPA":    40000, "AYACUCHO":   50000, "CAJAMARCA":  60000,
    "CALLAO":      70000, "CUSCO":      80000, "HUANCAVELICA": 90000,
    "HUANUCO":    100000, "ICA":       110000, "JUNIN":      120000,
    "LA LIBERTAD":130000, "LAMBAYEQUE":140000, "LIMA":       150000,
    "LORETO":     160000, "MADRE DE DIOS":170000, "MOQUEGUA": 180000,
    "PASCO":      190000, "PIURA":     200000, "PUNO":       210000,
    "SAN MARTIN": 220000, "TACNA":     230000, "TUMBES":     240000,
    "UCAYALI":    250000,
    # Peruanos en el Extranjero
    "EXTRANJERO": 990000,
    "PERUANOS EN EL EXTRANJERO": 990000,
    "AMERICA": 990000, "ASIA": 990000, "EUROPA": 990000,
    "OCEANIA": 990000, "AFRICA": 990000,
}

ESTADO_MAP = {
    "C": "contabilizada",
    "E": "jee",
    "P": "pendiente",
    "J": "jee",
}


def normalize_depto(d: str) -> str:
    """Quita tildes + uppercase para matching robusto."""
    if not d:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", d.strip().upper())
        if unicodedata.category(c) != "Mn"
    )


def depto_to_ubigeo(depto_name: str) -> int | None:
    return DEPTO_UBIGEO.get(normalize_depto(depto_name))


# ---------- Strategy protocol ----------

@dataclass
class ProbeResult:
    ok: bool
    message: str
    sample: dict | None = None


class ActasStrategy(ABC):
    """Interfaz para una fuente de datos de actas mesa-por-mesa.

    Cada strategy expone:
      - name: identificador corto, usado como 'source' en DB
      - priority: menor = mas preferido (0=mejor). El runner elige el mas bajo `available()`
      - probe(): test rapido sin side effects, retorna ProbeResult
      - available(): True si la fuente esta lista para descargar ahora
      - download(conn, id_eleccion): crea un actas_snapshot y lo puebla; retorna snapshot_id
    """
    name: str = "unknown"
    priority: int = 100  # 0 = preferido, 100 = fallback

    @abstractmethod
    def probe(self) -> ProbeResult: ...

    def available(self) -> bool:
        return self.probe().ok

    @abstractmethod
    def download(self, conn, id_eleccion: int = ID_ELECCION_PRESIDENCIAL) -> int | None:
        """Descarga y persiste. Retorna snapshot_id o None si fallo."""
