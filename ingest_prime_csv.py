"""
Ingesta del CSV de PRIME INSTITUTE (https://primeinstitute.com/onpe/data.csv)
en el schema `actas` + `acta_votos` del tracker.

El CSV es un snapshot fijado por PRIME el 17-abr-2026. Lo registramos como
snapshot de actas con modo='prime_csv'. Esto nos da datos mesa-nivel para
poder correr analyze_actas.py y anomalies.py sin depender del endpoint
directo de ONPE (que sigue en investigacion).

Mapeo de columnas CSV -> schema ONPE:
  mesa         -> actas.codigo (int, ya viene como 000001, parseado a 1)
  departamento -> se mapea a id_ubigeo_departamento via DEPTO_UBIGEO
  emitidos     -> actas.total_votos_emitidos
  validos      -> actas.total_votos_validos
  electores    -> actas.electores_habiles
  blancos      -> actas.votos_blancos
  nulos        -> actas.votos_nulos
  estado (C/E) -> actas.estado ('contabilizada' / 'jee')
  FU/SP/LA/NI/BE/AL/CH -> acta_votos.votos (uno por candidato, con codigo_agrupacion)
"""
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from db import get_conn, open_actas_snapshot, close_actas_snapshot, insert_acta_batch

CSV_PATH = Path(__file__).parent / "data" / "prime_data.csv"
ID_ELECCION_PRESIDENCIAL = 10

# Codigo de agrupacion politica en ONPE (validado contra snapshot presidencial id=4)
CSV_COL_TO_AGRUPACION = {
    "FU": 8,   # Fuerza Popular (Fujimori)
    "SP": 10,  # Juntos por el Peru (Sanchez Palomino)
    "LA": 35,  # Renovacion Popular (Lopez Aliaga)
    "NI": 16,  # Partido del Buen Gobierno (Nieto)
    "BE": 14,  # Partido Civico Obras (Belmont)
    "AL": 23,  # Partido Pais para Todos (Alvarez)
    "CH": 2,   # Ahora Nacion - AN (Lopez Chau)
}

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
    # Extranjero / casos especiales
    "EXTRANJERO": 990000,
    "PERUANOS EN EL EXTRANJERO": 990000,
}

ESTADO_MAP = {
    "C": "contabilizada",
    "E": "jee",
    "P": "pendiente",
    "J": "jee",
}

BATCH_SIZE = 2000


def normalize_depto(d: str) -> str:
    """Quita tildes y upper."""
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", d.strip().upper())
                   if unicodedata.category(c) != "Mn")


def main():
    force = "--force" in sys.argv
    if not CSV_PATH.exists():
        print(f"ERROR: {CSV_PATH} no existe. Corre `python verify_prime_csv.py` primero.")
        sys.exit(1)

    conn = get_conn()

    # Idempotencia: si ya hay un snapshot prime_csv, preguntar antes de duplicar
    existing = conn.execute(
        "SELECT id, captured_at, actas_ok FROM actas_snapshots "
        "WHERE modo='prime_csv' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if existing and not force:
        sid, captured_at, ok = existing
        print(f"[skip] Ya existe snapshot prime_csv id={sid} captured_at={captured_at} actas_ok={ok}")
        print("      Usa --force para re-ingestar")
        return

    captured_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    snap_id = open_actas_snapshot(conn, captured_at, ID_ELECCION_PRESIDENCIAL,
                                   modo="prime_csv", rango_desde=1, rango_hasta=999999)
    print(f"[open] actas_snapshot id={snap_id} eid={ID_ELECCION_PRESIDENCIAL} modo=prime_csv")

    t0 = time.time()
    actas_batch: list[dict] = []
    votos_batch: list[tuple] = []
    stats = {"codigos_consultados": 0, "actas_ok": 0, "no_content": 0, "errores": 0}
    deptos_no_match = set()

    with CSV_PATH.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats["codigos_consultados"] += 1
            try:
                codigo = int(row["mesa"])
                depto_raw = row.get("departamento", "").strip()
                depto_key = normalize_depto(depto_raw)
                ubigeo = DEPTO_UBIGEO.get(depto_key)
                if ubigeo is None and depto_key:
                    deptos_no_match.add(depto_raw)

                def ai(k):
                    v = (row.get(k) or "").strip()
                    try:
                        return int(v) if v else 0
                    except ValueError:
                        return 0

                estado = ESTADO_MAP.get((row.get("estado") or "").strip().upper(), "desconocido")

                actas_batch.append({
                    "codigo": codigo,
                    "id_eleccion": ID_ELECCION_PRESIDENCIAL,
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
                    "raw_json": {
                        "source": "prime_csv_v1",
                        "provincia": row.get("provincia"),
                        "distrito": row.get("distrito"),
                        "local": row.get("local"),
                    },
                })
                for csv_col, cod_agrup in CSV_COL_TO_AGRUPACION.items():
                    v = ai(csv_col)
                    if v:
                        votos_batch.append((codigo, ID_ELECCION_PRESIDENCIAL, cod_agrup, v))

                stats["actas_ok"] += 1

                if len(actas_batch) >= BATCH_SIZE:
                    insert_acta_batch(conn, snap_id, actas_batch, votos_batch)
                    print(f"  [flush] {stats['actas_ok']:,} actas  ({time.time()-t0:.1f}s)")
                    actas_batch.clear()
                    votos_batch.clear()
            except Exception as e:
                stats["errores"] += 1
                print(f"  [err] row mesa={row.get('mesa')}: {e}")

    if actas_batch:
        insert_acta_batch(conn, snap_id, actas_batch, votos_batch)
        print(f"  [flush] final: {stats['actas_ok']:,} actas")

    stats["duracion_s"] = round(time.time() - t0, 2)
    close_actas_snapshot(conn, snap_id, stats)

    print(f"\n[done] snapshot_id={snap_id}")
    print(f"  actas_ok:     {stats['actas_ok']:,}")
    print(f"  errores:      {stats['errores']:,}")
    print(f"  duracion:     {stats['duracion_s']}s")
    if deptos_no_match:
        print(f"  WARN: departamentos sin ubigeo match: {sorted(deptos_no_match)}")

    # Verificacion rapida
    n_actas = conn.execute("SELECT COUNT(*) FROM actas WHERE snapshot_id=?", (snap_id,)).fetchone()[0]
    n_votos = conn.execute("SELECT COUNT(*) FROM acta_votos WHERE snapshot_id=?", (snap_id,)).fetchone()[0]
    print(f"  DB: {n_actas:,} rows en actas, {n_votos:,} rows en acta_votos")

    conn.close()


if __name__ == "__main__":
    main()
