"""SQLite schema + helpers for ONPE snapshots."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "onpe.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    id_eleccion INTEGER NOT NULL,
    tipo TEXT NOT NULL,
    actas_contabilizadas_pct REAL,
    contabilizadas INTEGER,
    total_actas INTEGER,
    participacion_ciudadana_pct REAL,
    actas_enviadas_jee_pct REAL,
    actas_pendientes_jee_pct REAL,
    fecha_actualizacion_ms INTEGER,
    total_votos_emitidos INTEGER,
    totales_raw TEXT
);

CREATE TABLE IF NOT EXISTS candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL,
    nombre_agrupacion TEXT,
    codigo_agrupacion INTEGER,
    nombre_candidato TEXT,
    dni_candidato TEXT,
    total_votos_validos INTEGER,
    pct_votos_validos REAL,
    pct_votos_emitidos REAL,
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots(captured_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_eleccion ON snapshots(id_eleccion, captured_at);
CREATE INDEX IF NOT EXISTS idx_candidates_snapshot ON candidates(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_candidates_agrupacion ON candidates(codigo_agrupacion);

CREATE TABLE IF NOT EXISTS actas_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    id_eleccion INTEGER NOT NULL,
    rango_desde INTEGER,
    rango_hasta INTEGER,
    codigos_consultados INTEGER DEFAULT 0,
    actas_ok INTEGER DEFAULT 0,
    no_content INTEGER DEFAULT 0,
    errores INTEGER DEFAULT 0,
    duracion_s REAL,
    modo TEXT,            -- 'full' | 'incremental' | 'sample' | 'prime_csv'
    source TEXT,          -- 'prime_csv' | 'prime_csv_v2' | 'onpe_oficial' | 'selenium_direct' | 'mesa_search'
    source_etag TEXT,     -- For external source monitors (ETag / Last-Modified)
    source_sha256 TEXT    -- Content hash for tamper detection
);

CREATE TABLE IF NOT EXISTS actas (
    snapshot_id INTEGER NOT NULL,
    codigo INTEGER NOT NULL,
    id_eleccion INTEGER NOT NULL,
    id_ubigeo_departamento INTEGER,
    id_ubigeo_provincia INTEGER,
    id_ubigeo_distrito INTEGER,
    id_distrito_electoral INTEGER,
    estado TEXT,                       -- contabilizada | pendiente | jee | missing
    total_votos_validos INTEGER,
    total_votos_emitidos INTEGER,
    electores_habiles INTEGER,
    votos_blancos INTEGER,
    votos_nulos INTEGER,
    raw_json TEXT,
    PRIMARY KEY (snapshot_id, codigo, id_eleccion)
);

CREATE TABLE IF NOT EXISTS acta_votos (
    snapshot_id INTEGER NOT NULL,
    codigo INTEGER NOT NULL,
    id_eleccion INTEGER NOT NULL,
    codigo_agrupacion INTEGER NOT NULL,
    votos INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, codigo, id_eleccion, codigo_agrupacion)
);

CREATE TABLE IF NOT EXISTS anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    detected_at TEXT NOT NULL,
    snapshot_id INTEGER,
    tipo TEXT NOT NULL,                -- sum_mismatch | vote_change | missing | disproportionate_delta | electores_exceeded
    codigo INTEGER,
    id_eleccion INTEGER,
    codigo_agrupacion INTEGER,
    detalle_json TEXT,
    severity INTEGER DEFAULT 2         -- 1=info, 2=warn, 3=critical
);

CREATE INDEX IF NOT EXISTS idx_actas_snap_eid ON actas(snapshot_id, id_eleccion);
CREATE INDEX IF NOT EXISTS idx_actas_codigo ON actas(codigo);
CREATE INDEX IF NOT EXISTS idx_actas_depto ON actas(id_ubigeo_departamento);
CREATE INDEX IF NOT EXISTS idx_acta_votos_snap ON acta_votos(snapshot_id, id_eleccion);
CREATE INDEX IF NOT EXISTS idx_acta_votos_agrupacion ON acta_votos(codigo_agrupacion);
CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies(detected_at);
CREATE INDEX IF NOT EXISTS idx_anomalies_tipo ON anomalies(tipo, severity);
"""

def _migrate_add_columns(conn):
    """Anade columnas a tablas existentes sin perder data."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(actas_snapshots)")}
    if "source" not in cols:
        conn.execute("ALTER TABLE actas_snapshots ADD COLUMN source TEXT")
    if "source_etag" not in cols:
        conn.execute("ALTER TABLE actas_snapshots ADD COLUMN source_etag TEXT")
    if "source_sha256" not in cols:
        conn.execute("ALTER TABLE actas_snapshots ADD COLUMN source_sha256 TEXT")
    conn.commit()


def get_conn():
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.executescript(SCHEMA)
    _migrate_add_columns(conn)
    return conn


def open_actas_snapshot(conn, captured_at: str, id_eleccion: int, modo: str,
                        rango_desde: int | None = None, rango_hasta: int | None = None,
                        source: str | None = None,
                        source_etag: str | None = None,
                        source_sha256: str | None = None) -> int:
    cur = conn.execute(
        "INSERT INTO actas_snapshots (captured_at, id_eleccion, modo, rango_desde, rango_hasta, "
        " source, source_etag, source_sha256) VALUES (?,?,?,?,?,?,?,?)",
        (captured_at, id_eleccion, modo, rango_desde, rango_hasta, source, source_etag, source_sha256),
    )
    conn.commit()
    return cur.lastrowid


def close_actas_snapshot(conn, snap_id: int, stats: dict):
    conn.execute(
        "UPDATE actas_snapshots SET codigos_consultados=?, actas_ok=?, no_content=?, "
        "errores=?, duracion_s=? WHERE id=?",
        (stats.get("codigos_consultados", 0), stats.get("actas_ok", 0),
         stats.get("no_content", 0), stats.get("errores", 0),
         stats.get("duracion_s"), snap_id),
    )
    conn.commit()


def insert_acta_batch(conn, snap_id: int, actas: list[dict], votos: list[tuple]):
    """
    actas: list of dict con keys {codigo, id_eleccion, id_ubigeo_departamento, ...
                                  estado, total_votos_validos, ...}
    votos: list of tuple (codigo, id_eleccion, codigo_agrupacion, votos)
    """
    import json as _json
    conn.executemany(
        "INSERT OR REPLACE INTO actas "
        "(snapshot_id, codigo, id_eleccion, id_ubigeo_departamento, id_ubigeo_provincia, "
        " id_ubigeo_distrito, id_distrito_electoral, estado, total_votos_validos, "
        " total_votos_emitidos, electores_habiles, votos_blancos, votos_nulos, raw_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (snap_id, a["codigo"], a["id_eleccion"],
             a.get("id_ubigeo_departamento"), a.get("id_ubigeo_provincia"),
             a.get("id_ubigeo_distrito"), a.get("id_distrito_electoral"),
             a.get("estado"), a.get("total_votos_validos"),
             a.get("total_votos_emitidos"), a.get("electores_habiles"),
             a.get("votos_blancos"), a.get("votos_nulos"),
             _json.dumps(a.get("raw_json") or {}, ensure_ascii=False))
            for a in actas
        ],
    )
    conn.executemany(
        "INSERT OR REPLACE INTO acta_votos "
        "(snapshot_id, codigo, id_eleccion, codigo_agrupacion, votos) VALUES (?,?,?,?,?)",
        [(snap_id, codigo, eid, cag, v) for (codigo, eid, cag, v) in votos],
    )
    conn.commit()


def insert_anomaly(conn, tipo: str, *, snapshot_id: int | None = None,
                   codigo: int | None = None, id_eleccion: int | None = None,
                   codigo_agrupacion: int | None = None,
                   detalle: dict | None = None, severity: int = 2):
    import json as _json
    from datetime import datetime, timezone
    conn.execute(
        "INSERT INTO anomalies (detected_at, snapshot_id, tipo, codigo, id_eleccion, "
        " codigo_agrupacion, detalle_json, severity) VALUES (?,?,?,?,?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(timespec="seconds"),
         snapshot_id, tipo, codigo, id_eleccion, codigo_agrupacion,
         _json.dumps(detalle or {}, ensure_ascii=False), severity),
    )

def insert_snapshot(conn, captured_at, id_eleccion, tipo, totales, participantes):
    t = totales
    cur = conn.execute(
        """INSERT INTO snapshots (captured_at, id_eleccion, tipo,
            actas_contabilizadas_pct, contabilizadas, total_actas,
            participacion_ciudadana_pct, actas_enviadas_jee_pct, actas_pendientes_jee_pct,
            fecha_actualizacion_ms, total_votos_emitidos, totales_raw)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            captured_at, id_eleccion, tipo,
            t.get("actasContabilizadas"), t.get("contabilizadas"), t.get("totalActas"),
            t.get("participacionCiudadana"), t.get("actasEnviadasJee"), t.get("actasPendientesJee"),
            t.get("fechaActualizacion"), t.get("totalVotosEmitidos"),
            __import__("json").dumps(t, ensure_ascii=False),
        ),
    )
    sid = cur.lastrowid
    rows = [
        (
            sid,
            p.get("nombreAgrupacionPolitica"),
            p.get("codigoAgrupacionPolitica"),
            p.get("nombreCandidato"),
            p.get("dniCandidato"),
            p.get("totalVotosValidos"),
            p.get("porcentajeVotosValidos"),
            p.get("porcentajeVotosEmitidos"),
        )
        for p in participantes
    ]
    conn.executemany(
        """INSERT INTO candidates (snapshot_id, nombre_agrupacion, codigo_agrupacion,
            nombre_candidato, dni_candidato, total_votos_validos,
            pct_votos_validos, pct_votos_emitidos)
           VALUES (?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    return sid
