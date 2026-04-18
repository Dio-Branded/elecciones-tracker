# Auditoría ONPE 2026 — Tracker Independiente

Herramienta ciudadana que captura el total nacional publicado por ONPE y lo cruza contra la suma mesa por mesa para detectar desfases.

## Qué hace

1. **Scraper horario** (`scraper.py`) captura el snapshot nacional de las elecciones Presidencial / Senadores / Parlamento Andino desde el endpoint público de ONPE.
2. **Ingesta del CSV de PRIME** (`ingest_prime_csv.py`) — mientras ONPE no habilite el módulo de descarga masiva, usamos el dataset publicado por [PRIME INSTITUTE](https://primeinstitute.com/onpe/) (86,111 actas).
3. **Análisis cruzado** (`analyze_actas.py`) — compara nacional vs mesa a mesa, por candidato y por departamento.
4. **Detector de anomalías** (`anomalies.py`) — reglas: suma interna inconsistente, emitidos > electores, desfase desproporcionado por candidato, cambios entre snapshots, actas desaparecidas.
5. **Dashboard estático** (`dashboard/index.html`) — vista pública generada desde `dashboard/data/latest.json`.

## Arquitectura

```
scraper.py            ──▶  snapshots + candidates (nacional)
ingest_prime_csv.py   ──▶  actas_snapshots + actas + acta_votos (mesa-nivel)
                            │
                            ▼
analyze_actas.py  ──▶  desfase por candidato y depto
anomalies.py      ──▶  tabla anomalies (4 reglas)
build_dashboard_data.py ──▶ dashboard/data/latest.json
dashboard/index.html ──▶  HTML estático (Tailwind CDN + vanilla JS)
```

Todo persiste en `data/onpe.db` (SQLite, WAL mode).

## Correr

```bash
# 1. Dependencias
pip install playwright
python -m playwright install chromium

# 2. Primer snapshot nacional
python scraper.py

# 3. Validar PRIME (rápido, <1 min)
python verify_prime_csv.py

# 4. Ingestar el CSV (86k actas, ~3 seg)
python ingest_prime_csv.py

# 5. Analizar cruce
python analyze_actas.py

# 6. Detectar anomalías
python anomalies.py --historical

# 7. Generar JSON para dashboard
python build_dashboard_data.py

# 8. Ver dashboard local
cd dashboard && python -m http.server 8765
# abrir http://localhost:8765
```

En Windows el `run_hourly.bat` encadena 1→7. Programar con Task Scheduler para ejecutar cada hora.

## Publicación (Cloudflare Pages)

El directorio `dashboard/` es desplegable como sitio estático:

```bash
# Opcional: desplegar a Cloudflare Pages
cd dashboard
# Via wrangler CLI
npx wrangler pages deploy . --project-name elecciones-audit
# O conectar el repo a Pages desde el dashboard de Cloudflare
```

El único archivo dinámico es `dashboard/data/latest.json`. El `build_dashboard_data.py` lo regenera cada hora; si empujas el directorio `dashboard/` a un repo conectado con Cloudflare Pages, los commits automáticos propagarán el JSON nuevo.

## Metodología y limitaciones

- **Ratio** = (% del desfase total que aporta un candidato) / (% del voto que tiene). 1.0 = proporcional. >2x = sospechoso.
- Un error técnico uniforme produce ratios cercanos a 1.0 para todos los candidatos. Un ratio 4x concentrado en un solo candidato no es explicable por error aleatorio.
- **Este tracker no concluye fraude**. Muestra números verificables. Interpretación corresponde a ONPE, JEE, JNE y observadores electorales.
- El CSV de PRIME es un snapshot fijo del 17-abr-2026. Nuestro snapshot nacional se refresca cada hora. Cuando ONPE publique el dataset oficial en [datosabiertos.gob.pe](https://datosabiertos.gob.pe), migraremos a esa fuente.
- El endpoint `/presentacion-backend/actas` está protegido (CORS + CSRF) contra scraping no autenticado — requiere credenciales de organización política o navegador con sesión Angular completa.

## Integridad

Cada snapshot del dashboard incluye un `integrity_sha256` calculado sobre los campos estructurales (candidatos + desglose por departamento). Permite verificar que el JSON no fue alterado post-generación.

## Licencia

MIT. Metodología y código abiertos. Datos de ONPE y PRIME INSTITUTE usados bajo sus respectivas condiciones.
