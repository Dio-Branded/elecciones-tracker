# Auditoría ONPE 2026 — Tracker Independiente

Herramienta ciudadana que captura el total nacional publicado por ONPE y lo cruza contra la suma mesa por mesa para detectar desfases.

## Qué hace

1. **Scraper horario nacional** (`scraper.py`) captura el agregado presidencial / senadores / parlamento andino desde el endpoint público de ONPE.
2. **Scraper mesa-a-mesa** (`scraper_actas.py`) — arquitectura de múltiples fuentes con selección automática:
   - `mesa_search`: descarga directa del endpoint ONPE iterando `GET /actas/buscar/mesa?codigoMesa=NNNNNN` (~20 req/s con aiohttp + headers CORS), 86K actas en ~15 min
   - `prime_csv`: mirror del CSV público de [PRIME INSTITUTE](https://primeinstitute.com/onpe/) como fallback
   - `onpe_oficial`: monitor de `datosabiertos.gob.pe` para el dataset oficial post-electoral (cuando se publique)
3. **Cross-validator** (`cross_validate.py`) — cuando existen ≥2 fuentes, diff mesa-a-mesa y flag de tampering.
4. **Análisis cruzado** (`analyze_actas.py`) — suma mesa-a-mesa vs nacional, por candidato y por departamento.
5. **Detector de anomalías** (`anomalies.py`) — reglas: suma interna inconsistente, emitidos > electores, desfase desproporcionado por candidato, cambios entre snapshots, actas desaparecidas, source_mismatch.
6. **Dashboard estático** (`dashboard/index.html`) — vista pública con badges de fuente y hash de integridad.

## Arquitectura

```
scraper.py                ──▶  snapshots + candidates (nacional)
scraper_actas.py          ──▶  selecciona strategy auto
  │
  ├─ sources/mesa_search   ─▶  ONPE directo, aiohttp + cookies
  ├─ sources/prime_monitor ─▶  mirror PRIME CSV
  └─ sources/datosabiertos ─▶  dataset oficial (placeholder)
                │
                ▼
         actas_snapshots + actas + acta_votos
                │
                ▼
  analyze_actas.py  ──▶  desfase por candidato y depto
  anomalies.py      ──▶  anomalies (5 reglas)
  cross_validate.py ──▶  source_mismatch entre fuentes
  build_dashboard_data.py ──▶ dashboard/data/latest.json
  dashboard/index.html ──▶  HTML estático (Tailwind CDN)
```

Todo persiste en `data/onpe.db` (SQLite, WAL mode).

## Fuentes de datos

| Strategy | Priority | Cómo obtiene | Estado | Cobertura |
|---|---:|---|---|---|
| `onpe_oficial` | 10 | CKAN API de `datosabiertos.gob.pe` | Esperando publicación dataset 2026 | ? |
| `mesa_search` | 20 | `GET /presentacion-backend/actas/buscar/mesa?codigoMesa=NNNNNN` | **Activa** | 86K+ actas |
| `prime_csv` | 50 | Mirror del CSV público de PRIME | **Activa (fallback)** | 86,111 actas (snapshot 17-abr) |

El runner (`scraper_actas.py`) elige la strategy de menor priority que pase `probe()`. Cada ejecución crea un nuevo `actas_snapshot` etiquetado con la fuente.

## Correr

```bash
# 1. Dependencias
pip install playwright aiohttp
python -m playwright install chromium

# 2. Primer snapshot nacional
python scraper.py

# 3. Descargar mesas (auto-selecciona la mejor fuente disponible)
python scraper_actas.py
# O forzar una fuente específica:
python scraper_actas.py --strategy mesa_search --from 1 --to 999999 --concurrency 20
python scraper_actas.py --strategy prime_csv
python scraper_actas.py --probe-all    # reportar estado de cada strategy

# 4. Analizar cruce
python analyze_actas.py

# 5. Detectar anomalías
python anomalies.py --historical

# 6. Cross-validar fuentes (si hay ≥2)
python cross_validate.py

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
cd dashboard
npx wrangler pages deploy . --project-name elecciones-audit
```

O conectar el repo a Cloudflare Pages desde su dashboard; cada commit al `main` propaga el `latest.json` nuevo.

## Metodología y limitaciones

- **Ratio** = (% del desfase total que aporta un candidato) / (% del voto que tiene). 1.0 = proporcional. >2x = sospechoso.
- Un error técnico uniforme produce ratios cercanos a 1.0 para todos los candidatos. Un ratio 4x concentrado en un solo candidato no es explicable por error aleatorio.
- **Este tracker no concluye fraude**. Muestra números verificables. Interpretación corresponde a ONPE, JEE, JNE y observadores electorales.
- Cuando ≥2 fuentes existen, `cross_validate.py` señala discrepancias mesa-a-mesa entre ellas — fuerte indicador de tampering en al menos una de las fuentes.

## Endpoint ONPE — breakthrough técnico

El endpoint `GET /presentacion-backend/actas/buscar/mesa?codigoMesa=NNNNNN` (código padded a 6 dígitos como **string**, no integer) retorna JSON con todas las elecciones por mesa (presidencial, senadores, diputados, parlamento andino). Requiere headers `Origin`, `Referer` y `sec-fetch-*` para evitar que el gateway devuelva HTML del SPA. Las cookies se obtienen vía Playwright bootstrap.

Esto permite descargar nuestra propia copia de las 86K actas sin depender del CSV de PRIME.

## Integridad

Cada snapshot del dashboard incluye un `integrity_sha256` calculado sobre los campos estructurales (candidatos + desglose por departamento). Permite verificar que el JSON no fue alterado post-generación.

## Licencia

MIT. Metodología y código abiertos. Datos de ONPE usados bajo sus condiciones públicas.
