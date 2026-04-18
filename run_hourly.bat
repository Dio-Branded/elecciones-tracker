@echo off
REM ONPE tracker — captura snapshot horario + analisis de actas + anomalias
cd /d "%~dp0"
set PY="C:\Users\Marcelo\AppData\Local\Programs\Python\Python311\python.exe"
set LOG=logs\cron.log

echo ========== %DATE% %TIME% ========== >> %LOG%

REM 1. Snapshot nacional (presidencial + senadores + parlamento andino)
%PY% scraper.py >> %LOG% 2>&1

REM 2. Descargar mesas de la mejor fuente disponible (auto = priority ascendente)
REM    mesa_search (ONPE direct, 20 req/s) > prime_csv mirror > stub onpe_oficial
%PY% scraper_actas.py >> %LOG% 2>&1

REM 3. Analizar cruce mesa-a-mesa vs nacional
%PY% analyze_actas.py >> %LOG% 2>&1

REM 4. Detector de anomalias + comparacion historica
%PY% anomalies.py --historical >> %LOG% 2>&1

REM 5. Cross-validar fuentes si hay >=2
%PY% cross_validate.py >> %LOG% 2>&1

REM 6. Consolidar JSON para dashboard
%PY% build_dashboard_data.py >> %LOG% 2>&1

echo done >> %LOG%
