@echo off
REM ONPE tracker — captura snapshot horario + analisis de actas + anomalias
cd /d "%~dp0"
set PY="C:\Users\Marcelo\AppData\Local\Programs\Python\Python311\python.exe"
set LOG=logs\cron.log

echo ========== %DATE% %TIME% ========== >> %LOG%

REM 1. Snapshot nacional (presidencial + senadores + parlamento andino)
%PY% scraper.py >> %LOG% 2>&1

REM 2. Descargar mesas — modo incremental (pendientes + sample contabilizadas, ~1 min)
REM    Para el primer snapshot full del día, usar `run_daily.bat` (sin --incremental)
%PY% scraper_actas.py --strategy mesa_search --incremental >> %LOG% 2>&1

REM 3. Analizar cruce mesa-a-mesa vs nacional
%PY% analyze_actas.py >> %LOG% 2>&1

REM 4. Detector de anomalias + comparacion historica
%PY% anomalies.py --historical >> %LOG% 2>&1

REM 5. Cross-validar fuentes si hay >=2
%PY% cross_validate.py >> %LOG% 2>&1

REM 6. Consolidar JSON para dashboard
%PY% build_dashboard_data.py >> %LOG% 2>&1

echo done >> %LOG%
