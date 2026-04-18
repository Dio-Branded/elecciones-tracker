@echo off
REM ONPE tracker — snapshot full diario (1x al día)
REM Scrapea todas las 86K+ mesas desde 0. ~15 min.
REM Programar con Task Scheduler para 00:00 hora Lima.
cd /d "%~dp0"
set PY="C:\Users\Marcelo\AppData\Local\Programs\Python\Python311\python.exe"
set LOG=logs\cron.log

echo ========== DAILY FULL %DATE% %TIME% ========== >> %LOG%

REM Full scrape (sin --incremental)
%PY% scraper_actas.py --strategy mesa_search >> %LOG% 2>&1

REM Re-analizar con snapshot completo
%PY% analyze_actas.py >> %LOG% 2>&1
%PY% anomalies.py --historical >> %LOG% 2>&1
%PY% cross_validate.py >> %LOG% 2>&1
%PY% build_dashboard_data.py >> %LOG% 2>&1

echo daily done >> %LOG%
