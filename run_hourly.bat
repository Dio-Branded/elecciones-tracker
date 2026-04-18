@echo off
REM ONPE tracker — captura snapshot horario + analisis de actas
cd /d "%~dp0"
set PY="C:\Users\Marcelo\AppData\Local\Programs\Python\Python311\python.exe"
set LOG=logs\cron.log

echo ========== %DATE% %TIME% ========== >> %LOG%

REM 1. Snapshot nacional (presidencial + senadores + parlamento andino)
%PY% scraper.py >> %LOG% 2>&1

REM 2. Refrescar CSV de PRIME (solo si es mas viejo que 1h, maneja cache internamente)
%PY% verify_prime_csv.py >> %LOG% 2>&1

REM 3. Re-analizar actas vs nacional (usa el snapshot actual de cada lado)
%PY% analyze_actas.py >> %LOG% 2>&1

REM 4. Detector de anomalias (incluye comparacion historica con snapshot previo si existe)
%PY% anomalies.py --historical >> %LOG% 2>&1

REM 5. Consolidar JSON para dashboard
%PY% build_dashboard_data.py >> %LOG% 2>&1

echo done >> %LOG%
