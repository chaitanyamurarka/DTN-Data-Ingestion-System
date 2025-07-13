@echo off

REM Activate the virtual environment
call "%~dp0.venv\Scripts\activate.bat"

REM Run data ingestion scripts
echo Starting data ingestion microservices...
for %%f in ( iqfeed_keep_alive.py ohlc_ingest.py live_tick_ingest.py ) do (
    start "" "%~dp0.venv\Scripts\python.exe" "%%f"
    timeout /t 5 /nobreak > nul
)

pause