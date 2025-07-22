@echo off

REM Activate the virtual environment
call "%~dp0.venv\Scripts\activate.bat"

REM Run data ingestion scripts
echo Starting data ingestion microservices...
for %%f in ( scripts.iqfeed_keep_alive scripts.ohlc_ingest scripts.live_tick_ingest ) do (
    echo Starting %%f ...
    start "%%f" "%~dp0.venv\Scripts\python.exe" -m "%%f"
    timeout /t 5 /nobreak > nul
)

echo.
echo All microservices have been launched in separate windows.
pause