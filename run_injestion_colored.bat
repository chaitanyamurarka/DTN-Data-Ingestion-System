@echo off
REM Run the PowerShell launcher with proper execution policy
powershell.exe -ExecutionPolicy Bypass -File "%~dp0run_injestion.ps1"