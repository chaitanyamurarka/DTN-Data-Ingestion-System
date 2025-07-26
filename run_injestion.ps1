# DTN Data Ingestion Microservices Launcher
# PowerShell version with full color support

# Set console encoding to UTF-8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"
$env:FORCE_COLOR = "1"

# Enable ANSI color support (for PowerShell 7+)
if ($PSVersionTable.PSVersion.Major -ge 7) {
    $PSStyle.OutputRendering = 'ANSI'
}

# Define colors
$colors = @{
    Header = "`e[36m"    # Cyan
    Success = "`e[32m"   # Green
    Warning = "`e[33m"   # Yellow
    Error = "`e[31m"     # Red
    Info = "`e[34m"      # Blue
    Service = "`e[35m"   # Magenta
    Reset = "`e[0m"
}

# Clear screen and show header
Clear-Host
Write-Host "$($colors.Header)===============================================================$($colors.Reset)"
Write-Host "$($colors.Success)           DTN Data Ingestion Microservices Launcher$($colors.Reset)"
Write-Host "$($colors.Header)===============================================================$($colors.Reset)"
Write-Host ""

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPath = Join-Path $scriptDir ".venv"
$pythonPath = Join-Path $venvPath "Scripts\python.exe"

# Check if virtual environment exists
if (-not (Test-Path $pythonPath)) {
    Write-Host "$($colors.Error)[!] Virtual environment not found at: $venvPath$($colors.Reset)"
    Write-Host "$($colors.Error)[!] Please create a virtual environment first.$($colors.Reset)"
    Read-Host "Press Enter to exit"
    exit 1
}

# Define services
$services = @(
    @{
        Module = "scripts.iqfeed_keep_alive"
        Name = "IQFeed Keep Alive Service"
        Color = "Cyan"
    },
    @{
        Module = "scripts.ohlc_ingest"
        Name = "OHLC Data Ingestion Service"
        Color = "Green"
    },
    @{
        Module = "scripts.live_tick_ingest"
        Name = "Live Tick Ingestion Service"
        Color = "Yellow"
    }
)

Write-Host "$($colors.Warning)[*] Starting data ingestion microservices...$($colors.Reset)"
Write-Host ""

# Start each service
$count = 1
foreach ($service in $services) {
    Write-Host "$($colors.Info)[$count/$($services.Count)]$($colors.Reset) Starting $($colors.Service)$($service.Name)$($colors.Reset)"
    Write-Host "    Module: $($colors.Header)$($service.Module)$($colors.Reset)"
    
    try {
        # Create PowerShell profile for color support in new window
        $profileContent = @"
`$host.UI.RawUI.BackgroundColor = 'Black'
`$host.UI.RawUI.ForegroundColor = 'White'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
`$env:PYTHONIOENCODING = 'utf-8'
`$env:PYTHONUNBUFFERED = '1'
`$env:FORCE_COLOR = '1'
if (`$PSVersionTable.PSVersion.Major -ge 7) {
    `$PSStyle.OutputRendering = 'ANSI'
}
Clear-Host
Write-Host '===============================================================' -ForegroundColor $($service.Color)
Write-Host " $($service.Name) " -ForegroundColor $($service.Color)
Write-Host '===============================================================' -ForegroundColor $($service.Color)
Write-Host ''
"@
        
        # Start the service in a new PowerShell window
        $arguments = @(
            "-NoExit",
            "-Command",
            "& { $profileContent; & '$pythonPath' -m $($service.Module) }"
        )
        
        Start-Process powershell.exe -ArgumentList $arguments -WindowStyle Normal
        
        Write-Host "    $($colors.Success)[OK] Launched successfully$($colors.Reset)"
    }
    catch {
        Write-Host "    $($colors.Error)[FAIL] Failed to launch: $_$($colors.Reset)"
    }
    
    Write-Host ""
    $count++
    Start-Sleep -Seconds 3
}

Write-Host "$($colors.Header)===============================================================$($colors.Reset)"
Write-Host "$($colors.Success)[OK] All microservices have been launched in separate windows.$($colors.Reset)"
Write-Host "$($colors.Header)===============================================================$($colors.Reset)"
Write-Host ""
Write-Host "$($colors.Warning)Press any key to exit this launcher...$($colors.Reset)"
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")