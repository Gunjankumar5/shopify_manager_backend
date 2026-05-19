# PowerShell script to load .env variables into the current session
# Usage: .\load-env.ps1

$envFile = Join-Path $PSScriptRoot ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^(.*?)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
            Set-Item -Path "env:$key" -Value $value
        }
    }
    Write-Host ".env variables loaded into current session."
} else {
    Write-Host ".env file not found."
}
