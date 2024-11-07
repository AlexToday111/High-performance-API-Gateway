param([string]$BaseUrl = '', [string]$AdminUrl = '')
$ErrorActionPreference = 'Stop'
$repoDir = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$ports = @{GATEWAY_PORT='8080'; ADMIN_PORT='9091'; BACKEND_A_PORT='8081'; BACKEND_B_PORT='8082'; BACKEND_C_PORT='8083'}
$envFile = Join-Path $repoDir '.env'
if (Test-Path -LiteralPath $envFile) {
    foreach ($line in [IO.File]::ReadAllLines($envFile)) {
        if ($line -match '^([A-Z_]+)=(\d+)$') { $ports[$Matches[1]] = $Matches[2] }
    }
}
if (!$BaseUrl) { $BaseUrl = "http://localhost:$($ports.GATEWAY_PORT)" }
if (!$AdminUrl) { $AdminUrl = "http://localhost:$($ports.ADMIN_PORT)" }
$backends = @($ports.BACKEND_A_PORT, $ports.BACKEND_B_PORT, $ports.BACKEND_C_PORT) | ForEach-Object { "http://localhost:$_" }
$configFile = Join-Path $repoDir 'configs\docker.yaml'
$original = [IO.File]::ReadAllText($configFile)
function Publish-Config([string]$content) {
    $temporary = "$configFile.demo.tmp"
    [IO.File]::WriteAllText($temporary, $content, [Text.UTF8Encoding]::new($false))
    # Windows PowerShell coerces $null string arguments to empty strings. Supply
    # a real backup path, then remove only that explicitly named temporary file.
    $backup = "$configFile.demo.bak"
    [IO.File]::Replace($temporary, $configFile, $backup)
    Remove-Item -LiteralPath $backup
    Start-Sleep -Seconds 2
}
function Request-Status([string]$url) {
    $value = & curl.exe -s -o NUL -w '%{http_code}' $url
    if ($LASTEXITCODE -ne 0) { throw "curl failed: $url" }
    return [int]$value
}
try {
    Write-Output 'Round robin: three instance IDs'
    1..3 | ForEach-Object { (Invoke-RestMethod "$BaseUrl/api/users/fast").backend }
    Write-Output 'Timeout:'
    Request-Status "$BaseUrl/api/users/slow"
    Write-Output 'Active health: backend-a fails probes, then recovers'
    Invoke-WebRequest -UseBasicParsing -Method POST "$($backends[0])/control?unhealthy=true" | Out-Null
    Start-Sleep -Seconds 7
    Invoke-RestMethod "$AdminUrl/admin/upstreams" | ConvertTo-Json -Depth 5
    Invoke-WebRequest -UseBasicParsing -Method POST "$($backends[0])/control?unhealthy=false" | Out-Null
    Start-Sleep -Seconds 5
    Write-Output 'Invalid reload retains valid routes:'
    Publish-Config ($original + "`nunknownField: true`n")
    Request-Status "$BaseUrl/api/users/fast"
    Write-Output 'Breaker: open before passive health threshold, keep health probes green'
    Publish-Config ($original.Replace('failureThreshold: 5', 'failureThreshold: 2').Replace('openDuration: 10s', 'openDuration: 6s'))
    foreach ($backend in $backends) { Invoke-WebRequest -UseBasicParsing -Method POST "$backend/control?fail=true" | Out-Null }
    1..7 | ForEach-Object { Request-Status "$BaseUrl/api/users/fast" }
    Invoke-RestMethod "$AdminUrl/admin/upstreams" | ConvertTo-Json -Depth 5
    Start-Sleep -Seconds 5 # Keep OPEN visible for the Prometheus scrape.
    foreach ($backend in $backends) { Invoke-WebRequest -UseBasicParsing -Method POST "$backend/control?fail=false" | Out-Null }
    Start-Sleep -Seconds 7
    1..6 | ForEach-Object { Request-Status "$BaseUrl/api/users/fast" }
    Write-Output 'Breaker recovered:'
    Invoke-RestMethod "$AdminUrl/admin/upstreams" | ConvertTo-Json -Depth 5
} finally {
    foreach ($backend in $backends) { try { Invoke-WebRequest -UseBasicParsing -Method POST "$backend/control?unhealthy=false&fail=false&delay=0ms" | Out-Null } catch {} }
    # Restore the configured 10ms/50ms/10ms workload as well as YAML.
    foreach ($pair in @(@($backends[0], '10ms'), @($backends[1], '50ms'), @($backends[2], '10ms'))) {
        try { Invoke-WebRequest -UseBasicParsing -Method POST "$($pair[0])/control?delay=$($pair[1])" | Out-Null } catch {}
    }
    Publish-Config $original
}
