param(
    [string]$Image = $(if ($env:OMNIKV_IMAGE) { $env:OMNIKV_IMAGE } else { "omnikv:smoke" }),
    [string]$ProjectName = $(if ($env:OMNIKV_SMOKE_PROJECT) { $env:OMNIKV_SMOKE_PROJECT } else { "omnikv-smoke" }),
    [string]$HttpPort = $(if ($env:OMNIKV_HTTP_PORT) { $env:OMNIKV_HTTP_PORT } else { "18443" }),
    [string]$BuildImage = $(if ($env:OMNIKV_SMOKE_BUILD) { $env:OMNIKV_SMOKE_BUILD } else { "true" })
)

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$ComposeFile = if ($env:OMNIKV_SMOKE_COMPOSE_FILE) { $env:OMNIKV_SMOKE_COMPOSE_FILE } else { Join-Path $RootDir "docker-compose.smoke.yml" }

$env:OMNIKV_IMAGE = $Image
$env:OMNIKV_HTTP_PORT = $HttpPort
if (-not $env:OMNIKV_JWT_SECRET) {
    $env:OMNIKV_JWT_SECRET = "omnikv-smoke-jwt-secret-0123456789abcdef"
}
if (-not $env:OMNIKV_BOOTSTRAP_ADMIN_KEY) {
    $env:OMNIKV_BOOTSTRAP_ADMIN_KEY = "omnikv-smoke-admin-key-0123456789abcdef"
}

function Invoke-DockerCompose {
    & docker compose -f $ComposeFile -p $ProjectName @args
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $args"
    }
}

function Invoke-Curl {
    $output = & curl.exe -kfsS @args
    if ($LASTEXITCODE -ne 0) {
        throw "curl failed: $args"
    }
    return ($output -join "`n")
}

function Wait-OmniHealth {
    param([string]$BaseUrl)

    for ($i = 0; $i -lt 60; $i++) {
        try {
            Invoke-Curl "$BaseUrl/health" | Out-Null
            return
        } catch {
            Start-Sleep -Seconds 2
        }
    }

    Invoke-DockerCompose logs --tail=200
    throw "OmniKV did not become healthy at $BaseUrl/health"
}

function Assert-Value {
    param(
        [string]$JsonBody,
        [string]$Expected
    )

    $body = $JsonBody | ConvertFrom-Json
    if (-not $body.success) {
        throw "response success=false: $JsonBody"
    }
    if ($body.data.value -ne $Expected) {
        throw "expected value '$Expected', got '$($body.data.value)': $JsonBody"
    }
}

try {
    if ($BuildImage -ne "false") {
        & docker build --pull --tag $Image $RootDir
        if ($LASTEXITCODE -ne 0) {
            throw "docker build failed"
        }
    }

    Invoke-DockerCompose down -v --remove-orphans | Out-Null
    Invoke-DockerCompose up -d | Out-Null

    $baseUrl = "https://127.0.0.1:$HttpPort"
    Wait-OmniHealth $baseUrl

    $tokenJson = Invoke-Curl `
        -X POST "$baseUrl/auth/token" `
        -H "content-type: application/json" `
        -H "x-omni-admin-key: $env:OMNIKV_BOOTSTRAP_ADMIN_KEY" `
        --data '{"username":"compose-smoke","role":"write"}'
    $token = ($tokenJson | ConvertFrom-Json).data

    $key = "smoke:$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
    $value = "compose-smoke-value"

    Invoke-Curl `
        -X POST "$baseUrl/kv" `
        -H "authorization: Bearer $token" `
        -H "content-type: application/json" `
        --data "{`"key`":`"$key`",`"value`":`"$value`"}" | Out-Null

    $readJson = Invoke-Curl `
        -H "authorization: Bearer $token" `
        "$baseUrl/kv/$key"
    Assert-Value $readJson $value

    Invoke-DockerCompose restart omnikv | Out-Null
    Wait-OmniHealth $baseUrl

    $readAfterRestartJson = Invoke-Curl `
        -H "authorization: Bearer $token" `
        "$baseUrl/kv/$key"
    Assert-Value $readAfterRestartJson $value

    Write-Host "OmniKV Docker Compose smoke passed: authenticated write/read survived restart."
} finally {
    Invoke-DockerCompose down -v --remove-orphans | Out-Null
}
