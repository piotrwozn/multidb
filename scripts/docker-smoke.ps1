param(
    [string] $ImageTag = "multidb:phase49-smoke",
    [string] $ContainerName = "multidb-phase49-smoke",
    [string] $VolumeName = "multidb-phase49-smoke-data",
    [int] $AdminPort = 18080,
    [int] $PgPort = 15432,
    [string] $DockerCargoProfile = $env:MULTIDB_DOCKER_CARGO_PROFILE,
    [switch] $UsePrebuiltArtifacts,
    [string] $PrebuiltBin,
    [string] $PrebuiltStudioDir,
    [switch] $SkipBuild
)

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$AdminToken = "phase49-admin-token"
$AdminPassword = "phase50-admin-password"
$PgPassword = "phase49-pg-password"
$DockerDaemonReady = $false

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Label,
        [Parameter(Mandatory = $true)]
        [scriptblock] $Command
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

function Remove-SmokeContainer {
    try {
        docker rm -f $ContainerName 2>$null | Out-Null
    }
    catch {
    }
}

function Remove-SmokeVolume {
    $ExistingVolumes = docker volume ls --quiet --format "{{.Name}}" | Where-Object { $_ -eq $VolumeName }
    if ($LASTEXITCODE -ne 0) {
        throw "docker volume ls failed with exit code $LASTEXITCODE"
    }
    if ($ExistingVolumes) {
        Invoke-Checked -Label "docker volume rm" -Command {
            docker volume rm $VolumeName | Out-Null
        }
    }
}

function Start-SmokeContainer {
    Remove-SmokeContainer
    $ContainerId = docker run -d `
        --name $ContainerName `
        -p "127.0.0.1:${AdminPort}:8080" `
        -p "127.0.0.1:${PgPort}:5432" `
        -e "MULTIDB_RUNTIME_MODE=local-dev" `
        -e "MULTIDB_PG_TLS_MODE=disabled" `
        -e "MULTIDB_ADMIN_PASSWORD=$AdminPassword" `
        -e "MULTIDB_ADMIN_TOKEN=$AdminToken" `
        -e "MULTIDB_PG_USER=multidb" `
        -e "MULTIDB_PG_PASSWORD=$PgPassword" `
        -v "${VolumeName}:/var/lib/multidb" `
        $ImageTag
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($ContainerId)) {
        throw "docker run failed"
    }
}

function Wait-HttpOk {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [hashtable] $Headers = @{}
    )

    $Deadline = (Get-Date).AddSeconds(60)
    $Uri = "http://127.0.0.1:$AdminPort$Path"
    do {
        try {
            $Response = Invoke-WebRequest -Uri $Uri -Headers $Headers -TimeoutSec 2 -UseBasicParsing
            if ($Response.StatusCode -eq 200) {
                return $Response
            }
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    } while ((Get-Date) -lt $Deadline)

    docker logs $ContainerName
    throw "timed out waiting for $Uri"
}

function Invoke-ApiJson {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [string] $Method = "GET",
        [hashtable] $Headers = @{},
        [string] $Body = $null
    )

    $Uri = "http://127.0.0.1:$AdminPort$Path"
    $Params = @{
        Uri = $Uri
        Method = $Method
        Headers = $Headers
        TimeoutSec = 5
        UseBasicParsing = $true
    }
    if (-not [string]::IsNullOrEmpty($Body)) {
        $Params.Body = $Body
        $Params.ContentType = "application/json"
    }
    $Response = Invoke-WebRequest @Params
    $Payload = $Response.Content | ConvertFrom-Json
    if (-not $Payload.ok) {
        throw "$Path returned an error envelope"
    }
    return $Payload
}

function Assert-HttpUnauthorized {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [hashtable] $Headers = @{}
    )

    $Uri = "http://127.0.0.1:$AdminPort$Path"
    try {
        Invoke-WebRequest -Uri $Uri -Headers $Headers -TimeoutSec 5 -UseBasicParsing | Out-Null
    }
    catch {
        if ($_.Exception.Response -ne $null -and [int]$_.Exception.Response.StatusCode -eq 401) {
            return
        }
        throw
    }
    throw "$Path should have rejected the invalidated session"
}

function Test-TcpPort {
    param([Parameter(Mandatory = $true)][int] $Port)

    $Client = [System.Net.Sockets.TcpClient]::new()
    try {
        $Async = $Client.BeginConnect("127.0.0.1", $Port, $null, $null)
        if (-not $Async.AsyncWaitHandle.WaitOne([TimeSpan]::FromSeconds(2))) {
            return $false
        }
        $Client.EndConnect($Async)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $Client.Dispose()
    }
}

function New-PrebuiltDockerContext {
    function Resolve-ArtifactPath {
        param([Parameter(Mandatory = $true)][string] $Path)

        if ([System.IO.Path]::IsPathRooted($Path)) {
            return [System.IO.Path]::GetFullPath($Path)
        }
        return [System.IO.Path]::GetFullPath((Join-Path $Root $Path))
    }

    $TargetRoot = [System.IO.Path]::GetFullPath((Join-Path $Root "target"))
    if (-not $TargetRoot.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
        $TargetRoot = "$TargetRoot$([System.IO.Path]::DirectorySeparatorChar)"
    }

    $ContextRoot = [System.IO.Path]::GetFullPath((Join-Path $Root "target\docker-smoke-context"))
    if (-not $ContextRoot.StartsWith($TargetRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "refusing to clear docker smoke context outside target: $ContextRoot"
    }
    if (Test-Path $ContextRoot) {
        Remove-Item -LiteralPath $ContextRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $ContextRoot | Out-Null

    $ResolvedBin = $PrebuiltBin
    if ([string]::IsNullOrWhiteSpace($ResolvedBin)) {
        $ResolvedBin = Join-Path $Root "target\debug\multidb"
    }
    $ResolvedBin = Resolve-ArtifactPath -Path $ResolvedBin
    if (-not (Test-Path $ResolvedBin)) {
        throw "missing prebuilt multidb binary at $ResolvedBin"
    }
    Copy-Item -LiteralPath $ResolvedBin -Destination (Join-Path $ContextRoot "multidb") -Force

    $ResolvedStudioDir = $PrebuiltStudioDir
    if ([string]::IsNullOrWhiteSpace($ResolvedStudioDir)) {
        $ResolvedStudioDir = Join-Path $Root "studio\dist"
    }
    $ResolvedStudioDir = Resolve-ArtifactPath -Path $ResolvedStudioDir
    if (-not (Test-Path $ResolvedStudioDir)) {
        throw "missing prebuilt Studio dist at $ResolvedStudioDir"
    }
    $StudioContext = Join-Path $ContextRoot "studio"
    New-Item -ItemType Directory -Force -Path $StudioContext | Out-Null
    Copy-Item -Path (Join-Path $ResolvedStudioDir "*") -Destination $StudioContext -Recurse -Force

    return $ContextRoot
}

try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "docker is required for phase 49 smoke"
    }
    try {
        docker info *> $null
    }
    catch {
        throw "docker daemon is required for phase 49 smoke"
    }
    if ($LASTEXITCODE -ne 0) {
        throw "docker daemon is required for phase 49 smoke"
    }
    $DockerDaemonReady = $true

    Push-Location $Root
    try {
        if (-not $SkipBuild) {
            Invoke-Checked -Label "docker build" -Command {
                $DockerfilePath = Join-Path $Root "Dockerfile"
                $BuildContext = "."
                if ($UsePrebuiltArtifacts) {
                    $DockerfilePath = Join-Path $Root "Dockerfile.smoke"
                    $BuildContext = New-PrebuiltDockerContext
                }

                $BuildArgs = @("build", "-t", $ImageTag, "-f", $DockerfilePath)
                if (-not [string]::IsNullOrWhiteSpace($DockerCargoProfile)) {
                    $BuildArgs += @("--build-arg", "MULTIDB_CARGO_PROFILE=$DockerCargoProfile")
                }
                $BuildArgs += $BuildContext
                docker @BuildArgs
            }
        }

        Remove-SmokeVolume
        Invoke-Checked -Label "docker volume create" -Command {
            docker volume create $VolumeName | Out-Null
        }

        Start-SmokeContainer
        Wait-HttpOk -Path "/ready" | Out-Null
        Wait-HttpOk -Path "/health" | Out-Null
        $LoginBody = @{ username = "admin"; password = $AdminPassword } | ConvertTo-Json -Compress
        $Login = Invoke-ApiJson -Path "/api/auth/login" -Method "POST" -Body $LoginBody
        $SessionToken = $Login.data.token
        if ([string]::IsNullOrWhiteSpace($SessionToken)) {
            throw "/api/auth/login did not return a session token"
        }
        Invoke-ApiJson -Path "/api/status" -Headers @{ Authorization = "Bearer $SessionToken" } | Out-Null
        Invoke-ApiJson -Path "/api/auth/logout" -Method "POST" -Headers @{ Authorization = "Bearer $SessionToken" } | Out-Null
        Assert-HttpUnauthorized -Path "/api/status" -Headers @{ Authorization = "Bearer $SessionToken" }
        Invoke-ApiJson -Path "/api/status" -Headers @{ Authorization = "Bearer $AdminToken" } | Out-Null
        $Studio = Wait-HttpOk -Path "/"
        if (-not ($Studio.Content.Contains('<div id="root"') -or $Studio.Content.Contains("multidb"))) {
            throw "Studio index did not look like built HTML"
        }
        if (-not (Test-TcpPort -Port $PgPort)) {
            throw "PostgreSQL wire port $PgPort did not accept TCP"
        }
        Invoke-Checked -Label "database file exists" -Command {
            docker exec $ContainerName sh -c "test -s /var/lib/multidb/multidb.redb"
        }

        Remove-SmokeContainer
        Start-SmokeContainer
        Wait-HttpOk -Path "/ready" | Out-Null
        Invoke-Checked -Label "database file persisted after restart" -Command {
            docker exec $ContainerName sh -c "test -s /var/lib/multidb/multidb.redb"
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    if ($DockerDaemonReady) {
        Remove-SmokeContainer
        try {
            Remove-SmokeVolume
        }
        catch {
        }
    }
}

Write-Host "docker smoke ok: image=$ImageTag admin=http://127.0.0.1:$AdminPort pg=127.0.0.1:$PgPort"
