param(
    [string]$PolarisUrl = "http://localhost:8181"
)

$ErrorActionPreference = "Stop"

# Конфігурація
$ClientId = "root"
$ClientSecret = "secret"

$CatalogName = "polariscatalog"
$CatalogRole = "catalog_admin"
$PrincipalRole = "data_engineer"

function Log-Step {
    param([string]$Message)

    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Log-Success {
    param([string]$Message)

    Write-Host "[OK] $Message" -ForegroundColor Green
}

Log-Step "Requesting Polaris access token"

$tokenResponse = Invoke-RestMethod `
    -Method Post `
    -Uri "$PolarisUrl/api/catalog/v1/oauth/tokens" `
    -ContentType "application/x-www-form-urlencoded" `
    -Body @{
        grant_type    = "client_credentials"
        client_id     = $ClientId
        client_secret = $ClientSecret
        scope         = "PRINCIPAL_ROLE:ALL"
    }

if (-not $tokenResponse.access_token) {
    throw "Failed to obtain access token"
}

$accessToken = $tokenResponse.access_token

$headers = @{
    Authorization = "Bearer $accessToken"
}

Log-Success "Access token received"

function Invoke-PolarisJson {
    param(
        [Parameter(Mandatory = $true)][string]$Method,
        [Parameter(Mandatory = $true)][string]$Path,
        [object]$Body
    )

    $jsonBody = $null

    if ($null -ne $Body) {
        $jsonBody = $Body | ConvertTo-Json -Depth 10
    }

    try {
        Invoke-RestMethod `
            -Method $Method `
            -Uri "$PolarisUrl$Path" `
            -Headers $headers `
            -ContentType "application/json" `
            -Body $jsonBody | Out-Null

        Write-Host "[OK] $Path" -ForegroundColor Green
    }
    catch {
        $statusCode = $_.Exception.Response.StatusCode.value__

        if ($statusCode -eq 409) {
            Write-Host "[SKIP] Already exists: $Path" -ForegroundColor Yellow
            return
        }

        Write-Host "[ERROR] Request failed: $Path" -ForegroundColor Red
        throw
    }
}

Log-Step "Creating Iceberg catalog"

Invoke-PolarisJson `
    -Method Post `
    -Path "/api/management/v1/catalogs" `
    -Body @{
        name = $CatalogName
        type = "INTERNAL"

        properties = @{
            "default-base-location" = "s3://warehouse"
            "s3.endpoint"           = "http://minio:9000"
            "s3.path-style-access"  = "true"
            "s3.access-key-id"      = "admin"
            "s3.secret-access-key"  = "password"
            "s3.region"             = "dummy-region"
        }

        storageConfigInfo = @{
            roleArn          = "arn:aws:iam::000000000000:role/minio-polaris-role"
            storageType      = "S3"
            allowedLocations = @("s3://warehouse/*")
        }
    }

Log-Step "Creating catalog role"

Invoke-PolarisJson `
    -Method Post `
    -Path "/api/management/v1/catalogs/$CatalogName/catalog-roles" `
    -Body @{
        catalogRole = @{
            name = $CatalogRole
        }
    }

Log-Step "Granting catalog permissions"

Invoke-PolarisJson `
    -Method Put `
    -Path "/api/management/v1/catalogs/$CatalogName/catalog-roles/$CatalogRole/grants" `
    -Body @{
        grant = @{
            type      = "catalog"
            privilege = "CATALOG_MANAGE_CONTENT"
        }
    }

Log-Step "Creating principal role"

Invoke-PolarisJson `
    -Method Post `
    -Path "/api/management/v1/principal-roles" `
    -Body @{
        principalRole = @{
            name = $PrincipalRole
        }
    }

Log-Step "Connecting principal role to catalog role"

Invoke-PolarisJson `
    -Method Put `
    -Path "/api/management/v1/principal-roles/$PrincipalRole/catalog-roles/$CatalogName" `
    -Body @{
        catalogRole = @{
            name = $CatalogRole
        }
    }

Log-Step "Assigning principal role to root"

Invoke-PolarisJson `
    -Method Put `
    -Path "/api/management/v1/principals/root/principal-roles" `
    -Body @{
        principalRole = @{
            name = $PrincipalRole
        }
    }

Log-Step "Fetching root principal roles"

Invoke-RestMethod `
    -Method Get `
    -Uri "$PolarisUrl/api/management/v1/principals/root/principal-roles" `
    -Headers $headers | ConvertTo-Json -Depth 10

Log-Success "Polaris setup completed"