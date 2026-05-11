param(
    [string]$PolarisUrl = "http://localhost:8181"
)

$ErrorActionPreference = "Stop"

Write-Host "Requesting Polaris access token..."
$tokenResponse = Invoke-RestMethod `
    -Method Post `
    -Uri "$PolarisUrl/api/catalog/v1/oauth/tokens" `
    -ContentType "application/x-www-form-urlencoded" `
    -Body @{
        grant_type    = "client_credentials"
        client_id     = "root"
        client_secret = "secret"
        scope         = "PRINCIPAL_ROLE:ALL"
    }

$accessToken = $tokenResponse.access_token
$headers = @{
    Authorization = "Bearer $accessToken"
}

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
    }
    catch {
        $statusCode = $_.Exception.Response.StatusCode.value__
        if ($statusCode -eq 409) {
            Write-Host "Already exists: $Path"
            return
        }

        throw
    }
}

Write-Host "Creating Iceberg catalog polariscatalog..."
Invoke-PolarisJson -Method Post -Path "/api/management/v1/catalogs" -Body @{
    name = "polariscatalog"
    type = "INTERNAL"
    properties = @{
        "default-base-location" = "s3://warehouse"
        "s3.endpoint"          = "http://minio:9000"
        "s3.path-style-access" = "true"
        "s3.access-key-id"     = "admin"
        "s3.secret-access-key" = "password"
        "s3.region"            = "dummy-region"
    }
    storageConfigInfo = @{
        roleArn          = "arn:aws:iam::000000000000:role/minio-polaris-role"
        storageType      = "S3"
        allowedLocations = @("s3://warehouse/*")
    }
}

Write-Host "Creating catalog_admin catalog role..."
Invoke-PolarisJson -Method Post -Path "/api/management/v1/catalogs/polariscatalog/catalog-roles" -Body @{
    catalogRole = @{
        name = "catalog_admin"
    }
}

Write-Host "Granting catalog_admin role..."
Invoke-PolarisJson -Method Put -Path "/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants" -Body @{
    grant = @{
        type      = "catalog"
        privilege = "CATALOG_MANAGE_CONTENT"
    }
}

Write-Host "Creating data_engineer principal role..."
Invoke-PolarisJson -Method Post -Path "/api/management/v1/principal-roles" -Body @{
    principalRole = @{
        name = "data_engineer"
    }
}

Write-Host "Connecting data_engineer to catalog_admin..."
Invoke-PolarisJson -Method Put -Path "/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog" -Body @{
    catalogRole = @{
        name = "catalog_admin"
    }
}

Write-Host "Assigning data_engineer to root..."
Invoke-PolarisJson -Method Put -Path "/api/management/v1/principals/root/principal-roles" -Body @{
    principalRole = @{
        name = "data_engineer"
    }
}

Write-Host "Root principal roles:"
Invoke-RestMethod `
    -Method Get `
    -Uri "$PolarisUrl/api/management/v1/principals/root/principal-roles" `
    -Headers $headers | ConvertTo-Json -Depth 10
