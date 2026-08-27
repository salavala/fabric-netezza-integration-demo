[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-fA-F-]{36}$')]
    [string] $ClientId,

    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-fA-F-]{36}$')]
    [string] $SubscriptionId,

    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-fA-F-]{36}$')]
    [string] $TenantId,

    [string] $Repository = 'salavala/fabric-netezza-integration-demo',

    [string] $Environment = 'fabric'
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw 'GitHub CLI (gh) is required: https://cli.github.com/'
}

gh auth status | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'Authenticate GitHub CLI first with: gh auth login'
}

$clientSecret = Read-Host 'Microsoft Entra client secret' -AsSecureString
$secretPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($clientSecret)

try {
    $credentials = @{
        clientId       = $ClientId
        clientSecret   = [Runtime.InteropServices.Marshal]::PtrToStringBSTR(
            $secretPointer
        )
        subscriptionId = $SubscriptionId
        tenantId       = $TenantId
    } | ConvertTo-Json -Compress

    gh api `
        --method PUT `
        "repos/$Repository/environments/$Environment" `
        --silent
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to create or update GitHub environment '$Environment'."
    }

    $credentials | gh secret set AZURE_CREDENTIALS `
        --repo $Repository `
        --env $Environment
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to update the AZURE_CREDENTIALS environment secret.'
    }
}
finally {
    if ($secretPointer -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($secretPointer)
    }
    Remove-Variable credentials -ErrorAction SilentlyContinue
}

Write-Host "Updated AZURE_CREDENTIALS in GitHub environment '$Environment'."
