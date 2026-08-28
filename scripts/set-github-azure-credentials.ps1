<#
.SYNOPSIS
Stores an existing Microsoft Entra application's credentials in GitHub.

.DESCRIPTION
Creates or updates the requested GitHub Actions environment and writes the
service-principal JSON as the encrypted AZURE_CREDENTIALS environment secret.
The client secret is entered securely and is never saved to a local file.

.PARAMETER ClientId
Application (client) ID of the existing Microsoft Entra app registration.

.PARAMETER SubscriptionId
Azure subscription containing the target Fabric capacity.

.PARAMETER TenantId
Microsoft Entra tenant containing the app registration.

.PARAMETER Repository
GitHub repository in owner/name format. Defaults to this demo repository.

.PARAMETER Environment
GitHub Actions environment that receives the secret. Defaults to fabric.
#>

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

# Convert the secret only while constructing the payload expected by azure/login.
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
    # Clear unmanaged and PowerShell references even when a GitHub command fails.
    if ($secretPointer -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($secretPointer)
    }
    Remove-Variable credentials -ErrorAction SilentlyContinue
}

Write-Host "Updated AZURE_CREDENTIALS in GitHub environment '$Environment'."
