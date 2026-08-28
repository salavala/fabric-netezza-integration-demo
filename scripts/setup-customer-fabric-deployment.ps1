<#
.SYNOPSIS
Bootstraps a customer's identity and GitHub secret for Fabric deployment.

.DESCRIPTION
Signs into Azure interactively, discovers the active tenant and subscription,
creates a Microsoft Entra app and service principal, grants subscription Reader,
and stores its generated credential in a GitHub Actions environment. The
credential is sent directly to GitHub and is not written to disk.

.PARAMETER Repository
Customer fork in GitHub owner/name format.

.PARAMETER ApplicationName
Display name for the new Microsoft Entra app registration.

.PARAMETER SubscriptionId
Optional subscription to select after login. The active subscription is used
when this parameter is omitted.

.PARAMETER Environment
GitHub Actions environment that receives the secret. Defaults to fabric.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^[^/]+/[^/]+$')]
    [string] $Repository,

    [string] $ApplicationName = 'Fabric GitHub Deployment',

    [ValidatePattern('^[0-9a-fA-F-]{36}$')]
    [string] $SubscriptionId,

    [string] $Environment = 'fabric'
)

$ErrorActionPreference = 'Stop'

foreach ($command in 'az', 'gh') {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "Required command '$command' was not found on PATH."
    }
}

az login --output none
if ($LASTEXITCODE -ne 0) {
    throw 'Azure login failed.'
}

if ($SubscriptionId) {
    az account set --subscription $SubscriptionId
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to select Azure subscription '$SubscriptionId'."
    }
}

$account = az account show --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $account.id -or -not $account.tenantId) {
    throw 'Unable to discover the active Azure subscription and tenant.'
}

$SubscriptionId = $account.id
$tenantId = $account.tenantId

# GitHub authentication is separate from the interactive Azure authentication.
gh auth status | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'GitHub login is required. Run: gh auth login'
}

gh repo view $Repository --json nameWithOwner | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Unable to access GitHub repository '$Repository'."
}

$app = az ad app create `
    --display-name $ApplicationName `
    --sign-in-audience AzureADMyOrg `
    --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $app.appId) {
    throw 'Unable to create the Microsoft Entra application.'
}

$servicePrincipal = az ad sp create `
    --id $app.appId `
    --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $servicePrincipal.id) {
    throw 'Unable to create the Microsoft Entra service principal.'
}

$credential = az ad app credential reset `
    --id $app.appId `
    --append `
    --display-name 'GitHub Actions' `
    --years 1 `
    --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $credential.password) {
    throw 'Unable to create the Microsoft Entra client secret.'
}

try {
    # Azure RBAC makes the subscription visible to azure/login. Fabric capacity
    # access remains a separate assignment performed by a Fabric administrator.
    az role assignment create `
        --assignee-object-id $servicePrincipal.id `
        --assignee-principal-type ServicePrincipal `
        --role Reader `
        --scope "/subscriptions/$SubscriptionId" `
        --output none
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to grant the service principal subscription Reader access.'
    }

    $credentials = @{
        clientId       = $app.appId
        clientSecret   = $credential.password
        subscriptionId = $SubscriptionId
        tenantId       = $tenantId
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
    # Avoid retaining the generated password in PowerShell variables.
    Remove-Variable credentials -ErrorAction SilentlyContinue
    Remove-Variable credential -ErrorAction SilentlyContinue
}

Write-Host 'Customer deployment identity configured successfully.'
Write-Host "Repository:      $Repository"
Write-Host "Subscription ID: $SubscriptionId"
Write-Host "Tenant ID:       $tenantId"
Write-Host "Client ID:       $($app.appId)"
Write-Host ''
Write-Host (
    'A Fabric administrator must now allow service principals to use Fabric APIs ' +
    'and grant this Client ID access to the target Fabric capacity.'
)
