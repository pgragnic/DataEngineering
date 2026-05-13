@description('Région Azure')
param location string = resourceGroup().location

@description('Nom de base des ressources')
param appName string = 'etoro-portfolio'

@description('Clé agent eToro')
@secure()
param etoroAgentKey string

@description('Clé API eToro')
@secure()
param etoroApiKey string

@description('Montant investi en dollars')
param etoroInvestment string = '14892'

@description('Origine CORS autorisée')
param allowedOrigin string = '*'

// ── Noms des ressources ──────────────────────────────────────────────────────
var funcStorageName = toLower(take(replace('${appName}func', '-', ''), 24))
var webStorageName  = toLower(take(replace('${appName}web',  '-', ''), 24))
var planName        = '${appName}-plan'
var funcAppName     = '${appName}-api'
var cdnProfileName  = '${appName}-cdn'
var cdnEndpointName = '${appName}-web'

// ── Storage pour le runtime Functions ───────────────────────────────────────
resource funcStorage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: funcStorageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: { supportsHttpsTrafficOnly: true }
}

// ── Storage pour le site statique React ─────────────────────────────────────
resource webStorage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: webStorageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: true
  }
}

// ── Plan Consommation (serverless, scale à 0) ────────────────────────────────
resource plan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: planName
  location: location
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  properties: { reserved: true }
}

// ── Azure Function App ───────────────────────────────────────────────────────
resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: funcAppName
  location: location
  kind: 'functionapp,linux'
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'Python|3.12'
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${funcStorage.name};AccountKey=${funcStorage.listKeys().keys[0].value}'
        }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME',    value: 'python' }
        { name: 'ETORO_AGENT_KEY',             value: etoroAgentKey }
        { name: 'ETORO_API_KEY',               value: etoroApiKey }
        { name: 'ETORO_INVESTMENT',            value: etoroInvestment }
        { name: 'ALLOWED_ORIGIN',              value: allowedOrigin }
      ]
      cors: {
        allowedOrigins: [ allowedOrigin, 'https://portal.azure.com' ]
      }
    }
  }
}

// ── CDN pour le site statique ────────────────────────────────────────────────
resource cdnProfile 'Microsoft.Cdn/profiles@2023-05-01' = {
  name: cdnProfileName
  location: 'Global'
  sku: { name: 'Standard_Microsoft' }
}

resource cdnEndpoint 'Microsoft.Cdn/profiles/endpoints@2023-05-01' = {
  parent: cdnProfile
  name: cdnEndpointName
  location: 'Global'
  properties: {
    originHostHeader: '${webStorageName}.z6.web.core.windows.net'
    isHttpsAllowed: true
    isHttpAllowed: false
    origins: [
      {
        name: 'blob-origin'
        properties: {
          hostName: '${webStorageName}.z6.web.core.windows.net'
          httpsPort: 443
        }
      }
    ]
  }
}

// ── Outputs ──────────────────────────────────────────────────────────────────
output apiUrl         string = 'https://${functionApp.properties.defaultHostName}'
output functionAppName string = functionApp.name
output webStorageName  string = webStorage.name
output cdnEndpoint     string = cdnEndpoint.properties.hostName
