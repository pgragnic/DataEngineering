# Déploiement Azure — eToro Portfolio

## Architecture

```
Browser → Azure CDN → Blob Storage (React)
                └──► Azure Functions (Flask) → eToro API
```

## Équivalences AWS → Azure

| AWS              | Azure                    |
|------------------|--------------------------|
| Lambda           | Azure Functions          |
| API Gateway      | Functions HTTP trigger   |
| S3 static        | Blob Storage ($web)      |
| CloudFront       | Azure CDN                |
| SAM / CFN        | Bicep                    |
| Mangum           | WsgiMiddleware           |

## Prérequis (une seule fois, ~10 min)

### 1. Créer un Resource Group
```bash
az group create --name etoro-portfolio-rg --location westeurope
```

### 2. Créer un Service Principal pour GitHub Actions
```bash
az ad sp create-for-rbac \
  --name "etoro-github-actions" \
  --role Contributor \
  --scopes /subscriptions/<SUBSCRIPTION_ID>/resourceGroups/etoro-portfolio-rg \
  --json-auth
```
→ Copie le JSON complet, ce sera `AZURE_CREDENTIALS`

## GitHub — Secrets et Variables

### Secrets (Settings → Secrets → Actions)
| Nom | Valeur |
|-----|--------|
| `AZURE_CREDENTIALS` | JSON du service principal |
| `ETORO_AGENT_KEY` | Ta clé agent eToro |
| `ETORO_API_KEY` | Ta clé API eToro |

### Variables (Settings → Variables → Actions)
| Nom | Valeur |
|-----|--------|
| `AZURE_RESOURCE_GROUP` | `etoro-portfolio-rg` |
| `AZURE_CDN_ENDPOINT` | `etoro-portfolio-web.azureedge.net` |
| `ETORO_INVESTMENT` | `14892` |

## Déploiement

### Automatique
Push sur `main` → GitHub Actions déploie tout.

### Manuel (Azure CLI)
```bash
# Infra
az deployment group create \
  --resource-group etoro-portfolio-rg \
  --template-file etoro/azure/main.bicep \
  --parameters etoroAgentKey="..." etoroApiKey="..."

# Backend (après avoir copié lambda_app.py dans azure/)
cp etoro/lambda_app.py etoro/azure/lambda_app.py
cd etoro/azure && func azure functionapp publish etoro-portfolio-api

# Frontend
cd etoro-react
VITE_API_URL=https://etoro-portfolio-api.azurewebsites.net npm run build
az storage blob sync \
  --account-name etoroportfolioweb \
  --container '$web' \
  --source dist
```

## Coût estimé

| Service | Usage | Coût/mois |
|---------|-------|-----------|
| Azure Functions | ~5 000 appels | $0 (1M gratuits) |
| Blob Storage | < 1 GB | ~$0.02 |
| Azure CDN | < 1 GB | ~$0.08 |
| **Total** | | **~$0.10** |
