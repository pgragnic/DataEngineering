# Déploiement AWS — eToro Portfolio

## Architecture

```
Browser → CloudFront → S3 (React)
                  └──► API Gateway → Lambda (Flask) → eToro API
```

## Prérequis AWS (une seule fois)

### 1. Créer un bucket S3 pour le frontend
```bash
aws s3 mb s3://etoro-portfolio-frontend --region eu-west-1
aws s3 website s3://etoro-portfolio-frontend \
  --index-document index.html --error-document index.html
```

### 2. Créer une distribution CloudFront
- Origin : le bucket S3
- Default root object : `index.html`
- Récupère le domaine (ex: `d1abc123.cloudfront.net`) et l'ID de distribution

### 3. Créer un utilisateur IAM pour GitHub Actions
Policies requises :
- `AWSLambda_FullAccess`
- `AmazonAPIGatewayAdministrator`
- `AWSCloudFormationFullAccess`
- `IAMFullAccess`
- `AmazonS3FullAccess`
- `CloudFrontFullAccess`

## GitHub — Secrets et Variables

### Secrets (Settings → Secrets → Actions)
| Nom | Valeur |
|-----|--------|
| `AWS_ACCESS_KEY_ID` | Clé IAM |
| `AWS_SECRET_ACCESS_KEY` | Secret IAM |
| `ETORO_AGENT_KEY` | Ta clé agent eToro |
| `ETORO_API_KEY` | Ta clé API eToro |

### Variables (Settings → Variables → Actions)
| Nom | Valeur |
|-----|--------|
| `S3_BUCKET` | `etoro-portfolio-frontend` |
| `CLOUDFRONT_ID` | ID de ta distribution |
| `CLOUDFRONT_DOMAIN` | `d1abc123.cloudfront.net` |
| `ETORO_INVESTMENT` | `14892` |

## Déploiement

### Automatique
Push sur `main` → GitHub Actions déploie tout.

### Manuel (depuis ta machine)
```bash
# Backend
cd etoro
sam build
sam deploy \
  --parameter-overrides \
    EtoroAgentKey="..." \
    EtoroApiKey="..."

# Frontend
cd ../etoro-react
VITE_API_URL=https://<api-gateway-url> npm run build
aws s3 sync dist/ s3://etoro-portfolio-frontend --delete
aws cloudfront create-invalidation --distribution-id <ID> --paths "/*"
```

## Coût estimé

| Service | Usage | Coût/mois |
|---------|-------|-----------|
| Lambda | ~5 000 appels | $0 (free tier) |
| API Gateway | ~5 000 appels | $0 (free tier) |
| S3 | < 1 GB | ~$0.02 |
| CloudFront | < 1 GB | ~$0.10 |
| **Total** | | **~$0.12** |
