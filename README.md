# N.D.A.I – Nugget Data & AI Initiative (AFC)

L’objectif est de construire une plateforme **data + IA** permettant :
- la collecte et l’analyse des feedbacks clients (campagnes KYC)
- l’industrialisation d’un pipeline data (batch + streaming)
- la création de dashboards business (ventes + sentiment)


## Prérequis
- Docker + Docker Compose
- (Windows) PowerShell / Terminal


## 1. Lancer l’infrastructure
### Créer le fichier d’environnement
Créer `infra/.env` :
```env
API_KEY=change-me
```

### Démarrer les services
```
cd infra
docker compose up -d --build
```

### Vérifier l’état des services
```
docker compose ps
```

### Voir les logs
```
docker compose logs -f
```

Logsd'un service en particulier:
```
docker compose logs -f api-feedback
```

### Stopper les services
```
docker compose down
```


## 2. API
### Tester api-feedback
```
curl.exe http://localhost:8000/health
```

Attendu:
```
{"status":"ok"}
```

Test sans clé: 
```
curl.exe -X POST "http://localhost:8000/feedback" ` -H "Content-Type: application/json" ` --data-binary "@payload.json"
```

Attendu:
```
{"status":"ok"}
```


Test avec clé: 
```
curl.exe -X POST "http://localhost:8000/feedback" `
  -H "Content-Type: application/json" `
  -H "X-API-Key: <API_KEY>" `
  --data-binary "@payload.json"
```

Attendu: Doit contenir JSON avec feedback_id, stored, topic


