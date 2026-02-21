# N.D.A.I – Nugget Data & AI Initiative (AFC)

Projet de plateforme **Data + IA** pour AFC permettant :
- collecte de feedbacks clients (campagnes KYC)
- pipeline data streaming + batch
- dashboards business (ventes + sentiment)

---

# État actuel du projet

- Infrastructure Docker fonctionnelle  
- API privée d’ingestion de feedbacks  
- Intégration avec api_pusher  
- Stockage RAW dans MinIO  

---

# Prérequis
- Docker + Docker Compose
- Python (pour api_pusher)
- PowerShell / Terminal

---

# 1. Lancer l’infrastructure

## 1.1 Créer le fichier d’environnement

Créer `infra/.env` :

```env
API_KEY=change-me
BASIC_USER=ndai
BASIC_PASS=ndai-secret

```

--- 
## 1.2 Démarrer les services
```
cd infra
docker compose up -d --build
```

## 1.3 Vérifier état
```
docker compose ps
```
Services attendus : api-feedback, kafka, zookeeper, minio


## 1.4 Voir logs
```
docker compose logs -f
```

Pour vérifier celui de l'api :
```
docker compose logs -f api-feedback
```
---

## 1.5 Stopper
```
docker compose down
```

Reset complet :
```
docker compose down -v
```

---
# 2. API Feedback

Endpoint :
```
POST http://localhost:8000/feedback
```

Healthcheck :
```
curl.exe http://localhost:8000/health
```
Attendu : 
```
{"status":"ok"}
```
---

## 2.1 Tester APIkey 
```
curl.exe -X POST "http://localhost:8000/feedback" `
  -H "Content-Type: application/json" `
  -H "X-API-Key: <API_KEY>" `
  --data-binary "@payload.json"
```

Attendu: Doit contenir JSON avec feedback_id, stored, topic

---
## 2.2 Tester Basic Auth 
```
curl.exe -X POST "http://localhost:8000/feedback" -u ndai:ndai-secret -H "Content-Type: application/json" --data-binary "@payload.json"
```

---

# 3. Vérifier les données

## MiniO
Aller dans http://localhost:9001
Login : minio
Mot de passe: minio12345

Bucket attendu dans : raw_feedback/ 
Avec fichier json

---

# Intégration API_pusher
=> API privée pour recevoir les feedbacks clients depuis API_PUSHER, voir https://github.com/Prjprj/api_pusher

## 4.1 Modifier api_pusher/src/config.ini

```
[API]
endpoint_url = http://localhost:8080/afc/feedback
method = POST
timeout_seconds = 10

[API_AUTH]
active = True
username = ndai
password = ndai-secret

[LOG]
log_level = DEBUG
log_file = app.log
log_format = %%(asctime)s - %%(levelname)s - %%(filename)s - %%(funcName)s - %%(lineno)d - %%(message)s

[OLLAMA]
ollama_url = 127.0.0.1:11434
ollama_model = codellama

[GENERATION]
#mode = ollama
mode = manual
```

=> Pour l'instant, mode manuel pour éviter les dépendances Ollama lors des tests.

---

## 4.2 Lancer api_pusher
```
python src/__main__.py PUSH 10
```

Résultat attendu :
- plusieurs requêtes envoyées
- fichiers visibles dans MinIO
- messages visibles dans Kafka


---
# 5. Architecture
```
api_pusher
     ↓
api-feedback (FastAPI sécurisé)
     ↓
MinIO raw-feedback
     ↓
(bientot) Kafka topic feedback.created
```

---
6. Prochaines étapes
- Consumer Kafka Python
- Sentiment analysis (VADER)
- Stockage PostgreSQL
- Dashboards Metabase
- Pipeline batch ventes

---
