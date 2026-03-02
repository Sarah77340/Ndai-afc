# N.D.A.I – Nugget Data & AI Initiative (AFC)

Plateforme **Data + IA** pour AFC permettant :
- collecte de feedbacks clients (campagnes KYC)
- pipeline **streaming** (feedback) + **batch** (ventes)
- enrichissement (traduction + sentiment) et exposition en base pour dashboards (Metabase)

---

## Stack (local/dev)

Services (dossier `infra/`) :
- **api-feedback** (FastAPI) : endpoint sécurisé `/feedback`, produit des événements Kafka + archive le RAW
- **Kafka + Zookeeper** : event bus
- **MinIO** : stockage RAW (Bronze)
- **PostgreSQL** : stockage curated / tables analytiques
- **(optionnel / plus tard)** Metabase : dashboards

---

## Prérequis
- Docker + Docker Compose
- Python 3.10+ (pour `api_pusher` et scripts `tools/`)
- PowerShell (Windows) ou Terminal (Linux/Mac)

---

# 1) Démarrer l’infrastructure

## 1.1 Créer le fichier d’environnement
Créer `infra/.env` :

```env
API_KEY=change-me
BASIC_USER=ndai
BASIC_PASS=ndai-secret
```

## 1.2 Lancer les services
```powershell
cd infra
docker compose up -d --build
docker compose ps
```

Services attendus (selon compose) : `api-feedback`, `kafka`, `zookeeper`, `minio`, `postgres`, etc.

## 1.3 Logs
```powershell
docker compose logs -f
```

Logs API :
```powershell
docker compose logs -f api-feedback
```

## 1.4 Stop / reset
```powershell
docker compose down
```

Reset complet :
```powershell
docker compose down -v
```

---

# 2) API Feedback (ingestion)

Endpoint :
```text
POST http://localhost:8000/feedback
```

Healthcheck :
```powershell
curl.exe http://localhost:8000/health
```

Attendu :
```json
{"status":"ok"}
```

## 2.1 Test avec API Key
```powershell
curl.exe -X POST "http://localhost:8000/feedback" `
  -H "Content-Type: application/json" `
  -H "X-API-Key: <API_KEY>" `
  --data-binary "@payload.json"
```

Attendu : JSON de réponse contenant `feedback_id`, `stored`, `topic`.

## 2.2 Test avec Basic Auth
```powershell
curl.exe -X POST "http://localhost:8000/feedback" -u ndai:ndai-secret -H "Content-Type: application/json" --data-binary "@payload.json"
```

---

# 3) RAW storage (MinIO)

Console MinIO : http://localhost:9001  
Login : `minio` / `minio12345`

Bucket attendu (feedback) : `raw-feedback/` (ou équivalent) avec des fichiers JSON archivés.

---

# 4) Kafka (events) + traitement streaming

## 4.1 Topics

Topic principal (feedback) :
- `feedback.created`

Vérifier la liste :
```powershell
cd infra
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

## 4.2 Orchestration (Airflow) & Metadata — état actuel

Le projet vise à orchestrer les jobs (streaming + batch) via **Airflow** afin de centraliser :
- planification / déclenchement (DAGs)
- logs d’exécution et retries
- **métadonnées** opérationnelles : runs, états, timings, paramètres
- traçabilité (lineage “source → raw → curated” au niveau pipeline)

 **Pour le moment Airflow est désactivé / instable** (problèmes en cours).  
 Les jobs sont exécutés **manuellement** avec les commandes ci-dessous, tout en conservant Kafka comme backbone d’événements.

## 4.3 Lancer le job Spark Streaming (SANS Airflow)

Depuis la **racine** du projet, lancer le job streaming `feedback_job.py` (Kafka → Postgres) :

```powershell
docker run --rm -it --network infra_default `
  -v "${PWD}:/app" `
  -v "${PWD}\.ivy2:/tmp/.ivy2" `
  apache/spark-py:latest `
  /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 `
  /app/services/processing/feedback_job.py
```

### Rejouer des events (producer)
Toujours à la racine :
```powershell
python tools/replay_feedback_json.py
```

### Vérifier en base (sortie enrichie)
Dans `infra/` :
```powershell
docker compose exec postgres psql -U ndai -d ndai -c "SELECT current_database(); SELECT COUNT(*) FROM campaign_feedback_enriched;"
```

---

# 5) Pipeline batch ventes (sales)

## 5.1 Pré-requis : fichier `sales_data.csv` dans MinIO

Si tu ne vois **PAS** le fichier dans MinIO, alors le job Spark lit un fichier qui n’existe pas → résultat = **0 lignes**.

**Solution :**
1) Aller sur http://localhost:9001  
2) Login : `minio` / `minio12345`  
3) Créer le bucket **`raw-sales`**  
4) Upload `sales_data.csv`

## 5.2 Lancer le job Spark Batch (SANS Airflow)

Depuis la **racine** (attendre la fin complète du job) :
```powershell
docker run --rm -it --network infra_default `
  -v "${PWD}:/app" `
  -v "${PWD}\.ivy2:/tmp/.ivy2" `
  apache/spark-py:latest `
  /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 `
  /app/services/processing/sales_job.py
```

### Vérifier en base
Dans `infra/` :
```powershell
docker compose exec postgres psql -U ndai -d ndai -c "SELECT COUNT(*) FROM sales;"
```

---

# 6) (Optionnel) Data Quality – Great Expectations

Le contrôle qualité se fait via Great Expectations sur :
- `sales`
- `campaign_feedback_enriched`

Installer :
```powershell
pip install great_expectations==1.13.0
python -c "import great_expectations as gx; print(gx.__version__)"
```

Exécuter les validations :
```powershell
cd quality
python ge_run_validations_with_store.py
```

Consulter le rapport :
- `quality/gx/uncommitted/data_docs/local_site/index.html`

---

# 7) Intégration api_pusher (optionnel)

Le projet peut recevoir des feedbacks depuis `api_pusher` (repo externe).  
Config (extrait) : `api_pusher/src/config.ini` :

```ini
[API]
endpoint_url = http://localhost:8080/afc/feedback
method = POST
timeout_seconds = 10

[API_AUTH]
active = True
username = ndai
password = ndai-secret

[GENERATION]
mode = manual
```

Lancer :
```powershell
python src/__main__.py PUSH 10
```

Attendu :
- plusieurs requêtes envoyées
- fichiers visibles dans MinIO
- messages visibles dans Kafka
- lignes en base après traitement

---

## Roadmap (très court)
- Stabiliser Airflow (DAGs streaming/batch, métadonnées d’exécution)
- Metabase : dashboards ventes + sentiment
- Gouvernance : qualité, monitoring, lineage plus fin
