import json
import os
import time
import requests
from requests.auth import HTTPBasicAuth # Ajout pour le Basic Auth

# Configuration (Ajustée selon tes tests curl précédents)
API_URL = "http://localhost:8000/feedback"
API_KEY = "ndai-secret" 
BASIC_USER = "ndai"
BASIC_PASS = "ndai-secret"
INPUT_FILE = "data/feedback_data.json" # Vérifie bien le chemin de ton fichier
SLEEP_SECONDS = 1.0 # On commence doucement (1 message par seconde)

headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY,
}

# Chargement des données
try:
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        feedbacks = json.load(f)
except FileNotFoundError:
    print(f"Erreur : Le fichier {INPUT_FILE} est introuvable.")
    exit(1)

print(f" Démarrage du replay : {len(feedbacks)} feedbacks vers {API_URL}")

success = 0
failed = 0

for idx, feedback in enumerate(feedbacks, start=1):
    try:
        # Envoi avec API KEY + BASIC AUTH
        response = requests.post(
            API_URL, 
            json=feedback, 
            headers=headers, 
            auth=HTTPBasicAuth(BASIC_USER, BASIC_PASS), # Ajout ici
            timeout=5
        )

        if response.status_code in [200, 201]:
            success += 1
            print(f"[{idx}] OK")
        else:
            failed += 1
            print(f"[{idx}] ERROR {response.status_code} - {response.text}")

    except Exception as e:
        failed += 1
        print(f"[{idx}] EXCEPTION - {e}")

    time.sleep(SLEEP_SECONDS)

print(f"\n Replay terminé :\nSuccès: {success}\n Échecs: {failed}")