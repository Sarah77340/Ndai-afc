import json
import os
import time
import requests

API_URL = os.getenv("API_URL", "http://localhost:8000/api")
API_KEY = os.getenv("API_KEY")
INPUT_FILE = "feedback_data.json"
SLEEP_SECONDS = 0.5
success = 0
failed = 0
headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY,
}

if not API_KEY:
    raise RuntimeError("API_KEY not set")

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    feedbacks = json.load(f)
print(f"Replaying {len(feedbacks)} feedbacks to {API_URL}")


for idx, feedback in enumerate(feedbacks, start=1):
    try:
        response = requests.post(API_URL, json=feedback, headers=headers, timeout=5)

        if response.status_code == 200:
            success += 1
            print(f"[{idx}] OK (feedback sent succesusfuly)")
        else:
            failed += 1
            print(f"[{idx}] ERROR {response.status_code} - {response.text}")

        time.sleep(SLEEP_SECONDS)

    except Exception as e:
        failed += 1
        print(f"[{idx}] EXCEPTION - {e}")


print(f"Replay finished:\nSuccess: {success}\nFailed: {failed}")
