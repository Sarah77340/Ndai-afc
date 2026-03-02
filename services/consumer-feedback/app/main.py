import json
import os
import time
import logging
from typing import Optional, Tuple

import psycopg2
import requests
from confluent_kafka import Consumer, KafkaException
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("consumer-feedback")

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "feedback.created")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumer-feedback")
KAFKA_OFFSET_RESET = os.getenv("KAFKA_OFFSET_RESET", "earliest")

# Postgres
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "ndai")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ndai")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ndai")

# Vader thresholds
POS_THRESHOLD = float(os.getenv("POS_THRESHOLD", "0.05"))
NEG_THRESHOLD = float(os.getenv("NEG_THRESHOLD", "-0.05"))

# Translator
TRANSLATOR_URL = os.getenv("TRANSLATOR_URL", "http://translator:5000").rstrip("/")
TRANSLATE_TIMEOUT = float(os.getenv("TRANSLATE_TIMEOUT", "30"))

# Supported languages in LibreTranslate
SUPPORTED_LANGS = set(os.getenv("SUPPORTED_LANGS", "en,fr,es,de,it,pt,ar,zh,ja,hi").split(","))

# Some LT deployments return zh-cn; normalize to zh
LANG_NORMALIZE = {
    "zh-cn": "zh",
    "zh-tw": "zh",
    "zh-hans": "zh",
    "zh-hant": "zh",
}

analyzer = SentimentIntensityAnalyzer()


def label_from_compound(compound: float) -> str:
    if compound >= POS_THRESHOLD:
        return "positive"
    if compound <= NEG_THRESHOLD:
        return "negative"
    return "neutral"


def get_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def normalize_lang(lang: str) -> str:
    lang = (lang or "en").lower().strip()
    return LANG_NORMALIZE.get(lang, lang)


def detect_language(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return "en"

    try:
        r = requests.post(
            f"{TRANSLATOR_URL}/detect",
            json={"q": text},
            timeout=TRANSLATE_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()  # [{"language":"xx","confidence":...}, ...]
        lang = normalize_lang((data[0].get("language") or "en"))
        if lang not in SUPPORTED_LANGS:
            logger.warning("Detected lang=%s not supported -> fallback en", lang)
            return "en"
        return lang
    except Exception as e:
        logger.warning("Lang detect failed -> fallback en: %s", e)
        return "en"


def translate_to_en(text: str, source_lang: str) -> Tuple[Optional[str], bool]:
    text = (text or "").strip()
    if not text:
        return None, False

    if source_lang == "en":
        return text, True

    if source_lang not in SUPPORTED_LANGS:
        return text, False

    payload = {"q": text, "source": source_lang, "target": "en", "format": "text"}

    for attempt in range(1, 4):
        try:
            r = requests.post(
                f"{TRANSLATOR_URL}/translate",
                json=payload,
                timeout=TRANSLATE_TIMEOUT,
            )
            r.raise_for_status()
            out = r.json()
            translated = (out.get("translatedText") or "").strip()

            # Si le service renvoie le même texte, on considère que la traduction a échoué
            if not translated or translated.lower() == text.lower():
                return text, False

            return translated, True
        except Exception as e:
            logger.warning("Translate failed attempt %s/3 (lang=%s): %s", attempt, source_lang, e)
            time.sleep(1.5 * attempt)

    return text, False


def insert_feedback(payload: dict, lang: str, comment_en: Optional[str], method: str, compound: float, label: str):
    username = payload.get("username")
    feedback_date = payload.get("feedback_date")
    campaign_id = payload.get("campaign_id")
    comment = payload.get("comment")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaign_feedback_enriched
                  (username, feedback_date, campaign_id, comment,
                   sentiment_score, sentiment_label, lang, comment_en, sentiment_method)
                VALUES
                  (%s, %s, %s, %s,
                   %s, %s, %s, %s, %s);
                """,
                (username, feedback_date, campaign_id, comment,
                 compound, label, lang, comment_en, method),
            )


def main():
    time.sleep(5)

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": KAFKA_OFFSET_RESET,
        "enable.auto.commit": True,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    logger.info("Consumer started topic=%s bootstrap=%s group=%s", KAFKA_TOPIC, KAFKA_BOOTSTRAP, KAFKA_GROUP_ID)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            raw = (msg.value() or b"").decode("utf-8", errors="ignore").strip()
            if not raw:
                continue

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Non-JSON skipped: %r", raw[:200])
                continue

            try:
                text = (payload.get("comment") or "").strip()

                lang = detect_language(text)
                comment_en, translated_ok = translate_to_en(text, lang)

                scores = analyzer.polarity_scores(comment_en or "")
                compound = float(scores["compound"])
                label = label_from_compound(compound)

                if lang == "en":
                    method = "vader_en"
                else:
                    method = "vader_translated" if translated_ok else "vader_fallback"

                insert_feedback(payload, lang, comment_en, method, compound, label)

                logger.info(
                    "Saved campaign=%s lang=%s method=%s label=%s compound=%.3f",
                    payload.get("campaign_id"),
                    lang,
                    method,
                    label,
                    compound,
                )

            except Exception as e:
                logger.exception("error processing message (skip): %s", e)

    finally:
        consumer.close()


if __name__ == "__main__":
    main()