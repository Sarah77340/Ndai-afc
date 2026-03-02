import json
import os
import time
import logging
import hashlib

import psycopg2
import requests
from langdetect import detect, LangDetectException
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

# Sentiment thresholds
POS_THRESHOLD = float(os.getenv("POS_THRESHOLD", "0.05"))
NEG_THRESHOLD = float(os.getenv("NEG_THRESHOLD", "-0.05"))

# Translator (LibreTranslate)
TRANSLATOR_URL = os.getenv("TRANSLATOR_URL", "http://translator:5000").rstrip("/")
TRANSLATE_TIMEOUT = float(os.getenv("TRANSLATE_TIMEOUT", "30"))

# Langs supported by your translator container (keep aligned with LT_LOAD_ONLY)
# NOTE: libretranslate usually expects "zh" (not zh-cn / zh-hans)
SUPPORTED_LANGS = {
    "en", "fr", "es", "de", "it", "pt", "ar", "ja", "hi", "zh"
}

# map common detected variants -> libretranslate codes
LANG_NORMALIZE = {
    "zh-cn": "zh",
    "zh-tw": "zh",
    "zh-hans": "zh",
    "zh-hant": "zh",
    "jp": "ja",
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


def ensure_schema():
    """
    Adds columns if missing, and creates unique index for idempotency.
    Safe to call on each start.
    """
    ddl = """
    ALTER TABLE campaign_feedback_enriched
      ADD COLUMN IF NOT EXISTS lang VARCHAR(10),
      ADD COLUMN IF NOT EXISTS comment_en TEXT,
      ADD COLUMN IF NOT EXISTS sentiment_method VARCHAR(50),
      ADD COLUMN IF NOT EXISTS feedback_hash TEXT;
    """
    idx = """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_feedback_hash
    ON campaign_feedback_enriched(feedback_hash);
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute(idx)
    logger.info("✅ Schema OK (lang/comment_en/sentiment_method/feedback_hash + uq_feedback_hash)")


def compute_hash(payload: dict) -> str:
    username = (payload.get("username") or "").strip()
    feedback_date = (payload.get("feedback_date") or "").strip()
    campaign_id = (payload.get("campaign_id") or "").strip()
    comment = (payload.get("comment") or "").strip()
    key = f"{username}|{feedback_date}|{campaign_id}|{comment}".encode("utf-8")
    return hashlib.sha256(key).hexdigest()


def normalize_lang(lang: str) -> str:
    if not lang:
        return "en"
    lang = lang.strip().lower()
    return LANG_NORMALIZE.get(lang, lang)


def detect_lang(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return "en"
    try:
        lang = detect(text)
        return normalize_lang(lang)
    except LangDetectException:
        return "en"
    except Exception as e:
        logger.warning("⚠️ langdetect failed, fallback en: %s", e)
        return "en"


def translate_to_en(text: str, source_lang: str) -> str:
    """
    Uses LibreTranslate: POST /translate
    Returns translated text or raises.
    """
    payload = {
        "q": text,
        "source": source_lang,
        "target": "en",
        "format": "text",
    }
    r = requests.post(
        f"{TRANSLATOR_URL}/translate",
        json=payload,
        timeout=TRANSLATE_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return (data.get("translatedText") or "").strip()


def insert_feedback(payload: dict, lang: str, comment_en: str, method: str, compound: float, label: str):
    username = payload.get("username")
    feedback_date = payload.get("feedback_date")
    campaign_id = payload.get("campaign_id")
    comment = payload.get("comment")
    feedback_hash = compute_hash(payload)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaign_feedback_enriched
                  (username, feedback_date, campaign_id, comment,
                   lang, comment_en, sentiment_method,
                   sentiment_score, sentiment_label, feedback_hash)
                VALUES
                  (%s, %s, %s, %s,
                   %s, %s, %s,
                   %s, %s, %s)
                ON CONFLICT (feedback_hash) DO NOTHING;
                """,
                (username, feedback_date, campaign_id, comment,
                 lang, comment_en, method,
                 compound, label, feedback_hash),
            )


def main():
    # small startup delay so Postgres/Kafka are ready
    time.sleep(5)

    # ensure schema is ready
    try:
        ensure_schema()
    except Exception as e:
        logger.exception("❌ Schema ensure failed (stop): %s", e)
        raise

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": KAFKA_OFFSET_RESET,
        "enable.auto.commit": True,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    logger.info(
        "✅ Consumer started topic=%s bootstrap=%s group=%s",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP, KAFKA_GROUP_ID
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            raw = (msg.value() or b"").decode("utf-8", errors="ignore").strip()
            if not raw:
                logger.warning("⚠️ empty message skipped")
                continue

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("⚠️ non-JSON message skipped: %r", raw[:200])
                continue

            comment = (payload.get("comment") or "").strip()
            if not comment:
                logger.warning("⚠️ empty comment skipped payload=%s", payload)
                continue

            # 1) detect lang
            lang = detect_lang(comment)

            # If lang not supported by translator, fallback to en analysis
            # (still stored with lang=en + comment_en=original text)
            norm_lang = normalize_lang(lang)
            if norm_lang != "en" and norm_lang not in SUPPORTED_LANGS:
                logger.warning("⚠️ detected lang=%s not supported -> fallback en", norm_lang)
                norm_lang = "en"

            # 2) translate if needed
            method = "vader_en"
            comment_en = comment

            if norm_lang != "en":
                try:
                    comment_en = translate_to_en(comment, norm_lang)
                    if not comment_en:
                        comment_en = comment  # fallback
                    method = "vader_translated"
                except Exception as e:
                    # if translation fails, fallback to original text, still record lang + method translated
                    logger.warning("⚠️ translate failed (lang=%s): %s", norm_lang, e)
                    comment_en = comment
                    method = "vader_translated"

            # 3) vader on english text (or fallback)
            scores = analyzer.polarity_scores(comment_en)
            compound = float(scores["compound"])
            label = label_from_compound(compound)

            # 4) insert idempotent
            try:
                insert_feedback(payload, norm_lang, comment_en, method, compound, label)
                logger.info(
                    "✅ saved campaign=%s lang=%s method=%s label=%s compound=%.3f",
                    payload.get("campaign_id"), norm_lang, method, label, compound
                )
            except Exception as e:
                logger.exception("❌ db insert failed (skip): %s", e)

    finally:
        consumer.close()


if __name__ == "__main__":
    main()