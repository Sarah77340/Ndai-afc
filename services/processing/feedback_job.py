from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configuration pour le réseau DOCKER
KAFKA_BOOTSTRAP_SERVERS = "infra-kafka-1:9092"
KAFKA_TOPIC = "feedback.created"

spark = SparkSession.builder \
    .appName("FeedbackStreaming") \
    .getOrCreate()

# Schéma des données
schema = StructType([
    StructField("username", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("feedback_date", StringType(), True)
])

# Lecture du flux Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Transformation
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Affichage console pour le test
query_console = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_console.awaitTermination()