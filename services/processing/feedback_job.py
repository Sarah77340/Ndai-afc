from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType
import time

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "feedback.created"

PG_HOST = "postgres"
PG_PORT = "5432"
PG_DB = "ndai"
PG_USER = "ndai"
PG_PASSWORD = "ndai"
PG_TABLE = "campaign_feedback_enriched"

spark = SparkSession.builder.appName("FeedbackStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("username", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("feedback_date", StringType(), True)
])

df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load())

df_parsed = (df.selectExpr("CAST(value AS STRING) AS value_str")
    .select(from_json(col("value_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("feedback_date", to_date(col("feedback_date")))
)

def write_to_postgres(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    props = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}
    (batch_df.write.mode("append").jdbc(url=jdbc_url, table=PG_TABLE, properties=props))

query_pg = (df_parsed.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/feedback_job_pg")
    .start())

query_console = (df_parsed.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start())

while True:
    if query_pg.exception() is not None:
        raise query_pg.exception()
    time.sleep(5)

spark.streams.awaitAnyTermination()