from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Initialisation de Spark avec les accès MinIO
spark = SparkSession.builder \
    .appName("IngestionSales") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio12345") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. Lecture du CSV (Extract)
df = spark.read.csv("s3a://raw-sales/sales_data.csv", header=True, inferSchema=True)

# 2. Nettoyage et Cast (Transform)
# On transforme sale_date en type DATE et on s'assure que les prix sont numériques
df_cleaned = df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd")) \
               .withColumn("unit_price", col("unit_price").cast("decimal(10,2)")) \
               .withColumn("total_amount", col("total_amount").cast("decimal(10,2)")) \
               .filter(col("quantity") > 0) # On enlève les anomalies

# 3. Chargement dans Postgres (Load)
df_cleaned.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/ndai") \
    .option("dbtable", "sales") \
    .option("user", "ndai") \
    .option("password", "ndai") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("Ingestion terminée avec succès !")
spark.stop()