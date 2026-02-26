from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit

# Configuration simplifiée (sans S3 pour l'instant pour valider le JSON)
spark = SparkSession.builder \
    .appName("IngestionFeedbackLocal") \
    .getOrCreate()

print("--- LECTURE DU FICHIER JSON ---")

# On lit le fichier directement depuis le volume monté (/app)
df = spark.read.option("multiLine", "true").json("/app/data/feedback_data.json")

# Vérification du schéma
df.printSchema()

# Transformation
df_final = df.withColumn("feedback_date", to_date(col("feedback_date"), "yyyy-MM-dd")) \
             .withColumn("sentiment_score", lit(0.0)) \
             .withColumn("sentiment_label", lit("A TRAITER"))

print("--- INSERTION DANS POSTGRES ---")

# Écriture vers Postgres (On utilise le nom du service 'infra-postgres-1' car on est sur le réseau 'infra_default')
df_final.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://infra-postgres-1:5432/ndai") \
    .option("dbtable", "campaign_feedback_enriched") \
    .option("user", "ndai") \
    .option("password", "ndai") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("Feedbacks importés avec succès depuis le fichier local !")
spark.stop()