# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: raw ingestion
# MAGIC
# MAGIC Reads the Parquet file uploaded to the Unity Catalog volume and lands it
# MAGIC **as-is** in a Delta table. Bronze keeps data raw on purpose: if a Silver
# MAGIC transformation turns out to be wrong, we can always rebuild from Bronze
# MAGIC without re-uploading anything.
# MAGIC
# MAGIC Two audit columns are added:
# MAGIC * `_ingested_at` — when the row was loaded
# MAGIC * `_source_file` — which file it came from

# COMMAND ----------

from pyspark.sql import functions as F

RAW_PATH = "/Volumes/cinemateca/bronze/raw/cinemateca_movies.parquet"
TARGET_TABLE = "cinemateca.bronze.movies_raw"

# COMMAND ----------

df = (
    spark.read.parquet(RAW_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_name"))
)

# Full reload: Bronze mirrors the latest export. Incremental arrival of new
# files would use COPY INTO or Auto Loader instead (see README / next steps).
(
    df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

bronze = spark.table(TARGET_TABLE)
print(f"Rows ingested: {bronze.count()}")
display(bronze.select("col_a", "nume_film", "anul", "imdb_id", "_ingested_at", "_source_file").limit(5))
