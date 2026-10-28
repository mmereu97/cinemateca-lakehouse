# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: clean, validate, normalize
# MAGIC
# MAGIC Takes `bronze.movies_raw` and produces typed, validated, deduplicated
# MAGIC tables:
# MAGIC
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `silver.movies` | one row per film (key: `imdb_id`) |
# MAGIC | `silver.movie_genres` | one row per film × genre |
# MAGIC | `silver.movie_actors` | one row per film × actor (billing order kept) |
# MAGIC | `silver.movie_directors` | one row per film × director |
# MAGIC | `silver.movies_rejected` | quarantined rows + the reasons they failed |
# MAGIC
# MAGIC Design choices:
# MAGIC * **`imdb_id` is the key** — profiling showed it is unique, while the
# MAGIC   spreadsheet's own `movie_id` (column A) has a few duplicates.
# MAGIC * **Quarantine, don't drop**: rows failing validation go to a rejected
# MAGIC   table with explicit reasons, so nothing disappears silently.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "cinemateca"
bronze = spark.table(f"{CATALOG}.bronze.movies_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename + cast
# MAGIC Bronze kept the spreadsheet's Romanian column names; Silver exposes a
# MAGIC clean English schema with proper types.

# COMMAND ----------

movies = bronze.select(
    F.col("col_a").cast("int").alias("movie_id"),
    F.col("imdb_id"),
    F.col("imdb_url"),
    F.trim(F.col("nume_film")).alias("title"),
    F.trim(F.col("nume")).alias("original_title"),
    F.col("anul").cast("int").alias("release_year"),
    F.col("nr").cast("int").alias("duration_minutes"),
    F.col("nota").cast("double").alias("imdb_rating"),
    F.col("nr_p").cast("long").alias("votes_count"),
    F.col("media").cast("double").alias("my_rating"),
    F.col("nr_z").cast("int").alias("oscars_count"),
    F.trim(F.col("rating")).alias("age_rating"),
    F.trim(F.col("limba")).alias("language"),
    F.trim(F.col("tara")).alias("country"),
    F.trim(F.col("color_sau")).alias("color_type"),
    F.trim(F.col("exista")).alias("collection_name"),
    F.col("gen").alias("genres_raw"),
    F.col("gen_l").alias("simplified_genre"),   # precomputed upstream
    F.col("obsevatii_3").alias("tags_raw"),
    F.col("staruri").alias("actors_raw"),
    F.col("regizor").alias("directors_raw"),
    F.col("sex").alias("content_sex"),
    F.col("violence").alias("content_violence"),
    F.col("profanity").alias("content_profanity"),
    F.col("alcohol_drugs").alias("content_drugs"),
    F.col("frightening").alias("content_frightening"),
    F.col("plot"),
    F.col("_ingested_at"),
    F.col("_source_file"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data-quality validation (quarantine pattern)
# MAGIC Each rule produces a reason string; rows with ≥1 reason are quarantined.

# COMMAND ----------

checks = {
    "invalid_imdb_id": F.col("imdb_id").isNull() | ~F.col("imdb_id").rlike(r"^tt\d+$"),
    "missing_title": F.col("title").isNull() | (F.col("title") == ""),
    "year_out_of_range": F.col("release_year").isNull()
        | (F.col("release_year") < 1888) | (F.col("release_year") > 2027),
    "imdb_rating_out_of_range": F.col("imdb_rating").isNotNull()
        & ~F.col("imdb_rating").between(0, 10),
    "my_rating_out_of_range": F.col("my_rating").isNotNull()
        & ~F.col("my_rating").between(0, 10),
    "negative_votes": F.col("votes_count") < 0,
}

flagged = movies.withColumn(
    "rejection_reasons",
    F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in checks.items()])),
)

rejected = flagged.filter(F.size("rejection_reasons") > 0)
valid = flagged.filter(F.size("rejection_reasons") == 0).drop("rejection_reasons")

rejected.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.movies_rejected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate on `imdb_id`
# MAGIC Keep the most recently ingested row per film (window + row_number).

# COMMAND ----------

w = Window.partitionBy("imdb_id").orderBy(F.col("_ingested_at").desc())
deduped = (
    valid.withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

deduped.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.movies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize one-to-many attributes
# MAGIC Genres, actors and directors are comma-separated lists inside one cell —
# MAGIC fine for a spreadsheet, wrong for analytics. `explode` turns them into
# MAGIC proper rows; `posexplode` keeps the actors' billing order.

# COMMAND ----------

silver_movies = spark.table(f"{CATALOG}.silver.movies")

genres = (
    silver_movies
    .select("imdb_id", F.explode(F.split("genres_raw", r",\s*")).alias("genre"))
    .withColumn("genre", F.trim("genre"))
    .filter(F.col("genre") != "")
)
genres.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.movie_genres")

actors = (
    silver_movies
    .select("imdb_id", F.posexplode(F.split("actors_raw", r",\s*"))
            .alias("billing_order", "actor_name"))
    .withColumn("actor_name", F.trim("actor_name"))
    .filter(F.col("actor_name") != "")
)
actors.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.movie_actors")

directors = (
    silver_movies
    .select("imdb_id", F.explode(F.split("directors_raw", r",\s*")).alias("director_name"))
    .withColumn("director_name", F.trim("director_name"))
    .filter(F.col("director_name") != "")
)
directors.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.silver.movie_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality report

# COMMAND ----------

dup_movie_ids = (
    silver_movies.groupBy("movie_id").count().filter("count > 1").count()
)

print(f"silver.movies           : {silver_movies.count():>7} rows")
print(f"silver.movie_genres     : {spark.table(f'{CATALOG}.silver.movie_genres').count():>7} rows")
print(f"silver.movie_actors     : {spark.table(f'{CATALOG}.silver.movie_actors').count():>7} rows")
print(f"silver.movie_directors  : {spark.table(f'{CATALOG}.silver.movie_directors').count():>7} rows")
print(f"silver.movies_rejected  : {spark.table(f'{CATALOG}.silver.movies_rejected').count():>7} rows")
print(f"WARNING duplicate movie_id values (spreadsheet key): {dup_movie_ids}")

display(spark.table(f"{CATALOG}.silver.movies_rejected")
        .select("movie_id", "imdb_id", "title", "release_year", "rejection_reasons"))
