# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: final score, elite ranking, analytics
# MAGIC
# MAGIC Reimplements the scoring rules from
# MAGIC [`docs/SCORING.md`](https://github.com/mmereu97/cinemateca-data-pipeline/blob/main/docs/SCORING.md)
# MAGIC of the original project — previously computed in pandas/SQLite — as a
# MAGIC distributed PySpark job, then builds small analytics tables for the
# MAGIC dashboard.
# MAGIC
# MAGIC | Table | Content |
# MAGIC |---|---|
# MAGIC | `gold.movie_scores` | final score, elite bonus/flag/rank per film |
# MAGIC | `gold.movies_by_decade` | counts and average ratings per decade |
# MAGIC | `gold.movies_by_genre` | counts, averages and elite counts per genre |
# MAGIC | `gold.top_directors` | directors ranked by average owner rating (≥ 5 films) |
# MAGIC | `gold.rating_divergence` | films where the owner disagrees most with IMDb |
# MAGIC
# MAGIC The notebook ends with a **validation cell**: the known-good top 3
# MAGIC (verified against the legacy implementation) must come out identical,
# MAGIC otherwise the run fails. This is regression testing for transformations.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "cinemateca"
movies = spark.table(f"{CATALOG}.silver.movies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final score
# MAGIC
# MAGIC ```
# MAGIC final_score = clamp(1, 100, round(
# MAGIC       my_rating * 10
# MAGIC     + (my_rating - imdb_rating) * 10     # bonus when owner > IMDb
# MAGIC     + (my_rating < 5    ? -10 : 0)       # low-rating penalty
# MAGIC     + (votes < 15000    ? -10 : 0)       # thin-votes penalty
# MAGIC     + oscars                             # one point per Oscar
# MAGIC     + elite_bonus                        # era-aware, see below
# MAGIC     + floor(votes / 100000)              # popularity bonus
# MAGIC ))
# MAGIC ```
# MAGIC The elite bonus requires `my_rating ≥ 7` **and** enough votes *for the
# MAGIC film's era* — older films face a lower vote bar than recent ones.

# COMMAND ----------

# Era-aware minimum votes for the elite bonus.
era_threshold = (
    F.when(F.col("release_year") <= 1965, 40_000)
    .when(F.col("release_year") <= 1989, 100_000)
    .when(F.col("release_year") <= 1999, 130_000)
    .when(F.col("release_year") <= 2009, 300_000)
    .otherwise(400_000)
)

elite_bonus = (
    F.when(
        (F.col("my_rating") >= 7) & (F.col("votes_count") >= era_threshold),
        F.when(F.col("my_rating") > 8, 10).otherwise(5),
    ).otherwise(0)
)

scored = (
    movies
    .withColumn("elite_bonus", elite_bonus)
    .withColumn(
        "final_score_raw",
        F.round(
            F.col("my_rating") * 10
            + (F.col("my_rating") - F.col("imdb_rating")) * 10
            + F.when(F.col("my_rating") < 5, -10).otherwise(0)
            + F.when(F.col("votes_count") < 15_000, -10).otherwise(0)
            + F.coalesce(F.col("oscars_count"), F.lit(0))
            + F.col("elite_bonus")
            + F.floor(F.col("votes_count") / 100_000)
        ),
    )
    .withColumn("final_score", F.least(F.lit(100), F.greatest(F.lit(1), F.col("final_score_raw"))))
    .withColumn("is_elite", (F.col("final_score") >= 80) & (F.col("elite_bonus") > 0))
    .drop("final_score_raw")
)

# Elite rank: position among elite films, by owner rating then votes.
rank_window = Window.orderBy(F.col("my_rating").desc(), F.col("votes_count").desc())
elite_ranked = (
    scored.filter("is_elite")
    .withColumn("elite_rank", F.row_number().over(rank_window))
    .select("imdb_id", "elite_rank")
)

movie_scores = (
    scored.join(elite_ranked, "imdb_id", "left")
    .select(
        "imdb_id", "movie_id", "title", "release_year", "simplified_genre",
        "my_rating", "imdb_rating", "votes_count", "oscars_count",
        "elite_bonus", "final_score", "is_elite", "elite_rank",
    )
)

movie_scores.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.movie_scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation: the top 3 must match the legacy implementation

# COMMAND ----------

expected_top3 = ["The Shawshank Redemption", "The Godfather", "The Dark Knight"]
actual_top3 = [
    r["title"].strip()
    for r in spark.table(f"{CATALOG}.gold.movie_scores")
    .filter("is_elite").orderBy("elite_rank").limit(3).collect()
]
print("Expected:", expected_top3)
print("Actual  :", actual_top3)
assert actual_top3 == expected_top3, "Top 3 does not match the legacy scoring!"

n_elite = spark.table(f"{CATALOG}.gold.movie_scores").filter("is_elite").count()
print(f"Elite films: {n_elite} — validation PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics tables for the dashboard

# COMMAND ----------

scores = spark.table(f"{CATALOG}.gold.movie_scores")

# --- Films per decade --------------------------------------------------------
by_decade = (
    scores
    .withColumn("decade", (F.floor(F.col("release_year") / 10) * 10).cast("int"))
    .groupBy("decade")
    .agg(
        F.count("*").alias("films"),
        F.round(F.avg("my_rating"), 2).alias("avg_my_rating"),
        F.round(F.avg("imdb_rating"), 2).alias("avg_imdb_rating"),
        F.sum(F.col("is_elite").cast("int")).alias("elite_films"),
    )
    .orderBy("decade")
)
by_decade.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.movies_by_decade")

# --- Films per genre ---------------------------------------------------------
genres = spark.table(f"{CATALOG}.silver.movie_genres")
by_genre = (
    genres.join(scores, "imdb_id")
    .groupBy("genre")
    .agg(
        F.count("*").alias("films"),
        F.round(F.avg("my_rating"), 2).alias("avg_my_rating"),
        F.round(F.avg("final_score"), 1).alias("avg_final_score"),
        F.sum(F.col("is_elite").cast("int")).alias("elite_films"),
    )
    .orderBy(F.col("films").desc())
)
by_genre.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.movies_by_genre")

# --- Top directors (at least 5 films) ---------------------------------------
directors = spark.table(f"{CATALOG}.silver.movie_directors")
top_directors = (
    directors.join(scores, "imdb_id")
    .groupBy("director_name")
    .agg(
        F.count("*").alias("films"),
        F.round(F.avg("my_rating"), 2).alias("avg_my_rating"),
        F.sum(F.col("is_elite").cast("int")).alias("elite_films"),
    )
    .filter(F.col("films") >= 5)
    .orderBy(F.col("avg_my_rating").desc())
)
top_directors.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.top_directors")

# --- Where the owner disagrees with IMDb ------------------------------------
divergence = (
    scores
    .withColumn("rating_gap", F.round(F.col("my_rating") - F.col("imdb_rating"), 1))
    .select("imdb_id", "title", "release_year", "my_rating", "imdb_rating",
            "rating_gap", "votes_count")
    .orderBy(F.col("rating_gap").desc())
)
divergence.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.gold.rating_divergence")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

print("Top 10 elite films:")
display(scores.filter("is_elite").orderBy("elite_rank").limit(10))

# COMMAND ----------

print("Most over-rated by the owner vs IMDb (top 5) and most under-rated (bottom 5):")
display(spark.table(f"{CATALOG}.gold.rating_divergence").limit(5))
display(spark.table(f"{CATALOG}.gold.rating_divergence").orderBy("rating_gap").limit(5))
