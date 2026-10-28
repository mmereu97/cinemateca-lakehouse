# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Incremental updates: MERGE INTO + time travel
# MAGIC
# MAGIC The monthly export is a full reload, but real pipelines receive
# MAGIC *increments*: a few new films, a few changed ratings. This notebook
# MAGIC demonstrates the Delta Lake way to apply them — and then undo them:
# MAGIC
# MAGIC 1. Simulate an incoming batch: 1 updated rating + 1 brand-new film.
# MAGIC 2. **`MERGE INTO`** — update-or-insert (upsert) in one atomic statement.
# MAGIC 3. **`DESCRIBE HISTORY`** — every write is a numbered table version.
# MAGIC 4. **Time travel** — query the table *as it was* before the merge.
# MAGIC 5. **`RESTORE`** — roll the table back, leaving the pipeline pristine.
# MAGIC
# MAGIC Without Delta (plain Parquet) an upsert means rewriting whole files by
# MAGIC hand and there is no history to query or restore.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "cinemateca"
TABLE = f"{CATALOG}.silver.movies"

# Remember the current table version so we can time-travel / restore later.
pre_merge_version = spark.sql(f"DESCRIBE HISTORY {TABLE}").select("version").first()[0]
print(f"Table version before merge: {pre_merge_version}")

print("Before merge:")
display(
    spark.table(TABLE)
    .filter(F.col("imdb_id").isin("tt0468569", "tt9999999"))
    .select("imdb_id", "title", "release_year", "my_rating", "votes_count")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The incoming batch
# MAGIC *The Dark Knight* gets a new owner rating, and a (fictional) new film
# MAGIC arrives. In production this DataFrame would come from a new file landing
# MAGIC in the volume.

# COMMAND ----------

updates = spark.createDataFrame(
    [
        # (imdb_id, title, release_year, my_rating, imdb_rating, votes_count, oscars_count)
        ("tt0468569", "The Dark Knight", 2008, 9.9, 9.1, 3_240_696, 2),   # rating change
        ("tt9999999", "Demo: A New Arrival", 2026, 8.0, 7.5, 50_000, 0),  # new film
    ],
    "imdb_id string, title string, release_year int, my_rating double, "
    "imdb_rating double, votes_count long, oscars_count int",
)
updates.createOrReplaceTempView("movie_updates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. MERGE INTO (upsert)

# COMMAND ----------

merge_result = spark.sql(f"""
    MERGE INTO {TABLE} AS target
    USING movie_updates AS source
    ON target.imdb_id = source.imdb_id
    WHEN MATCHED AND target.my_rating <> source.my_rating THEN
        UPDATE SET
            target.my_rating = source.my_rating,
            target.votes_count = source.votes_count
    WHEN NOT MATCHED THEN
        INSERT (imdb_id, title, release_year, my_rating, imdb_rating,
                votes_count, oscars_count)
        VALUES (source.imdb_id, source.title, source.release_year,
                source.my_rating, source.imdb_rating, source.votes_count,
                source.oscars_count)
""")
display(merge_result)  # num_updated_rows=1, num_inserted_rows=1

print("After merge:")
display(
    spark.table(TABLE)
    .filter(F.col("imdb_id").isin("tt0468569", "tt9999999"))
    .select("imdb_id", "title", "release_year", "my_rating", "votes_count")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Every write is a table version

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {TABLE}")
    .select("version", "timestamp", "operation", "operationMetrics")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time travel: query the table as it was *before* the merge

# COMMAND ----------

print(f"The Dark Knight, as of version {pre_merge_version} (before) vs now:")
before = spark.sql(f"""
    SELECT imdb_id, title, my_rating
    FROM {TABLE} VERSION AS OF {pre_merge_version}
    WHERE imdb_id = 'tt0468569'
""")
display(before)

print("The demo film does not exist in the old version:")
ghost = spark.sql(f"""
    SELECT count(*) AS demo_film_rows
    FROM {TABLE} VERSION AS OF {pre_merge_version}
    WHERE imdb_id = 'tt9999999'
""")
display(ghost)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. RESTORE: roll back, leave no trace
# MAGIC The merge above was a demo, so we restore the table to the pre-merge
# MAGIC version. RESTORE is itself a new version — the history keeps everything.

# COMMAND ----------

spark.sql(f"RESTORE TABLE {TABLE} TO VERSION AS OF {pre_merge_version}")

rows = spark.table(TABLE).count()
dk = spark.table(TABLE).filter("imdb_id = 'tt0468569'").select("my_rating").first()[0]
demo = spark.table(TABLE).filter("imdb_id = 'tt9999999'").count()
print(f"Rows after restore: {rows} | Dark Knight rating back to: {dk} | demo film rows: {demo}")
assert demo == 0, "Restore failed: demo film still present"
print("Restore verified — table is back to its pre-merge state.")
