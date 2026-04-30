# Databricks notebook source
# MAGIC %md
# MAGIC # IMDb Big Data Analysis Project
# MAGIC This notebook analyzes the IMDb dataset to identify trends in movie production, ratings, and genres.
# MAGIC
# MAGIC **Verified Logic:** This script has been pre-validated with local samples.
# MAGIC **Optimized for Databricks Serverless (Spark 4.1.0):** Uses SQL-based approach to bypass Py4J security restrictions.
# MAGIC
# MAGIC ## Data File Locations
# MAGIC - `/Volumes/my_catalog/default/main/title.basics.tsv` - Original IMDb basics data
# MAGIC - `/Volumes/my_catalog/default/main/title.ratings.tsv` - Original IMDb ratings data
# MAGIC - `/Volumes/my_catalog/default/main/new_movies_2026.tsv` - New 2026 movies data
# MAGIC - `/Volumes/my_catalog/default/main/new_ratings_2026.tsv` - New 2026 ratings data
# MAGIC
# MAGIC ## Schema
# MAGIC **title.basics:** tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
# MAGIC **title.ratings:** tconst, averageRating, numVotes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Data Loading
# MAGIC We load the `title_basics` and `title_ratings` tables.
# MAGIC You can load them from the registered catalog tables or directly from the Volumes path.

# COMMAND ----------

from pyspark.sql.functions import col, desc, count, avg, when

# --- Option A: Load from Unity Catalog Tables (Default) ---
# Ensure you have followed the SQL instructions in the roadmap to register these tables.
title_basics = spark.read.table("title_basics")
title_ratings = spark.read.table("title_ratings")

# --- Option B: Load Directly from Volumes (Uncomment to use) ---
# basics_path = "/Volumes/my_catalog/default/main/title.basics.tsv"
# ratings_path = "/Volumes/my_catalog/default/main/title.ratings.tsv"
# title_basics = spark.read.format("csv").options(header="true", delimiter="\t", inferSchema="true").load(basics_path)
# title_ratings = spark.read.format("csv").options(header="true", delimiter="\t", inferSchema="true").load(ratings_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Efficiency Sampling (Development Only)
# MAGIC **Uncomment the line below** to work with a 1% sample of the data. This saves significant tokens and DBU resources during development.

# COMMAND ----------

# title_basics = title_basics.sample(0.01)
# title_ratings = title_ratings.sample(0.01)

# Print counts for verification
print(f"Basics rows to process: {title_basics.count()}")
print(f"Ratings rows to process: {title_ratings.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Data Cleaning
# MAGIC IMDb uses the string `\N` to represent NULL values. We replace these with actual Spark `NULL` values to ensure correct analysis.

# COMMAND ----------

title_basics = title_basics.replace("\\N", None)
title_ratings = title_ratings.replace("\\N", None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Data Integration (Join)
# MAGIC We join both datasets using the unique identifier `tconst`.

# COMMAND ----------

movies_joined = title_basics.join(title_ratings, on="tconst", how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Data Filtering
# MAGIC We isolate records where `titleType` is 'movie' and ensure critical metadata (titles, years, genres, ratings) is not null.

# COMMAND ----------

movies = movies_joined.filter(col("titleType") == "movie") \
               .filter(col("primaryTitle").isNotNull()) \
               .filter(col("startYear").isNotNull()) \
               .filter(col("genres").isNotNull()) \
               .filter(col("averageRating").isNotNull()) \
               .filter(col("numVotes").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Data Transformation
# MAGIC We cast columns to their appropriate data types (Integer, Double) for mathematical operations.

# COMMAND ----------

movies = movies.withColumn("startYear", col("startYear").cast("int")) \
               .withColumn("runtimeMinutes", col("runtimeMinutes").cast("int")) \
               .withColumn("averageRating", col("averageRating").cast("double")) \
               .withColumn("numVotes", col("numVotes").cast("int"))

# --- SQL Registration Options (Switch ON/OFF as needed) ---
# Option 1: Temporary View (Default - Memory only, ready for Step 12 & 13)
movies.createOrReplaceTempView("movies_view")

# Option 2: Permanent Table (Uncomment to save to Catalog for other users/tools)
# movies.write.mode("overwrite").saveAsTable("final_movies_cleaned")

# Display a preview of cleaned movie data
display(movies.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Top 20 Highly Rated Movies
# MAGIC Identifying movies with the highest average rating.

# COMMAND ----------

display(movies.orderBy(desc("averageRating")).select(
    "primaryTitle", "startYear", "genres", "averageRating", "numVotes"
).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Movie Production Trends
# MAGIC Calculating the total number of movies produced per year.

# COMMAND ----------

movies_by_year = movies.groupBy("startYear").count().orderBy("startYear")
display(movies_by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Top 15 Most Common Genres
# MAGIC Analyzing the distribution of movie genres across the dataset.

# COMMAND ----------

movies_by_genre = movies.groupby("genres").count().orderBy(desc("count"))
display(movies_by_genre.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Average Rating Evolution
# MAGIC Tracking how the average IMDb movie rating has changed over the years.

# COMMAND ----------

avg_rating_by_year = movies.groupBy("startYear") \
    .agg(avg("averageRating").alias("avg_rating")) \
    .orderBy("startYear")

display(avg_rating_by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Genre Explosion (Module 4/6 Compliance)
# MAGIC Movies often have multiple genres (e.g., "Action,Drama"). To analyze them correctly, we "explode" the genre string into individual rows.

# COMMAND ----------

from pyspark.sql.functions import split, explode

# Split the comma-separated genres and explode into multiple rows
movies_exploded = movies.withColumn("genre", explode(split(col("genres"), ",")))

# Top 10 Genres by Average Rating (Minimum 100 movies for significance)
top_genres_by_rating = movies_exploded.groupBy("genre") \
    .agg(
        avg("averageRating").alias("avg_rating"),
        count("tconst").alias("movie_count")
    ) \
    .filter(col("movie_count") >= 100) \
    .orderBy(desc("avg_rating"))

display(top_genres_by_rating)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Statistical Correlation (Module 6 Compliance - SQL)
# MAGIC
# MAGIC **Why SQL instead of pyspark.ml?**
# MAGIC - Databricks Serverless has Py4J security restrictions that block Python ML constructors
# MAGIC - SQL functions (CORR, REGR_*) are built into Databricks engine, bypassing these restrictions
# MAGIC - SQL is also more efficient: it runs in the Spark optimizer, not through Python UDFs
# MAGIC - Perfect fit: correlation is a statistical calculation, not a complex ML pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CORR(runtimeMinutes, numVotes) as runtime_votes_corr,
# MAGIC     CORR(runtimeMinutes, averageRating) as runtime_rating_corr,
# MAGIC     CORR(numVotes, averageRating) as votes_rating_corr,
# MAGIC     CORR(startYear, averageRating) as year_rating_corr
# MAGIC FROM movies_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Predictive Modeling (Module 7 Compliance - SQL)
# MAGIC
# MAGIC **Why SQL instead of pyspark.ml.LinearRegression?**
# MAGIC - Python LinearRegression constructor is also blocked by Py4J security in Serverless
# MAGIC - Databricks SQL provides native regression functions: REGR_SLOPE, REGR_INTERCEPT, REGR_R2
# MAGIC - These functions are optimized and run at the engine level (no Python overhead)
# MAGIC - We can generate predictions directly in SQL using the regression formula
# MAGIC - Result: same analysis, same accuracy, but no security errors and better performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Model Summary: Calculate regression coefficients
# MAGIC SELECT
# MAGIC     REGR_SLOPE(averageRating, numVotes) as slope,
# MAGIC     REGR_INTERCEPT(averageRating, numVotes) as intercept,
# MAGIC     REGR_R2(averageRating, numVotes) as r_squared,
# MAGIC     CORR(numVotes, averageRating) as correlation,
# MAGIC     COUNT(*) as total_movies
# MAGIC FROM movies_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample Predictions: Show actual vs predicted ratings for 20 movies
# MAGIC SELECT
# MAGIC     primaryTitle,
# MAGIC     numVotes,
# MAGIC     averageRating as actual_rating,
# MAGIC     ROUND(
# MAGIC         REGR_INTERCEPT(averageRating, numVotes) OVER () +
# MAGIC         REGR_SLOPE(averageRating, numVotes) OVER () * numVotes,
# MAGIC         2
# MAGIC     ) as predicted_rating,
# MAGIC     ROUND(
# MAGIC         averageRating - (
# MAGIC             REGR_INTERCEPT(averageRating, numVotes) OVER () +
# MAGIC             REGR_SLOPE(averageRating, numVotes) OVER () * numVotes
# MAGIC         ),
# MAGIC         2
# MAGIC     ) as residual
# MAGIC FROM movies_view
# MAGIC ORDER BY numVotes DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13.5: Statistical Regression via SQL (Extended Analysis)
# MAGIC Additional regression statistics and insights from the data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CORR(numVotes, averageRating) as correlation_coefficient,
# MAGIC     REGR_SLOPE(averageRating, numVotes) as regression_slope,
# MAGIC     REGR_INTERCEPT(averageRating, numVotes) as regression_intercept,
# MAGIC     REGR_R2(averageRating, numVotes) as r_squared
# MAGIC FROM movies_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14: Data Merge with 2026 Movies (Module 5 Compliance)
# MAGIC We ingest new 2026 movie data and MERGE it with existing datasets.
# MAGIC Two approaches: Auto Loader (Streaming) or Batch Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ### BEFORE: Count existing movies

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'BEFORE MERGE' as phase,
# MAGIC     COUNT(*) as total_movies,
# MAGIC     MAX(startYear) as latest_year,
# MAGIC     MIN(startYear) as earliest_year,
# MAGIC     ROUND(AVG(averageRating), 2) as avg_rating
# MAGIC FROM movies_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ### APPROACH 1: Auto Loader (Streaming) - Recommended for Serverless
# MAGIC Toggle: UNCOMMENT to use | COMMENT out to skip

# COMMAND ----------

# ============ AUTO LOADER APPROACH (STREAMING) ============
# UNCOMMENT THIS BLOCK TO TEST AUTO LOADER

# basics_stream = spark.readStream \
#     .format("cloudFiles") \
#     .option("cloudFiles.format", "csv") \
#     .option("header", "true") \
#     .option("delimiter", "\t") \
#     .load("/Volumes/my_catalog/default/main/new_movies_2026.tsv")
#
# ratings_stream = spark.readStream \
#     .format("cloudFiles") \
#     .option("cloudFiles.format", "csv") \
#     .option("header", "true") \
#     .option("delimiter", "\t") \
#     .load("/Volumes/my_catalog/default/main/new_ratings_2026.tsv")
#
# # Join the streams
# streaming_joined = basics_stream.join(ratings_stream, on="tconst", how="inner")
#
# # Apply same cleaning and filtering as original data
# streaming_final = streaming_joined.filter(col("titleType") == "movie") \
#     .filter(col("primaryTitle").isNotNull()) \
#     .filter(col("startYear").isNotNull()) \
#     .filter(col("genres").isNotNull()) \
#     .filter(col("averageRating").isNotNull()) \
#     .filter(col("numVotes").isNotNull()) \
#     .withColumn("startYear", col("startYear").cast("int")) \
#     .withColumn("runtimeMinutes", col("runtimeMinutes").cast("int")) \
#     .withColumn("averageRating", col("averageRating").cast("double")) \
#     .withColumn("numVotes", col("numVotes").cast("int"))
#
# # Write to Delta with Auto Loader
# checkpoint_path = "/Volumes/my_catalog/default/main/checkpoint_2026_autoloader"
# merge_table_path = "/Volumes/my_catalog/default/main/movies_2026_merged"
#
# query = streaming_final.writeStream \
#     .queryName("autoloader_2026_movies") \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", checkpoint_path) \
#     .option("path", merge_table_path) \
#     .trigger(availableNow=True) \
#     .start()
#
# query.awaitTermination()
# print("✓ Auto Loader ingestion complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### APPROACH 2: Batch Loading (Simple & Reliable) - RECOMMENDED
# MAGIC Toggle: UNCOMMENT to use | COMMENT out to skip

# COMMAND ----------

# ============ BATCH LOADING APPROACH (SIMPLE) ============
# UNCOMMENT THIS BLOCK TO TEST BATCH LOADING

new_movies_2026 = spark.read.format("csv").options(
    header="true",
    delimiter="\t",
    inferSchema="true"
).load("/Volumes/my_catalog/default/main/new_movies_2026.tsv")

new_ratings_2026 = spark.read.format("csv").options(
    header="true",
    delimiter="\t",
    inferSchema="true"
).load("/Volumes/my_catalog/default/main/new_ratings_2026.tsv")

# Join new data
new_joined = new_movies_2026.join(new_ratings_2026, on="tconst", how="inner")

# Apply same filtering as original
new_data_cleaned = new_joined.filter(col("titleType") == "movie") \
    .filter(col("primaryTitle").isNotNull()) \
    .filter(col("startYear").isNotNull()) \
    .filter(col("genres").isNotNull()) \
    .filter(col("averageRating").isNotNull()) \
    .filter(col("numVotes").isNotNull()) \
    .withColumn("startYear", col("startYear").cast("int")) \
    .withColumn("runtimeMinutes", col("runtimeMinutes").cast("int")) \
    .withColumn("averageRating", col("averageRating").cast("double")) \
    .withColumn("numVotes", col("numVotes").cast("int"))

# Merge with existing movies
movies_view_complete = movies.union(new_data_cleaned)

# Create temporary view
movies_view_complete.createOrReplaceTempView("movies_view_complete")

print("✓ Batch load complete. Merged view created: movies_view_complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### AFTER: Count complete dataset with merged 2026 data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'AFTER MERGE' as phase,
# MAGIC     COUNT(*) as total_movies,
# MAGIC     MAX(startYear) as latest_year,
# MAGIC     MIN(startYear) as earliest_year,
# MAGIC     ROUND(AVG(averageRating), 2) as avg_rating
# MAGIC FROM movies_view_complete;

# COMMAND ----------

# MAGIC %md
# MAGIC ### COMPARISON: Before vs After

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'ORIGINAL' as dataset,
# MAGIC     COUNT(*) as total_movies,
# MAGIC     MAX(startYear) as latest_year,
# MAGIC     ROUND(AVG(averageRating), 2) as avg_rating
# MAGIC FROM movies_view
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'MERGED (OLD + NEW)' as dataset,
# MAGIC     COUNT(*) as total_movies,
# MAGIC     MAX(startYear) as latest_year,
# MAGIC     ROUND(AVG(averageRating), 2) as avg_rating
# MAGIC FROM movies_view_complete
# MAGIC ORDER BY dataset;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Statistics on 2026 Merged Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) as total_2026_movies,
# MAGIC     COUNT(DISTINCT startYear) as years_covered,
# MAGIC     ROUND(AVG(averageRating), 2) as avg_rating,
# MAGIC     ROUND(MIN(averageRating), 2) as min_rating,
# MAGIC     ROUND(MAX(averageRating), 2) as max_rating,
# MAGIC     ROUND(AVG(numVotes), 0) as avg_votes
# MAGIC FROM movies_view_complete
# MAGIC WHERE startYear >= 2026;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample of 2026 Movies in Merged Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     primaryTitle,
# MAGIC     startYear,
# MAGIC     genres,
# MAGIC     averageRating,
# MAGIC     numVotes
# MAGIC FROM movies_view_complete
# MAGIC WHERE startYear >= 2026
# MAGIC LIMIT 20;
