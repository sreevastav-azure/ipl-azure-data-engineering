# Databricks notebook source
# MAGIC %md
# MAGIC ## IPL Silver Notebook — Transformations

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1 — Read Bronze Tables

# COMMAND ----------
matches = spark.table("ipl_bronze.cricket.matches")
deliveries = spark.table("ipl_bronze.cricket.deliveries")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2 — Transform Matches Table

# COMMAND ----------
from pyspark.sql.functions import col, when, year, month, regexp_replace

# 1. Fix result_margin — some are runs some are wickets
# 2. Fix target_runs and target_overs — cast to int/double
# 3. Add match_year and match_month
# 4. Fill nulls
# 5. Fix super_over column

df_matches = matches \
    .withColumn("result_margin", col("result_margin").cast("double")) \
    .withColumn("target_runs", col("target_runs").cast("int")) \
    .withColumn("target_overs", col("target_overs").cast("double")) \
    .withColumn("match_year", year(col("date"))) \
    .withColumn("match_month", month(col("date"))) \
    .withColumn("city", when(col("city").isNull(), "Unknown").otherwise(col("city"))) \
    .withColumn("player_of_match", when(col("player_of_match").isNull(), "No Award").otherwise(col("player_of_match"))) \
    .withColumn("method", when(col("method").isNull(), "Normal").otherwise(col("method"))) \
    .withColumn("winner", when(col("winner").isNull(), "No Result").otherwise(col("winner"))) \
    .withColumn("is_super_over", when(col("super_over") == "Y", 1).otherwise(0)) \
    .select(
        col("id").cast("int"),
        col("season").cast("string"),
        col("city").cast("string"),
        col("date").cast("date"),
        col("match_type").cast("string"),
        col("player_of_match").cast("string"),
        col("venue").cast("string"),
        col("team1").cast("string"),
        col("team2").cast("string"),
        col("toss_winner").cast("string"),
        col("toss_decision").cast("string"),
        col("winner").cast("string"),
        col("result").cast("string"),
        col("result_margin").cast("double"),
        col("target_runs").cast("int"),
        col("target_overs").cast("double"),
        col("is_super_over").cast("int"),
        col("method").cast("string"),
        col("umpire1").cast("string"),
        col("umpire2").cast("string"),
        col("match_year").cast("int"),
        col("match_month").cast("int")
    )

df_matches.display()
print(f"Total matches: {df_matches.count()}")

# COMMAND ----------
# Write matches to silver
df_matches.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_silver.cricket.matches")

print("matches done ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3 — Transform Deliveries Table

# COMMAND ----------
from pyspark.sql.functions import col, when

df_deliveries = deliveries \
    .withColumn("extras_type", when(col("extras_type").isNull(), "none").otherwise(col("extras_type"))) \
    .withColumn("player_dismissed", when(col("player_dismissed").isNull(), "not_out").otherwise(col("player_dismissed"))) \
    .withColumn("dismissal_kind", when(col("dismissal_kind").isNull(), "not_out").otherwise(col("dismissal_kind"))) \
    .withColumn("fielder", when(col("fielder").isNull(), "none").otherwise(col("fielder"))) \
    .withColumn("is_boundary", when(col("batsman_runs") >= 4, 1).otherwise(0)) \
    .withColumn("is_six", when(col("batsman_runs") == 6, 1).otherwise(0)) \
    .withColumn("is_four", when(col("batsman_runs") == 4, 1).otherwise(0)) \
    .select(
        col("match_id").cast("int"),
        col("inning").cast("int"),
        col("batting_team").cast("string"),
        col("bowling_team").cast("string"),
        col("over").cast("int"),
        col("ball").cast("int"),
        col("batter").cast("string"),
        col("bowler").cast("string"),
        col("non_striker").cast("string"),
        col("batsman_runs").cast("int"),
        col("extra_runs").cast("int"),
        col("total_runs").cast("int"),
        col("extras_type").cast("string"),
        col("is_wicket").cast("int"),
        col("player_dismissed").cast("string"),
        col("dismissal_kind").cast("string"),
        col("fielder").cast("string"),
        col("is_boundary").cast("int"),
        col("is_six").cast("int"),
        col("is_four").cast("int")
    )

df_deliveries.display()
print(f"Total deliveries: {df_deliveries.count()}")

# COMMAND ----------
# Write deliveries to silver
df_deliveries.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_silver.cricket.deliveries")

print("deliveries done ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4 — Verify Silver Tables

# COMMAND ----------
print("=== MATCHES ===")
spark.table("ipl_silver.cricket.matches").display()

print("=== DELIVERIES ===")
spark.table("ipl_silver.cricket.deliveries").display()
