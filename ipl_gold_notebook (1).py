# Databricks notebook source
# MAGIC %md
# MAGIC ## IPL Gold Notebook — Aggregations & Analytics
# MAGIC
# MAGIC Reads from `ipl_silver.cricket.matches` and `ipl_silver.cricket.deliveries`
# MAGIC and produces the following Gold tables:
# MAGIC
# MAGIC | Gold Table | Description |
# MAGIC |---|---|
# MAGIC | `ipl_gold.cricket.match_summary` | One row per match with enriched result info |
# MAGIC | `ipl_gold.cricket.batting_scorecard` | Per-batter per-match aggregates |
# MAGIC | `ipl_gold.cricket.bowling_scorecard` | Per-bowler per-match aggregates |
# MAGIC | `ipl_gold.cricket.team_season_stats` | Team win/loss/NR record per season |
# MAGIC | `ipl_gold.cricket.batter_season_stats` | Season-level batting leaderboard |
# MAGIC | `ipl_gold.cricket.bowler_season_stats` | Season-level bowling leaderboard |
# MAGIC | `ipl_gold.cricket.partnership_stats` | Batter-pair partnership aggregates |
# MAGIC | `ipl_gold.cricket.venue_stats` | Venue-level scoring tendencies |
# MAGIC | `ipl_gold.cricket.toss_analysis` | Toss decision win-rate analysis |
# MAGIC | `ipl_gold.cricket.phase_stats` | Powerplay / Middle / Death over aggregates |

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1 — Read Silver Tables

# COMMAND ----------
from pyspark.sql.functions import (
    col, when, sum as _sum, count, avg, max as _max, min as _min,
    round as _round, lit, concat_ws, least, greatest,
    expr, countDistinct, first
)

matches    = spark.table("ipl_silver.cricket.matches")
deliveries = spark.table("ipl_silver.cricket.deliveries")

# Join deliveries → matches to carry season/date through aggregations
deliveries_enriched = deliveries.join(
    matches.select("id", "season", "match_year", "match_month", "venue", "city"),
    deliveries.match_id == matches.id,
    how="left"
)

print("Silver tables loaded ✅")
deliveries_enriched.cache()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2 — Match Summary

# COMMAND ----------
df_match_summary = matches.select(
    col("id").alias("match_id"),
    col("season"),
    col("match_year"),
    col("match_month"),
    col("city"),
    col("venue"),
    col("date"),
    col("team1"),
    col("team2"),
    col("toss_winner"),
    col("toss_decision"),
    col("winner"),
    col("result"),
    col("result_margin"),
    col("target_runs"),
    col("target_overs"),
    col("is_super_over"),
    col("method"),
    col("player_of_match"),
    col("umpire1"),
    col("umpire2"),
    # Derived
    when(col("winner") == col("toss_winner"), 1).otherwise(0).alias("toss_winner_won_match"),
    when(col("result") == "runs", col("result_margin")).otherwise(None).cast("double").alias("winning_margin_runs"),
    when(col("result") == "wickets", col("result_margin")).otherwise(None).cast("double").alias("winning_margin_wickets"),
)

df_match_summary.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.match_summary")

print(f"match_summary: {df_match_summary.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3 — Batting Scorecard (per batter per match)

# COMMAND ----------
df_batting_scorecard = deliveries_enriched \
    .groupBy("match_id", "season", "inning", "batting_team", "batter") \
    .agg(
        _sum("batsman_runs").alias("runs_scored"),
        count("ball").alias("balls_faced"),
        _sum("is_four").alias("fours"),
        _sum("is_six").alias("sixes"),
        _sum("is_boundary").alias("boundaries"),
        _sum(when(col("player_dismissed") == col("batter"), 1).otherwise(0)).alias("got_out"),
        first("dismissal_kind").alias("dismissal_kind"),
        first("fielder").alias("fielder"),
        first("bowler").alias("dismissed_by"),
    ) \
    .withColumn(
        "strike_rate",
        _round(col("runs_scored") / col("balls_faced") * 100, 2)
    ) \
    .withColumn(
        "is_fifty",
        when((col("runs_scored") >= 50) & (col("runs_scored") < 100), 1).otherwise(0)
    ) \
    .withColumn(
        "is_hundred",
        when(col("runs_scored") >= 100, 1).otherwise(0)
    ) \
    .withColumn(
        "is_duck",
        when((col("runs_scored") == 0) & (col("got_out") == 1), 1).otherwise(0)
    )

df_batting_scorecard.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.batting_scorecard")

print(f"batting_scorecard: {df_batting_scorecard.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4 — Bowling Scorecard (per bowler per match)

# COMMAND ----------
from pyspark.sql.functions import floor

df_bowling_scorecard = deliveries_enriched \
    .groupBy("match_id", "season", "inning", "bowling_team", "bowler") \
    .agg(
        count("ball").alias("balls_bowled"),
        _sum("total_runs").alias("runs_conceded"),
        _sum("is_wicket").alias("wickets"),
        _sum(when(col("extras_type") == "wides", 1).otherwise(0)).alias("wides"),
        _sum(when(col("extras_type") == "noballs", 1).otherwise(0)).alias("no_balls"),
        _sum(when(col("extras_type") == "none", col("total_runs")).otherwise(0)).alias("dot_runs"),
        countDistinct("over").alias("overs_bowled_raw"),
    ) \
    .withColumn("overs_bowled", _round(col("balls_bowled") / 6, 1)) \
    .withColumn(
        "economy_rate",
        _round(col("runs_conceded") / (col("balls_bowled") / 6), 2)
    ) \
    .withColumn(
        "bowling_avg",
        _round(
            when(col("wickets") > 0, col("runs_conceded") / col("wickets")).otherwise(None),
            2
        )
    ) \
    .withColumn(
        "bowling_sr",
        _round(
            when(col("wickets") > 0, col("balls_bowled") / col("wickets")).otherwise(None),
            2
        )
    ) \
    .withColumn(
        "is_three_wicket_haul",
        when((col("wickets") >= 3) & (col("wickets") < 5), 1).otherwise(0)
    ) \
    .withColumn(
        "is_five_wicket_haul",
        when(col("wickets") >= 5, 1).otherwise(0)
    )

df_bowling_scorecard.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.bowling_scorecard")

print(f"bowling_scorecard: {df_bowling_scorecard.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5 — Team Season Stats

# COMMAND ----------
# Build a long table: one row per team per match
team_home = matches.select(
    col("season"), col("team1").alias("team"), col("winner"), col("result")
)
team_away = matches.select(
    col("season"), col("team2").alias("team"), col("winner"), col("result")
)
team_matches = team_home.union(team_away)

df_team_season_stats = team_matches \
    .groupBy("season", "team") \
    .agg(
        count("*").alias("matches_played"),
        _sum(when(col("team") == col("winner"), 1).otherwise(0)).alias("wins"),
        _sum(when((col("team") != col("winner")) & (col("result") != "no result"), 1).otherwise(0)).alias("losses"),
        _sum(when(col("result") == "no result", 1).otherwise(0)).alias("no_results"),
    ) \
    .withColumn("points", col("wins") * 2 + col("no_results")) \
    .withColumn(
        "win_pct",
        _round(col("wins") / col("matches_played") * 100, 2)
    )

df_team_season_stats.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.team_season_stats")

print(f"team_season_stats: {df_team_season_stats.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6 — Batter Season Stats (Leaderboard)

# COMMAND ----------
df_batter_season_stats = deliveries_enriched \
    .groupBy("season", "batting_team", "batter") \
    .agg(
        countDistinct("match_id").alias("matches"),
        count("ball").alias("balls_faced"),
        _sum("batsman_runs").alias("runs_scored"),
        _max("batsman_runs").alias("highest_score_ball"),   # highest off a single ball — see innings level
        _sum("is_four").alias("fours"),
        _sum("is_six").alias("sixes"),
        _sum("is_boundary").alias("boundaries"),
        _sum(when(col("player_dismissed") == col("batter"), 1).otherwise(0)).alias("dismissals"),
    ) \
    .withColumn(
        "batting_avg",
        _round(
            when(col("dismissals") > 0, col("runs_scored") / col("dismissals")).otherwise(col("runs_scored").cast("double")),
            2
        )
    ) \
    .withColumn(
        "strike_rate",
        _round(col("runs_scored") / col("balls_faced") * 100, 2)
    )

# Innings-level aggregation for HS, 50s, 100s, ducks — join from batting_scorecard
innings_agg = df_batting_scorecard \
    .groupBy("season", "batter") \
    .agg(
        _max("runs_scored").alias("highest_score"),
        _sum("is_fifty").alias("fifties"),
        _sum("is_hundred").alias("hundreds"),
        _sum("is_duck").alias("ducks"),
    )

df_batter_season_stats = df_batter_season_stats \
    .join(innings_agg, on=["season", "batter"], how="left")

df_batter_season_stats.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.batter_season_stats")

print(f"batter_season_stats: {df_batter_season_stats.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 7 — Bowler Season Stats (Leaderboard)

# COMMAND ----------
df_bowler_season_stats = deliveries_enriched \
    .groupBy("season", "bowling_team", "bowler") \
    .agg(
        countDistinct("match_id").alias("matches"),
        count("ball").alias("balls_bowled"),
        _sum("total_runs").alias("runs_conceded"),
        _sum("is_wicket").alias("wickets"),
        _sum(when(col("extras_type") == "wides", 1).otherwise(0)).alias("wides"),
        _sum(when(col("extras_type") == "noballs", 1).otherwise(0)).alias("no_balls"),
    ) \
    .withColumn("overs_bowled", _round(col("balls_bowled") / 6, 1)) \
    .withColumn(
        "economy_rate",
        _round(col("runs_conceded") / (col("balls_bowled") / 6), 2)
    ) \
    .withColumn(
        "bowling_avg",
        _round(
            when(col("wickets") > 0, col("runs_conceded") / col("wickets")).otherwise(None),
            2
        )
    ) \
    .withColumn(
        "bowling_sr",
        _round(
            when(col("wickets") > 0, col("balls_bowled") / col("wickets")).otherwise(None),
            2
        )
    )

# Best bowling figures per bowler per season
best_bowling = df_bowling_scorecard \
    .groupBy("season", "bowler") \
    .agg(
        _max("wickets").alias("best_bowling_wickets"),
        _sum("is_five_wicket_haul").alias("five_wicket_hauls"),
        _sum("is_three_wicket_haul").alias("three_wicket_hauls"),
    )

df_bowler_season_stats = df_bowler_season_stats \
    .join(best_bowling, on=["season", "bowler"], how="left")

df_bowler_season_stats.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.bowler_season_stats")

print(f"bowler_season_stats: {df_bowler_season_stats.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 8 — Partnership Stats

# COMMAND ----------
# A partnership is defined as batter + non_striker for the same match + inning + over block
df_partnerships = deliveries_enriched \
    .withColumn(
        "pair",
        when(col("batter") < col("non_striker"),
             concat_ws("_", col("batter"), col("non_striker"))
        ).otherwise(
             concat_ws("_", col("non_striker"), col("batter"))
        )
    ) \
    .groupBy("match_id", "season", "inning", "batting_team", "pair") \
    .agg(
        _sum("batsman_runs").alias("partnership_runs"),
        count("ball").alias("balls"),
        _sum("is_four").alias("fours"),
        _sum("is_six").alias("sixes"),
    ) \
    .withColumn(
        "partnership_sr",
        _round(col("partnership_runs") / col("balls") * 100, 2)
    )

df_partnerships.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.partnership_stats")

print(f"partnership_stats: {df_partnerships.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 9 — Venue Stats

# COMMAND ----------
df_venue_stats = deliveries_enriched \
    .groupBy("venue", "city", "inning") \
    .agg(
        countDistinct("match_id").alias("matches_played"),
        _sum("total_runs").alias("total_runs"),
        _sum("batsman_runs").alias("batting_runs"),
        _sum("extra_runs").alias("extra_runs"),
        _sum("is_wicket").alias("wickets"),
        _sum("is_four").alias("fours"),
        _sum("is_six").alias("sixes"),
        count("ball").alias("balls_bowled"),
    ) \
    .withColumn(
        "avg_runs_per_match",
        _round(col("total_runs") / col("matches_played"), 2)
    ) \
    .withColumn(
        "avg_wickets_per_match",
        _round(col("wickets") / col("matches_played"), 2)
    ) \
    .withColumn(
        "run_rate",
        _round(col("total_runs") / (col("balls_bowled") / 6), 2)
    )

df_venue_stats.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.venue_stats")

print(f"venue_stats: {df_venue_stats.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 10 — Toss Analysis

# COMMAND ----------
df_toss_analysis = matches \
    .groupBy("season", "toss_decision") \
    .agg(
        count("*").alias("toss_count"),
        _sum(when(col("winner") == col("toss_winner"), 1).otherwise(0)).alias("toss_winner_also_won"),
    ) \
    .withColumn(
        "toss_win_match_pct",
        _round(col("toss_winner_also_won") / col("toss_count") * 100, 2)
    )

# Venue-level toss tendency
df_toss_venue = matches \
    .groupBy("venue", "toss_decision") \
    .agg(
        count("*").alias("count"),
        _sum(when(col("winner") == col("toss_winner"), 1).otherwise(0)).alias("won"),
    ) \
    .withColumn("win_pct", _round(col("won") / col("count") * 100, 2))

df_toss_analysis.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.toss_analysis")

df_toss_venue.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.toss_venue_analysis")

print(f"toss_analysis: {df_toss_analysis.count()} rows ✅")
print(f"toss_venue_analysis: {df_toss_venue.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 11 — Phase-wise Stats (Powerplay / Middle / Death)

# COMMAND ----------
df_phases = deliveries_enriched \
    .withColumn(
        "phase",
        when(col("over") <= 6, "Powerplay")
        .when((col("over") >= 7) & (col("over") <= 15), "Middle")
        .otherwise("Death")
    ) \
    .groupBy("match_id", "season", "inning", "batting_team", "bowling_team", "phase") \
    .agg(
        count("ball").alias("balls"),
        _sum("total_runs").alias("runs"),
        _sum("batsman_runs").alias("batting_runs"),
        _sum("extra_runs").alias("extras"),
        _sum("is_wicket").alias("wickets"),
        _sum("is_four").alias("fours"),
        _sum("is_six").alias("sixes"),
    ) \
    .withColumn("run_rate", _round(col("runs") / (col("balls") / 6), 2))

df_phases.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ipl_gold.cricket.phase_stats")

print(f"phase_stats: {df_phases.count()} rows ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 12 — Verify All Gold Tables

# COMMAND ----------
gold_tables = [
    "ipl_gold.cricket.match_summary",
    "ipl_gold.cricket.batting_scorecard",
    "ipl_gold.cricket.bowling_scorecard",
    "ipl_gold.cricket.team_season_stats",
    "ipl_gold.cricket.batter_season_stats",
    "ipl_gold.cricket.bowler_season_stats",
    "ipl_gold.cricket.partnership_stats",
    "ipl_gold.cricket.venue_stats",
    "ipl_gold.cricket.toss_analysis",
    "ipl_gold.cricket.toss_venue_analysis",
    "ipl_gold.cricket.phase_stats",
]

print("=" * 60)
for tbl in gold_tables:
    cnt = spark.table(tbl).count()
    print(f"  {tbl:<45} → {cnt:>7} rows")
print("=" * 60)
print("All Gold tables verified ✅")
