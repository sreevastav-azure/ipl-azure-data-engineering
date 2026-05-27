# 🏏 IPL Data Engineering Pipeline — Azure End-to-End Project

## 📌 Project Overview
An end-to-end data engineering pipeline built on **Microsoft Azure** that ingests, transforms, and analyzes **Indian Premier League (IPL) cricket data** from 2008 to 2020 using the **Medallion Architecture** (Bronze → Silver → Gold).

---

## 🏗️ Architecture

```
Kaggle Dataset (CSV)
        ↓
Azure Data Lake Storage (Bronze Container)
        ↓
Azure Data Factory (ipl_pipeline)
        ↓
    ┌─────────────────────────────────┐
    │  Bronze Notebook                │
    │  → Raw CSV → Delta Tables       │
    └─────────────┬───────────────────┘
                  ↓
    ┌─────────────────────────────────┐
    │  Silver Notebook                │
    │  → Clean + Transform + Type     │
    └─────────────┬───────────────────┘
                  ↓
    ┌─────────────────────────────────┐
    │  Gold Notebook                  │
    │  → Aggregations + Analytics     │
    └─────────────────────────────────┘
        ↓
Azure Databricks (Unity Catalog)
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Azure Data Lake Storage Gen2 | Raw data storage |
| Azure Data Factory | Pipeline orchestration |
| Azure Databricks | Data transformation (PySpark) |
| Delta Lake | Table format |
| Unity Catalog | Data governance |
| PySpark | Data processing |
| SQL | Analytics queries |

---

## 📂 Project Structure

```
ipl-azure-data-engineering/
├── notebooks/
│   ├── ipl_bronze_notebook.py    → Raw data ingestion
│   ├── ipl_silver_notebook.py    → Transformations
│   └── ipl_gold_notebook.py      → Analytics tables
├── README.md
```

---

## 📊 Dataset

**Source:** [Kaggle — IPL Complete Dataset 2008-2020](https://www.kaggle.com/datasets/patrickb1912/ipl-complete-dataset-20082020)

| File | Description | Rows |
|---|---|---|
| matches.csv | Match level data | ~816 matches |
| deliveries.csv | Ball by ball data | ~193,000 deliveries |

---

## 🥉 Bronze Layer
**Catalog:** `ipl_bronze.cricket`

| Table | Description |
|---|---|
| matches | Raw match data as-is from CSV |
| deliveries | Raw ball-by-ball data as-is from CSV |

**Transformations:** None — raw data preserved as-is

---

## 🥈 Silver Layer
**Catalog:** `ipl_silver.cricket`

| Table | Description |
|---|---|
| matches | Cleaned matches with derived columns |
| deliveries | Cleaned deliveries with derived columns |

**Transformations:**
- Fixed data types (result_margin, target_runs, target_overs)
- Added `match_year` and `match_month` columns
- Filled nulls (city → Unknown, winner → No Result)
- Added `is_super_over` flag
- Added `is_boundary`, `is_four`, `is_six` flags on deliveries

---

## 🥇 Gold Layer
**Catalog:** `ipl_gold.cricket`

| Table | Description |
|---|---|
| match_summary | Enriched match results |
| batting_scorecard | Per batter per match stats |
| bowling_scorecard | Per bowler per match stats |
| team_season_stats | Team win/loss record per season |
| batter_season_stats | Season batting leaderboard |
| bowler_season_stats | Season bowling leaderboard |
| partnership_stats | Batter pair partnerships |
| venue_stats | Venue level scoring stats |
| toss_analysis | Toss decision win rate |
| toss_venue_analysis | Venue level toss impact |
| phase_stats | Powerplay/Middle/Death over stats |

---

## 🔍 Key Insights

| Insight | Answer |
|---|---|
| 🏏 Top Run Scorer (All Time) | Virat Kohli |
| 🎯 Top Wicket Taker (All Time) | Yuzvendra Chahal |
| 🏆 Most Successful Team | Mumbai Indians |
| 🌀 Toss Win = Match Win? | ~52% of the time |

---

## ⚙️ Pipeline

**Azure Data Factory Pipeline:** `ipl_pipeline`

```
bronze_notebook → silver_notebook → gold_notebook
     ↓                  ↓                ↓
  ~1 min             ~2 min           ~3 min
                                  Total: ~6m 39s
```

---

## 🚀 How to Run

1. Upload `matches.csv` and `deliveries.csv` to ADLS `ipl-bronze` container
2. Create catalogs and schemas in Databricks:
```sql
CREATE CATALOG IF NOT EXISTS ipl_bronze;
CREATE CATALOG IF NOT EXISTS ipl_silver;
CREATE CATALOG IF NOT EXISTS ipl_gold;
CREATE SCHEMA IF NOT EXISTS ipl_bronze.cricket;
CREATE SCHEMA IF NOT EXISTS ipl_silver.cricket;
CREATE SCHEMA IF NOT EXISTS ipl_gold.cricket;
```
3. Trigger `ipl_pipeline` in Azure Data Factory
4. Query Gold tables for insights

---

## 👨‍💻 Author
**Vastav Thakkellapally**
- Azure Data Engineer
- Skills: Azure Data Factory | Azure Databricks | PySpark | Delta Lake | Unity Catalog | SQL
