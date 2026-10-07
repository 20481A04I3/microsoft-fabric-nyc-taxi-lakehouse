# NYC Taxi Mobility Lakehouse - Microsoft Fabric & Direct Lake

An enterprise-grade, end-to-end Medallion Lakehouse solution built on **Microsoft Fabric**, orchestrating raw data ingestion, PySpark Delta Lake transformations, Direct Lake semantic modeling, and an executive dark-mode Power BI dashboard.

---

## Architecture Overview

```text
[Raw Parquet Ingestion] 
       ↓ (Fabric Data Factory Pipeline)
[Bronze Layer: OneLake Raw Files]
       ↓ (PySpark Notebook Transformation)
[Silver Layer: Cleaned Delta Table (silver_taxi_trips)]
       ↓ (Daily Business Aggregations)
[Gold Layer: Aggregated Delta Table (gold_daily_taxi_metrics)]
       ↓ (Direct Lake Semantic Model)
[Power BI Executive Dashboard]

Key Highlights & Pipeline Stages
1. Bronze Layer (Ingestion): Orchestrated via Fabric Data Factory Pipeline (pl_ingest_taxi_bronze) to batch ingest raw NYC Yellow Taxi trip records directly into OneLake files.

2. Silver Layer (Cleaning & Delta): Executed in PySpark Notebook (bronze_to_silver_cleaning_01) to eliminate invalid trip coordinates, remove negative fare anomalies, filter illogical dates, derive ride durations, and write to ACID-compliant Delta tables.

3. Gold Layer (Aggregations): Aggregated 3M+ trips into daily business metrics (total_revenue, total_trips, avg_distance_miles, avg_duration_minutes, avg_tip_amount) stored as Delta table gold_daily_taxi_metrics.

4. Direct Lake Semantic Layer: Built Direct Lake semantic model (sm_nyctaxi_gold) querying Delta Parquet files in OneLake without data movement, schedule refreshes, or memory latency.

5. Presentation Layer: Built an interactive Power BI dashboard featuring KPI metric cards ($74.63M Revenue, 3M Rides, $3.48 Avg Tip), dual-axis demand vs. revenue curves, traffic duration wave trends, and a dynamic date-range slicer.

Repository Contents
-> bronze_to_silver_cleaning_01.py - PySpark cleaning and aggregation logic.

-> gold_validation.sql - SQL Analytics endpoint queries for Gold metrics validation.

-> Screenshots/ - Architecture lineage, pipeline execution runs, and delta table structures.

-> dashboard_view_complete.png - Full executive Power BI dashboard visual.

