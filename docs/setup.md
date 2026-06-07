# Architecture

This document explains the design decisions behind the YouTube Trending
Video Data Pipeline, what each component does, and why it was chosen.

---

## Medallion Architecture

The pipeline follows the medallion architecture pattern with three data layers:

| Layer | Bucket | Purpose |
|---|---|---|
| Bronze | `abbey-youtube-data-pipeline-bronze` | Raw data exactly as received — never modified |
| Silver | `abbey-youtube-data-pipeline-silver` | Cleansed, typed, deduplicated Parquet data |
| Gold | `abbey-youtube-data-pipeline-gold` | Business aggregations ready for analytics |

### Why medallion architecture?

- Each layer is independently queryable and reprocessable
- If a transformation is wrong, reprocess from Bronze without re-ingesting
- Clear separation between raw, cleansed and aggregated data
- Industry standard pattern used at Databricks, Netflix, Uber

---

## Landing Zone

The Landing Zone sits before Bronze and acts as a governance gate.

```
Data Source → Landing Zone → (scan + validate) → Bronze or Quarantine
```

### Why a Landing Zone?

In production pipelines, data cannot be trusted at the point of entry.
The Landing Zone ensures:

- No malware enters the data lake
- No corrupt or invalid files reach Bronze
- Every file is auditable — you know what came in, what passed, what failed
- Bronze always contains only clean, validated raw data

### What happens in the Landing Zone?

Every file that lands in the Landing Zone S3 bucket triggers an EventBridge
rule which starts Step Functions Workflow 1:

1. **Malware Scanning Lambda** — scans the file for malware signatures
2. **Metadata Validation Lambda** — checks file format, structure and schema
3. **Router Lambda** — based on results:
   - `CLEAN` + `VALID` → copies file to Bronze S3, deletes from Landing Zone
   - `INFECTED` or `INVALID` → copies file to Quarantine S3, sends SNS alert

---

## Two Independent Workflows

The pipeline is intentionally split into two separate workflows:

### Workflow 1 — Ingestion

- Triggered: per file, event-driven
- Purpose: governance gate — scan, validate, route
- Technology: EventBridge + Step Functions

### Workflow 2 — Transformation

- Triggered: daily schedule (2am UTC)
- Purpose: transform raw data into analytics-ready tables
- Technology: Glue Workflow + EventBridge + Step Functions

### Why keep them separate?

```
Ingestion is event-driven (fires immediately per file)
Transformation is batch (runs once after all files have landed)
```

Combining them would mean the transformation starts after the first
file lands, before all files have arrived. Separating them means
transformation only runs after the full daily batch is in Bronze.

---

## Component Design Decisions

### Why Step Functions over Lambda chaining?

Lambda chaining (Lambda A calls Lambda B calls Lambda C) has no visibility,
no retry logic and no branching. Step Functions provides:

- Visual graph showing exactly where a failure occurred
- Built-in retry with exponential backoff
- Pass/Fail branching with Choice states
- Full execution history with input/output at every state

### Why Glue Workflow for transformation chaining?

The transformation chain (crawler → Bronze→Silver → Silver→Gold) is a
linear sequence where each step depends on the previous. Glue Workflow
handles this natively with event triggers between nodes:

```
Crawler SUCCEEDED → Bronze→Silver job starts
Bronze→Silver SUCCEEDED → Silver→Gold... (not in Glue Workflow)
```

Silver→Gold is intentionally excluded from the Glue Workflow because it
must only run after data quality checks pass. It is triggered by
Step Functions Workflow 2 instead.

### Why EventBridge between Silver S3 and Step Functions?

Glue Workflow cannot directly trigger Step Functions. EventBridge acts
as the bridge — it watches for new objects in Silver S3 and starts the
data quality Step Functions execution automatically.

### Why Athena + QuickSight for analytics?

- Athena queries Parquet files directly from S3 — no data warehouse needed
- QuickSight connects to Athena for dashboards with no ETL between them
- Both are serverless — no infrastructure to manage
- Cost scales with usage — pay per query, not per hour

---

## Data Quality Checks

The Data Quality Lambda runs 13 checks against Silver layer tables before
allowing aggregation to proceed:

| Check | Description |
|---|---|
| Row count | Minimum 10 rows must exist |
| Null percentage | Critical columns must have less than 5% nulls |
| Schema validation | All expected columns must be present |
| Value ranges | No negative views, no extreme values above 50 billion |
| Freshness | Data must be no older than 48 hours |

If any check fails, the pipeline stops and sends an SNS alert. The
Silver→Gold aggregation job only runs after all checks pass.

---

## Storage Design

### S3 Bucket Structure

```
Landing Zone:
youtube/raw_statistics/region=ca/CAvideos.csv
youtube/raw_statistics/region=gb/GBvideos.csv

Bronze:
youtube/raw_statistics/region=ca/date=2026-06-02/data.json
youtube/raw_statistics_reference_data/region=ca/date=2026-06-02/data.json

Silver:
youtube/statistics/region=ca/part-00000.snappy.parquet
youtube/reference_data/region=ca/part-00000.snappy.parquet

Gold:
youtube/trending_analytics/region=ca/part-00000.snappy.parquet
youtube/channel_analytics/region=ca/part-00000.snappy.parquet
youtube/category_analytics/region=ca/part-00000.snappy.parquet
```

### Why Parquet with Snappy compression?

- Parquet is columnar — Athena only reads the columns needed, reducing cost
- Snappy compression reduces storage size by roughly 70% vs raw JSON/CSV
- Both are industry standard for data lake analytics workloads

### Why partition by region?

Partitioning by region means Athena queries like
`WHERE region = 'gb'` only scan the `region=gb/` partition — not the
entire table. This reduces query cost and time significantly.
