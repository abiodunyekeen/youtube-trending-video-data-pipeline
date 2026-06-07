# Pipeline Flow

This document walks through every stage of the pipeline in detail —
what happens, which service handles it, and what the output is.

---

## Data Ingestion

### Source 1 — YouTube Data API (automated)

A scheduled EventBridge rule fires daily at 1am UTC and triggers
the `abbey-youtube-data-pipeline-api-ingestion` Lambda function.

The Lambda:
1. Calls the YouTube Data API v3 to fetch trending video statistics
2. Collects data for all configured regions (CA, GB, US, IN, DE, FR, etc.)
3. Writes JSON files to the Landing Zone:

```
s3://abbey-youtube-data-pipeline-landing-zone/youtube/raw_statistics/region=ca/date=2026-06-02/data.json
```

### Source 2 — Kaggle CSV / Manual Upload

Historical data from Kaggle or bulk transfers can be uploaded manually
using any of three methods:

**AWS Console:**
```
S3 → abbey-youtube-data-pipeline-landing-zone
→ youtube/raw_statistics/region=xx/
→ Upload files
```

**AWS CLI:**
```bash
aws s3 cp CAvideos.csv \
  s3://abbey-youtube-data-pipeline-landing-zone/youtube/raw_statistics/region=ca/
```

**SFTP:**
```
Connect to the configured SFTP endpoint
Upload files — they land automatically in the Landing Zone bucket
```

---

## Landing Zone — Governance Gate

Every file that lands in the Landing Zone immediately triggers:

```
S3 Object Created event
    → EventBridge rule: abbey-youtube-landing-zone-ingestion-rule
        → Step Functions Workflow 1
```

### Step Functions Workflow 1 — States

**1. RunScannersInParallel (Parallel state)**

Both scanners run simultaneously to save time:

- **Malware Scanning Lambda** — scans the file content for known
  malware signatures. Returns `scan_result: CLEAN` or `scan_result: INFECTED`

- **Metadata Validation Lambda** — checks the file has the expected
  structure, correct columns, and valid data types.
  Returns `isValid: true` or `isValid: false`

**2. EvaluateScanResults (Pass state)**

Combines the outputs from both scanners into a single routing payload:
```json
{
  "bucket": "abbey-youtube-data-pipeline-landing-zone",
  "key": "youtube/raw_statistics/region=ca/CAvideos.csv",
  "scan_result": "CLEAN",
  "is_valid": true
}
```

**3. RouteFile (Task state — Router Lambda)**

The Router Lambda reads the scan results and decides where the file goes:

| scan_result | is_valid | Destination |
|---|---|---|
| CLEAN | true | Bronze S3 |
| INFECTED | any | Quarantine S3 + SNS alert |
| CLEAN | false | Quarantine S3 + SNS alert |

The Router copies the file to the destination bucket then deletes
it from the Landing Zone. The Landing Zone never accumulates files.

**4. CheckIfQuarantined (Choice state)**

If the file was quarantined, sends an SNS failure notification.
If the file reached Bronze, the workflow ends successfully.

---

## Bronze Layer — Raw Data

Bronze S3 holds raw data exactly as it arrived — no modifications.
Files are organized using Hive-style partitioning:

```
s3://abbey-youtube-data-pipeline-bronze/
└── youtube/
    ├── raw_statistics/
    │   └── region=ca/
    │       └── date=2026-06-02/
    │           └── data.json
    └── raw_statistics_reference_data/
        └── region=ca/
            └── date=2026-06-02/
                └── categories.json
```

---

## Glue Workflow — Transformation Trigger

The Glue Workflow runs automatically every day at 2am UTC (one hour
after the API ingestion at 1am, giving files time to land and process).

**Stage 1 — Glue Crawler**

The crawler scans Bronze S3 and registers or updates tables in the
Glue Data Catalog (`abbey-youtube-data-pipeline-bronze-db`):

```
Tables created/updated:
- raw_statistics        (JSON, partitioned by region + date)
- clean_reference_data  (JSON, partitioned by region + date)
```

**Stage 2 — Bronze → Silver Glue Job**

Triggered automatically when the crawler succeeds. The job:

1. Reads from the Bronze catalog table
2. Detects file format (Kaggle CSV or YouTube API JSON) automatically
3. Enforces schema — casts all columns to correct types
4. Cleanses data — removes null video IDs, standardises region codes
5. Parses trending dates from `YY.DD.MM` format to proper dates
6. Fills null numeric columns with 0
7. Adds derived columns: `like_ratio`, `engagement_rate`
8. Deduplicates — keeps the latest record per video + region + date
9. Adds metadata: `_processed_at`, `_job_name`
10. Writes Parquet to Silver S3 partitioned by region
11. Registers `clean_statistics` table in Silver catalog

---

## Silver Layer — Cleansed Data

Silver S3 holds clean, typed, deduplicated Parquet data:

```
s3://abbey-youtube-data-pipeline-silver/
└── youtube/
    ├── statistics/
    │   └── region=ca/
    │       └── part-00000.snappy.parquet
    └── reference_data/
        └── region=ca/
            └── part-00000.snappy.parquet
```

When new files are written to Silver S3, an EventBridge rule fires:

```
S3 Object Created (silver bucket, youtube/statistics/ prefix)
    → EventBridge rule: abbey-silver-s3-to-dq-rule
        → Step Functions Workflow 2
```

---

## Step Functions Workflow 2 — Data Quality

**Stage 1 — RunDataQualityChecks (Lambda)**

The Data Quality Lambda runs 13 checks against both Silver tables
using Amazon Athena (via `awswrangler`):

```
clean_statistics checks (9):
  ✓ Row count ≥ 10
  ✓ video_id null% < 5%
  ✓ title null% < 5%
  ✓ channel_title null% < 5%
  ✓ views null% < 5%
  ✓ region null% < 5%
  ✓ Schema — all expected columns present
  ✓ Value ranges — no negative views, no extreme values
  ✓ Freshness — data no older than 48 hours

clean_reference_data checks (4):
  ✓ Row count ≥ 10
  ✓ region null% < 5%
  ✓ Schema — all expected columns present
  ✓ Freshness check (skipped if no timestamp column)
```

**Stage 2 — PassOrFailDecision (Choice state)**

If `quality_passed = true` → proceeds to aggregation
If `quality_passed = false` → sends SNS alert and fails

**Stage 3 — RunGlueAggregation (Glue job)**

The Silver→Gold job runs only after all quality checks pass:

1. Reads `clean_statistics` and `clean_reference_data` from Silver catalog
2. Joins with category lookup for category names
3. Produces three Gold tables:
   - `trending_analytics` — daily summaries per region
   - `channel_analytics` — channel performance with regional rankings
   - `category_analytics` — category trends with view share percentages
4. Writes Parquet to Gold S3 partitioned by region
5. Registers tables in Gold catalog (`abbey-youtube-data-pipeline-gold-db`)

---

## Gold Layer — Analytics Ready

Gold S3 holds business-level aggregations optimised for Athena and QuickSight:

```
s3://abbey-youtube-data-pipeline-gold/
└── youtube/
    ├── trending_analytics/region=ca/part-00000.snappy.parquet
    ├── channel_analytics/region=ca/part-00000.snappy.parquet
    └── category_analytics/region=ca/part-00000.snappy.parquet
```

Query example in Athena:
```sql
SELECT region, trending_date_parsed, total_views, avg_engagement_rate
FROM trending_analytics
WHERE region = 'gb'
ORDER BY trending_date_parsed DESC
LIMIT 100;
```
