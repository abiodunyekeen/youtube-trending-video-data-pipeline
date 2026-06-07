# Troubleshooting

This document covers every real error encountered building this pipeline
and exactly how each was fixed.

---

## Glue Crawler Issues

### Problem — Crawler generates scattered hash-suffixed tables

**Symptom:**
```
date_2026_05_31
date_2026_05_31_3ca3282841981b17986ad5af3dca2041
date_2026_05_31_4d14af01923f2a314d5f860c40579e10
```

**Cause:**
Mixed file formats (CSV and JSON) coexist in the same region folder,
or a new `date=` partition was added to a folder that previously only
contained flat CSV files. The crawler treats them as incompatible
schemas and creates separate tables with hash suffixes.

**Fix:**
```
Glue → Crawlers → your crawler → Edit → Advanced options
✅ Create a single schema for each S3 path
Table level: 3
Schema changes: Update the table definition in the data catalog
```

Then delete all scattered tables and re-run the crawler.

---

### Problem — Glue job fails with Entity Not Found

**Error:**
```
Entity Not Found (Service: Glue, Status Code: 400)
An error occurred while calling getCatalogSource
```

**Cause:**
The Glue catalog table that the job is trying to read does not exist.
Either the crawler has not run yet, or the table name in the job
parameters does not match the actual catalog table name.

**Fix:**
1. Run the Glue crawler first
2. Check `Glue → Databases → your database → Tables` for the exact table name
3. Update the `--bronze_table` or `--silver_table` job parameter to match exactly

---

## Lambda Issues

### Problem — Data Quality Lambda returns 404 bucket error

**Error:**
```
Waiter BucketExists failed: Max attempts exceeded.
Previously accepted state: Matched expected HTTP status code: 404
```

**Cause:**
`awswrangler` is trying to write Athena query results to a bucket that
either does not exist or the Lambda IAM role cannot access.

**Fix:**
Add `s3_output` explicitly to the Athena query call:
```python
df = wr.athena.read_sql_query(
    sql=query,
    database=database,
    ctas_approach=False,
    s3_output="s3://abbey-youtube-data-pipeline-athena-results/query-results/"
)
```

---

### Problem — Data Quality Lambda returns InvalidRequestException

**Error:**
```
Unable to verify/create output bucket abbey-youtube-data-pipeline-athena-results
```

**Cause:**
The Lambda IAM role does not have S3 permissions on the Athena results bucket.

**Fix:**
Add these permissions to the Lambda IAM role:
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetBucketLocation",
    "s3:GetObject",
    "s3:PutObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::abbey-youtube-data-pipeline-athena-results",
    "arn:aws:s3:::abbey-youtube-data-pipeline-athena-results/*"
  ]
}
```

---

### Problem — Data quality checks return "True" string instead of true boolean

**Symptom:**
```json
"passed": "True"   ← string, not boolean
```

**Cause:**
Pandas operations return `numpy.bool_` not Python `bool`. When
`json.dumps()` serialises numpy booleans it produces the string
`"True"` instead of JSON `true`.

**Fix:**
Wrap all pandas comparison results in `bool()`:
```python
passed = bool(null_pct <= MAX_NULL_PCT)
passed = bool(negative == 0 and extreme == 0)
```

---

### Problem — Freshness check returns NaT and fails

**Symptom:**
```json
{
  "check": "freshness",
  "latest_record": "NaT",
  "passed": false
}
```

**Cause:**
The table has no timestamp column, or all timestamp values are null.
`pd.to_datetime().max()` returns `NaT` (Not a Time) which fails
comparison with the cutoff datetime.

**Fix:**
Add a NaT check before the comparison in `check_freshness`:
```python
latest = pd.to_datetime(df[ts_col]).max()
if pd.isna(latest):
    return {
        "check": "freshness",
        "table": table_name,
        "passed": True,
        "message": "No valid timestamps found — skipping freshness check"
    }
```

---

## Glue Job Issues

### Problem — LAUNCH ERROR, script not found in S3

**Error:**
```
LAUNCH ERROR | Error downloading from S3 for bucket:
aws-glue-assets-608040299654-us-east-1,
key: scripts/abbey-youtube-data-pipeline-bronze-to-silver.py
```

**Cause:**
The Glue script file is missing from S3. This commonly happens when
the `aws-glue-assets` bucket is accidentally emptied.

**Fix:**
```bash
aws s3 cp glue_jobs/bronze_to_silver.py \
  s3://aws-glue-assets-{account-id}-us-east-1/scripts/abbey-youtube-data-pipeline-bronze-to-silver.py
```

**Important:** Never empty the `aws-glue-assets` bucket. It contains
all Glue job scripts, Spark history logs and temporary files.

---

### Problem — Silver→Gold job fails with Entity Not Found

**Error:**
```
Entity Not Found on clean_statistics table
```

**Cause:**
The Bronze→Silver job has not run yet, so `clean_statistics` does not
exist in the Silver catalog. The Silver→Gold job depends on this table.

**Fix:**
Run the pipeline in the correct order:
1. Run Glue crawler first
2. Run Bronze→Silver job
3. Only then run Silver→Gold job

---

## EventBridge Issues

### Problem — Step Functions not triggered after Glue job succeeds

**Cause (most common):** The job name in the EventBridge event pattern
does not exactly match the actual Glue job name.

**Fix:**
```
Glue → Jobs → copy the exact job name
EventBridge → Rules → your rule → Edit → Event pattern
→ verify jobName matches exactly character for character
```

**Test the rule manually:**
```
EventBridge → Rules → your rule → Send test event
```
```json
{
  "source": "aws.glue",
  "detail-type": "Glue Job Run Status",
  "detail": {
    "jobName": "abbey-youtube-data-pipeline-silver-to-gold",
    "state": "SUCCEEDED",
    "jobRunId": "jr_test123"
  }
}
```

---

### Problem — Landing Zone EventBridge rule not triggering

**Cause:** EventBridge notifications are not enabled on the S3 bucket.

**Fix:**
```
S3 → abbey-youtube-data-pipeline-landing-zone
→ Properties → Amazon EventBridge → Edit → ON → Save
```

---

## IAM Issues

### Problem — Glue job cannot access S3

**Fix:**
Attach `AmazonS3FullAccess` to the Glue job IAM role, or add a
scoped inline policy for the specific buckets the job needs to access.

---

### Problem — Lambda cannot start Glue crawler or workflow

**Fix:**
Add to the Lambda IAM role:
```json
{
  "Effect": "Allow",
  "Action": [
    "glue:StartCrawler",
    "glue:GetCrawler",
    "glue:StartWorkflowRun",
    "glue:GetWorkflow"
  ],
  "Resource": "*"
}
```

---

## General Debugging Steps

When something fails, check in this order:

```
1. Step Functions → Executions → click the failed execution
   → expand the failed state → read the error and cause

2. CloudWatch → Log groups → /aws/lambda/function-name
   → find the log stream matching the failure timestamp
   → read the ERROR lines

3. Glue → Jobs → Run history → click the failed run
   → Logs → Driver logs → scroll to ERROR lines

4. EventBridge → Rules → your rule → Monitoring tab
   → check TriggeredRules and FailedInvocations metrics
```
