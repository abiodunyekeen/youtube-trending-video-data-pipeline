"""
Lambda: Metadata Validation
─────────────────────────────
Triggered by Step Functions Workflow 1 as the second parallel branch
alongside Malware Scanning. Validates the structure, schema and data
quality of every file before it is allowed to proceed to Bronze S3.

Supports two file formats used in this pipeline:
    - CSV  — Kaggle YouTube trending statistics datasets
    - JSON — YouTube API category reference data

No file reaches Bronze without passing schema and structure checks.

Validation checks performed:

    Both formats:
        - File exists and is not empty (size > 0)
        - File extension is supported (.csv or .json)

    CSV files:
        - All 16 expected columns are present (schema drift check)
        - Numeric columns (views, likes, dislikes, category_id,
          comment_count) contain castable integer values
        - Only the first 20KB is downloaded for efficiency —
          avoids loading large Kaggle files entirely into memory

    JSON files:
        - Top-level 'items' key exists
        - 'items' is a list
        - First item contains 'id', 'snippet' and 'snippet.title'
          fields — validates YouTube API reference data structure
"""

import boto3
import csv
import io
import json

s3 = boto3.client('s3')

# ─────────────────────────────────────────────────────────────────────────────
# Expected CSV Schema
# ─────────────────────────────────────────────────────────────────────────────
# Used to validate YouTube trending statistics CSV files
# and prevent schema drift in the ingestion layer.
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED_CSV_SCHEMA = {
    'video_id': str,
    'trending_date': str,
    'title': str,
    'channel_title': str,
    'category_id': int,
    'publish_time': str,
    'tags': str,
    'views': int,
    'likes': int,
    'dislikes': int,
    'comment_count': int,
    'thumbnail_link': str,
    'comments_disabled': str,
    'ratings_disabled': str,
    'video_error_or_removed': str,
    'description': str
}


# ─────────────────────────────────────────────────────────────────────────────
# Main Lambda Handler
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):

    # -------------------------------------------------------------------------
    # Get bucket and key from Step Functions input payload
    # -------------------------------------------------------------------------
    bucket = event.get('bucket')
    key = event.get('key')

    # -------------------------------------------------------------------------
    # Validate required event fields
    # -------------------------------------------------------------------------
    if not bucket or not key:

        return {
            'isValid': False,
            'reason': 'Missing bucket or key in input event'
        }

    try:

        # ---------------------------------------------------------------------
        # Get object metadata
        # Used for:
        #   - empty file validation
        #   - file size tracking
        # ---------------------------------------------------------------------
        response = s3.head_object(
            Bucket=bucket,
            Key=key
        )

        file_size = response['ContentLength']

        # ---------------------------------------------------------------------
        # Prevent empty file ingestion
        # ---------------------------------------------------------------------
        if file_size == 0:

            return {
                'isValid': False,
                'reason': 'File is empty'
            }

        # =====================================================================
        # JSON VALIDATION PATH
        # =====================================================================
        # Handles:
        #   - YouTube category reference data
        #   - metadata JSON files
        # =====================================================================

        if key.endswith(".json"):

            # -------------------------------------------------------------
            # Read JSON object from S3
            # -------------------------------------------------------------
            obj = s3.get_object(
                Bucket=bucket,
                Key=key
            )

            body = (
                obj['Body']
                .read()
                .decode('utf-8')
            )

            # -------------------------------------------------------------
            # Parse JSON
            # -------------------------------------------------------------
            data = json.loads(body)

            # -------------------------------------------------------------
            # Validate structure
            # -------------------------------------------------------------
            if 'items' not in data:

                return {
                    'isValid': False,
                    'reason': 'Invalid JSON structure: missing items'
                }

            # -------------------------------------------------------------
            # Ensure items is a list
            # -------------------------------------------------------------
            if not isinstance(data['items'], list):

                return {
                    'isValid': False,
                    'reason': 'Invalid JSON structure: items must be a list'
                }

            # -------------------------------------------------------------
            # Optional Deep Validation
            # Validate first category object structure
            # -------------------------------------------------------------
            if len(data['items']) > 0:

                first_item = data['items'][0]

                if 'id' not in first_item:

                    return {
                        'isValid': False,
                        'reason': 'Missing category id field'
                    }

                if 'snippet' not in first_item:

                    return {
                        'isValid': False,
                        'reason': 'Missing snippet field'
                    }

                if 'title' not in first_item['snippet']:

                    return {
                        'isValid': False,
                        'reason': 'Missing snippet.title field'
                    }

            # -------------------------------------------------------------
            # JSON validation successful
            # -------------------------------------------------------------
            return {

                'isValid': True,

                'bucket': bucket,

                'key': key,

                'processed_metadata': {

                    'file_type': 'json',

                    'file_size': file_size,

                    'record_count': len(data['items'])
                }
            }

        # =====================================================================
        # CSV VALIDATION PATH
        # =====================================================================
        # Handles:
        #   - Kaggle YouTube trending datasets
        #   - raw statistics ingestion
        # =====================================================================

        elif key.endswith(".csv"):

            # -------------------------------------------------------------
            # Download only first chunk of file
            # Efficient for large datasets
            # -------------------------------------------------------------
            obj = s3.get_object(
                Bucket=bucket,
                Key=key,
                Range='bytes=0-20480'
            )

            body = (
                obj['Body']
                .read()
                .decode('utf-8')
            )

            # -------------------------------------------------------------
            # Parse CSV headers
            # -------------------------------------------------------------
            reader = csv.DictReader(
                io.StringIO(body)
            )

            actual_headers = reader.fieldnames

            # -------------------------------------------------------------
            # Schema Validation
            # Prevent schema drift
            # -------------------------------------------------------------
            missing_columns = [

                col

                for col in EXPECTED_CSV_SCHEMA.keys()

                if col not in actual_headers
            ]

            if missing_columns:

                return {

                    'isValid': False,

                    'reason': (
                        f'Schema Drift: '
                        f'Missing columns {missing_columns}'
                    )
                }

            # -------------------------------------------------------------
            # Validate numeric columns using first row
            # -------------------------------------------------------------
            first_row = next(reader)

            numeric_fields = [

                'category_id',
                'views',
                'likes',
                'dislikes',
                'comment_count'
            ]

            for field in numeric_fields:

                try:

                    int(first_row[field])

                except (ValueError, TypeError):

                    return {

                        'isValid': False,

                        'reason': (
                            f'Data Quality Error: '
                            f'Column {field} must be numeric'
                        )
                    }

            # -------------------------------------------------------------
            # CSV validation successful
            # -------------------------------------------------------------
            return {

                'isValid': True,

                'bucket': bucket,

                'key': key,

                'processed_metadata': {

                    'file_type': 'csv',

                    'file_size': file_size,

                    'column_count': len(actual_headers)
                }
            }

        # =====================================================================
        # UNSUPPORTED FILE TYPES
        # =====================================================================

        else:

            return {

                'isValid': False,

                'reason': (
                    f'Unsupported file type: {key}'
                )
            }

    # =========================================================================
    # GLOBAL ERROR HANDLER
    # =========================================================================

    except Exception as e:

        return {

            'isValid': False,

            'reason': (
                f'Validation Failure: {str(e)}'
            )
        }