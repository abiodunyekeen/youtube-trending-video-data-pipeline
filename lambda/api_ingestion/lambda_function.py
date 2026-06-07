"""
Lambda: Landing Zone Router
────────────────────────────
Triggered by Step Functions Workflow 1 after malware scanning
and metadata validation have completed.

Reads the scan and validation results then routes the file to
its correct destination — Bronze S3 if the file is clean and
valid, or Quarantine S3 if the file is infected or corrupt and invalid
file structure.The original file is deleted from the Landing Zone after routing.



Environment Variables:
    CLEAN_BUCKET_NAME      — Bronze S3 bucket for clean valid files
    QUARANTINE_BUCKET_NAME — Quarantine S3 bucket for rejected files
"""
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def lambda_handler(event, context):

    # Source file info from Step Functions
    source_bucket = event.get('bucket')
    source_key = event.get('key')

    # Results from previous validation steps
    scan_result = event.get('scan_result', 'INFECTED')
    is_valid = event.get('is_valid', False)

    # Destination buckets from Lambda Environment Variables
    CLEAN_BUCKET = os.environ.get('CLEAN_BUCKET_NAME')
    QUARANTINE_BUCKET = os.environ.get('QUARANTINE_BUCKET_NAME')

    # Validate required input
    if not source_bucket or not source_key:
        logger.error("Missing bucket or key in input payload")

        return {
            'statusCode': 400,
            'status': 'ERROR',
            'error': 'Missing bucket or key'
        }

    # Validate environment variables
    if not CLEAN_BUCKET or not QUARANTINE_BUCKET:
        logger.error("Environment variables missing")

        return {
            'statusCode': 500,
            'status': 'ERROR',
            'error': 'Missing environment variables'
        }

    # Decide routing destination
    if scan_result == "CLEAN" and is_valid:

        dest_bucket = CLEAN_BUCKET
        status = "BRONZE"

    else:

        dest_bucket = QUARANTINE_BUCKET
        status = "QUARANTINE"

    try:

        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }

        # Copy object
        s3.copy_object(
            CopySource=copy_source,
            Bucket=dest_bucket,
            Key=source_key
        )

        # Delete original object
        s3.delete_object(
            Bucket=source_bucket,
            Key=source_key
        )

        logger.info(
            f"File routed successfully → {status}"
        )

        return {
            'statusCode': 200,
            'status': status,
            'moved_to': dest_bucket,
            'bucket': dest_bucket,
            'key': source_key
        }

    except Exception as e:

        logger.error(f"Routing failed: {str(e)}")

        raise e