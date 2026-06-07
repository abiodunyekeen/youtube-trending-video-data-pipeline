#!/bin/bash

set -euo pipefail

BUCKET="abbey-youtube-data-pipeline-landing-zone"

echo "Starting upload..."

for file in ../data/*; do
  filename=$(basename "$file")

  # Extract region (first 2 letters)
  region=$(echo "$filename" | cut -c1-2 | tr '[:upper:]' '[:lower:]')

  # Decide destination based on file type
  if [[ "$filename" == *videos.csv ]]; then
    DEST_PATH="youtube/raw_statistics/region=$region/"

  elif [[ "$filename" == *category_id.json ]]; then
    DEST_PATH="youtube/raw_statistics_reference_data/region=$region/"

  else
    echo "Skipping unknown file: $filename"
    continue
  fi

  echo "Uploading $filename to $DEST_PATH"

  aws s3 cp "$file" "s3://$BUCKET/$DEST_PATH"
done

echo "Upload completed successfully."