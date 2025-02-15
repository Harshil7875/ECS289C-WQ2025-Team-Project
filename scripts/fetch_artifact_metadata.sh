#!/bin/bash
# fetch_artifact_metadata.sh
# This script reads artifact image tags from Export.json (downloaded from BugSwarm's dataset website)
# and uses the BugSwarm CLI to fetch detailed metadata for each artifact.
# The metadata for each artifact is stored as a separate JSON file in the "metadata" directory.
#
# When a rate limit error is encountered, the script will retry fetching metadata for that artifact
# after waiting for a specified delay. It will retry up to a maximum number of times before skipping.

# Settings for retry mechanism
max_retries=5
retry_delay=60  # in seconds

# Check if Export.json exists in the current directory
if [ ! -f Export.json ]; then
  echo "Export.json not found! Please place Export.json in the current directory."
  exit 1
fi

# Create an output directory for metadata files
output_dir="Data/metadata"
mkdir -p "$output_dir"

# Use jq to extract the image tags from Export.json.
image_tags=$(jq -r '.[] | .image_tag' Export.json)

# Loop through each extracted image tag
for tag in $image_tags; do
  echo "Fetching metadata for artifact: $tag"
  
  attempt=1
  while true; do
    echo "Attempt $attempt for $tag"
    # Capture both stdout and stderr
    output=$(bugswarm show --image-tag "$tag" 2>&1)
    exit_code=$?
    
    # Save the output to a file (even if it contains an error message)
    output_file="${output_dir}/${tag}_metadata.json"
    echo "$output" > "$output_file"
    
    # Check if the output contains the rate limiting error message.
    if echo "$output" | grep -q "You are being rate limited"; then
      echo "[WARN] Rate limit encountered for $tag. Waiting $retry_delay seconds before retrying."
      attempt=$((attempt+1))
      if [ $attempt -gt $max_retries ]; then
        echo "[ERROR] Max retries reached for $tag. Skipping this artifact."
        break
      fi
      sleep $retry_delay
    else
      if [ $exit_code -eq 0 ]; then
        echo "Successfully fetched metadata for $tag."
      else
        echo "[ERROR] An error occurred while fetching metadata for $tag."
      fi
      break
    fi
  done
done

echo "All metadata fetched and stored in the '$output_dir' directory."
