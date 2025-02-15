# scripts/process-artifact-json.py

import json
import glob
import csv
import os
from datetime import datetime

def parse_timestamp(ts_str):
    """Parse an ISO 8601 timestamp string into a datetime object."""
    try:
        return datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%SZ')
    except Exception as e:
        return None

# Directory where metadata JSON files are stored (created earlier by the CLI script)
input_dir = "/Users/harshil/Developer/GitHub_Repos/FailFix/Data/metadata"
# Output CSV file to store the flattened data
output_csv = "/Users/harshil/Developer/GitHub_Repos/FailFix/Data/processed/artifact_data.csv"

# Define the CSV columns for output
fieldnames = [
    'image_tag', 
    'language', 
    'failed_commit', 
    'passed_commit', 
    'time_to_fix_hours', 
    'exceptions'
]

data_rows = []

# Loop over all JSON files in the metadata directory
for filename in glob.glob(f"{input_dir}/*.json"):
    # Check if file is empty
    if os.path.getsize(filename) == 0:
        print(f"Warning: {filename} is empty. Skipping.")
        continue

    with open(filename, 'r') as f:
        try:
            artifact = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in {filename}: {e}. Skipping.")
            continue
    
    # Extract basic fields
    image_tag = artifact.get("image_tag")
    language = artifact.get("lang")
    
    # Extract commit timestamps from failed and passed jobs
    failed_commit_ts = artifact.get("failed_job", {}).get("committed_at")
    passed_commit_ts = artifact.get("passed_job", {}).get("committed_at")
    
    # Compute time-to-fix (in hours) if both timestamps are available
    time_to_fix_hours = None
    if failed_commit_ts and passed_commit_ts:
        failed_dt = parse_timestamp(failed_commit_ts)
        passed_dt = parse_timestamp(passed_commit_ts)
        if failed_dt and passed_dt:
            time_to_fix_hours = (passed_dt - failed_dt).total_seconds() / 3600.0
    
    # Extract exception details from the classification object
    exceptions = artifact.get("classification", {}).get("exceptions", [])
    # Join exceptions into a semicolon-separated string if any
    exceptions_str = ";".join(exceptions) if exceptions else ""
    
    # Create a record for this artifact
    row = {
         "image_tag": image_tag,
         "language": language,
         "failed_commit": failed_commit_ts,
         "passed_commit": passed_commit_ts,
         "time_to_fix_hours": time_to_fix_hours,
         "exceptions": exceptions_str
    }
    data_rows.append(row)

# Write the flattened data to a CSV file
with open(output_csv, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in data_rows:
         writer.writerow(row)

print(f"Processed {len(data_rows)} artifacts and saved the flattened data to '{output_csv}'.")
