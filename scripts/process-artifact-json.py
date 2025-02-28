#!/usr/bin/env python3
import os
import glob
import csv
import json
import logging
import re

# --------------------------------------------------
# Robust JSON parsing functions (from your module)
# --------------------------------------------------
class JSONParseError(Exception):
    def __init__(self, original_error, cleaned_error, text):
        self.original_error = original_error
        self.cleaned_error = cleaned_error
        self.text = text
        super().__init__(f"{original_error} | {cleaned_error}")

def fix_invalid_escapes(text):
    """
    Replace backslashes that are not part of a valid escape sequence with double backslashes.
    Valid escapes in JSON are: " \ / b f n r t and uXXXX.
    """
    return re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', text)

def escape_control_chars_in_strings(text):
    """
    Find JSON string literals in the text (including those spanning multiple lines)
    and replace literal newline, carriage return, and tab characters with their escape sequences.
    """
    def replace_control(match):
        s = match.group(0)
        inner = s[1:-1]
        inner = inner.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        return f'"{inner}"'
    return re.sub(r'"(?:\\.|[^"\\])*"', replace_control, text, flags=re.DOTALL)

def fix_multiline_strings(text):
    """
    Detect and fix multi-line string literals for keys like "run" that span multiple lines without proper termination.
    Joins the lines and replaces literal newlines with '\\n' so the JSON string remains intact.
    """
    lines = text.splitlines()
    fixed_lines = []
    in_multiline = False
    multiline_accum = ""
    for line in lines:
        if not in_multiline:
            # Look for a pattern like '"run": "'
            m = re.search(r'("run":\s*")([^"]*)$', line)
            if m and not line.rstrip().endswith('"'):
                in_multiline = True
                multiline_accum = m.group(2)
                prefix = line[:m.start(2)]
                fixed_lines.append(prefix)  # Save the prefix; will update later.
            else:
                fixed_lines.append(line)
        else:
            # Accumulate until we hit a closing quote
            if '"' in line:
                idx = line.find('"')
                multiline_accum += "\n" + line[:idx]
                fixed_string = multiline_accum.replace("\n", "\\n")
                # Replace the last saved line with the fixed string appended to the prefix.
                fixed_lines[-1] = re.sub(r'("run":\s*")[^"]*$', r'\1' + fixed_string, fixed_lines[-1])
                fixed_lines.append(line[idx:])
                in_multiline = False
                multiline_accum = ""
            else:
                multiline_accum += "\n" + line
    return "\n".join(fixed_lines)

def clean_json_text(text):
    """
    Apply cleaning functions to fix common JSON formatting issues.
    """
    text = fix_multiline_strings(text)
    text = fix_invalid_escapes(text)
    text = escape_control_chars_in_strings(text)
    return text

def robust_json_loads(text):
    """
    Try loading JSON. If it fails, clean the text and try again.
    Raises a JSONParseError if both attempts fail.
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        original_error = e
        cleaned_text = clean_json_text(text)
        try:
            return json.loads(cleaned_text)
        except json.JSONDecodeError as e2:
            raise JSONParseError(original_error, e2, text)

# --------------------------------------------------
# End of robust JSON functions
# --------------------------------------------------

# Configure logging
logging.basicConfig(
    filename='artifact_processing.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

def serialize_field(value):
    """
    If the value is a dictionary or list, convert it to a JSON string.
    Otherwise, return the value as-is (or as an empty string if None).
    """
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value)
        except Exception as e:
            logging.warning(f"Failed to serialize value {value}: {e}")
            return str(value)
    return value

# Directory where artifact JSON files are stored
input_dir = "/Users/harshil/Developer/GitHub_Repos/FailFix/Data/metadata"  # Update as needed
# Output CSV file path
output_csv = "/Users/harshil/Developer/GitHub_Repos/FailFix/Data/processed/artifact_data_table_2.csv"  # Update as needed

# Ensure the output directory exists
os.makedirs(os.path.dirname(output_csv), exist_ok=True)

# Complete list of fields from the artifact schema
fieldnames = [
    '_created', '_deleted', '_etag', '_id', '_links', '_updated', 'added_version',
    'base_branch', 'branch', 'build_system', 'cached', 'ci_service', 'current_image_tag',
    'deprecated_version', 'failed_job', 'filtered_reason', 'image_tag', 'is_error_pass',
    'lang', 'match', 'merged_at', 'metrics', 'passed_job', 'pr_num', 'repo',
    'repo_mined_version', 'reproduce_attempts', 'reproduce_successes', 'reproduced',
    'reproducibility_status', 'stability', 'status', 'test_framework'
]

data_rows = []
processed_files = 0
skipped_files = 0
invalid_files = 0

# Process each JSON file in the input directory
for filename in glob.glob(os.path.join(input_dir, '*.json')):
    # Skip empty files
    if os.path.getsize(filename) == 0:
        logging.warning(f"File {filename} is empty. Skipping.")
        skipped_files += 1
        continue

    try:
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            text = f.read()
        artifact = robust_json_loads(text)
    except JSONParseError as e:
        logging.error(f"Error parsing JSON in {filename}: {e}")
        invalid_files += 1
        continue
    except Exception as ex:
        logging.error(f"Unexpected error processing {filename}: {ex}")
        invalid_files += 1
        continue

    # Build the CSV row using the complete list of fields.
    # For 'current_image_tag', default to the value in 'image_tag' if not provided.
    row = {}
    for field in fieldnames:
        if field == 'current_image_tag':
            value = artifact.get('image_tag', '')
        else:
            value = artifact.get(field, '')
        row[field] = serialize_field(value)
    
    data_rows.append(row)
    processed_files += 1

# Write the flattened data to the output CSV file
with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for row in data_rows:
        writer.writerow(row)

# Print summary statistics
total_files = len(glob.glob(os.path.join(input_dir, '*.json')))
print(f"Total JSON files found: {total_files}")
print(f"Processed artifacts: {processed_files}")
print(f"Skipped empty files: {skipped_files}")
print(f"Files with invalid JSON: {invalid_files}")
print(f"Data saved to: '{output_csv}'")
