#!/usr/bin/env bash
set -euo pipefail

if [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
elif [[ -x "insertion/.venv/bin/python" ]]; then
  PY="insertion/.venv/bin/python"
else
  PY="python3"
fi

IMP="insertion/csv_importer.py"

if [[ ! -f "$IMP" ]]; then
  echo "ERROR: importer not found: $IMP" >&2
  exit 1
fi

check_csv() {
  local csv="$1"
  if [[ ! -f "$csv" ]]; then
    echo "ERROR: missing CSV file: $csv" >&2
    exit 1
  fi

  local line_count
  line_count="$(wc -l < "$csv" | tr -d ' ')"
  if [[ "$line_count" -lt 2 ]]; then
    echo "ERROR: CSV has no data rows: $csv (lines=$line_count)" >&2
    exit 1
  fi

  local header
  header="$(head -n 1 "$csv" | tr -d '\r')"
  if [[ -z "$header" ]]; then
    echo "ERROR: CSV header is empty: $csv" >&2
    exit 1
  fi
}

run() {
  local table="$1"
  local csv="$2"
  echo ""
  echo "=== Importing ${table} <- ${csv} ==="
  check_csv "$csv"
  "$PY" "$IMP" import "$csv" --table "$table" --batch-size 2000 --drop
}

run person persons_transformed.csv
run department departments.csv
run dose dose.csv
run drugs drugs.csv
run employee employees.csv
run doctors doctors.csv
run nurses nurses.csv
run medication medication.csv
run patient patients.csv
run diagnosis diagnosis.csv

echo ""
echo "All imports completed."
