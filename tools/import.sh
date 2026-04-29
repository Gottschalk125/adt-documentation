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
ENV_FILE=".env"
SCHEMA_SQL="DATABASE.sql"
GENERATOR_CACHE_FILE=".generator_row_counts"
GENERATOR_CACHE_VERSION="v2"

if [[ ! -f "$IMP" ]]; then
  echo "ERROR: importer not found: $IMP" >&2
  exit 1
fi

if [[ ! -f "$SCHEMA_SQL" ]]; then
  echo "ERROR: schema file not found: $SCHEMA_SQL" >&2
  exit 1
fi

load_env() {
  if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
  fi
}

ensure_psql() {
  if ! command -v psql >/dev/null 2>&1; then
    echo "ERROR: psql is required for schema rebuilds and sequence syncing." >&2
    exit 1
  fi
}

run_psql() {
  if [[ -n "${DATABASE_URL:-}" ]]; then
    psql "$DATABASE_URL" "$@"
  else
    psql "$@"
  fi
}

rebuild_database() {
  load_env
  ensure_psql

  echo ""
  echo "=== Rebuilding database schema from ${SCHEMA_SQL} ==="
  run_psql -v ON_ERROR_STOP=1 -f "$SCHEMA_SQL"
}

sync_sequences() {
  load_env
  ensure_psql

  echo ""
  echo "=== Syncing bigint sequences to imported IDs ==="
  run_psql -v ON_ERROR_STOP=1 <<'SQL'
SELECT setval(pg_get_serial_sequence('public.medication', 'id'), COALESCE((SELECT MAX(id) FROM public.medication), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.diagnosis', 'id'), COALESCE((SELECT MAX(id) FROM public.diagnosis), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.patient', 'id'), COALESCE((SELECT MAX(id) FROM public.patient), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.dose', 'id'), COALESCE((SELECT MAX(id) FROM public.dose), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.department', 'id'), COALESCE((SELECT MAX(id) FROM public.department), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.drugs', 'id'), COALESCE((SELECT MAX(id) FROM public.drugs), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.station', 'id'), COALESCE((SELECT MAX(id) FROM public.station), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.bookings', 'id'), COALESCE((SELECT MAX(id) FROM public.bookings), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.rooms', 'id'), COALESCE((SELECT MAX(id) FROM public.rooms), 0) + 1, false);
SQL
}

file_row_count() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "-1"
    return
  fi

  local total_lines
  total_lines="$(wc -l < "$path" | tr -d ' ')"
  if [[ "$total_lines" -le 0 ]]; then
    echo "0"
  elif [[ "$path" == *.csv ]]; then
    echo $((total_lines - 1))
  else
    echo "$total_lines"
  fi
}

cache_line_for() {
  local key="$1"
  if [[ -f "$GENERATOR_CACHE_FILE" ]]; then
    awk -F'|' -v key="${GENERATOR_CACHE_VERSION}:${key}" '$1 == key { print $0 }' "$GENERATOR_CACHE_FILE"
  fi
}

cache_value_for() {
  local key="$1"
  local field_index="$2"
  local line
  line="$(cache_line_for "$key")"
  if [[ -z "$line" ]]; then
    return
  fi
  echo "$line" | cut -d'|' -f"$field_index"
}

should_skip_generator() {
  local key="$1"
  shift

  while [[ "$#" -gt 0 ]]; do
    local csv="$1"
    local current_rows
    local cached_rows
    current_rows="$(file_row_count "$csv")"
    cached_rows="$(cache_value_for "${key}:${csv}" 3)"

    if [[ "$current_rows" -lt 0 || -z "$cached_rows" || "$current_rows" != "$cached_rows" ]]; then
      return 1
    fi
    shift
  done

  return 0
}

update_generator_cache() {
  local key="$1"
  shift
  local tmp_file
  tmp_file="$(mktemp)"

  if [[ -f "$GENERATOR_CACHE_FILE" ]]; then
    awk -F'|' -v prefix="${GENERATOR_CACHE_VERSION}:${key}:" 'index($1, prefix) != 1 { print $0 }' "$GENERATOR_CACHE_FILE" >"$tmp_file"
  fi

  while [[ "$#" -gt 0 ]]; do
    local csv="$1"
    local rows
    rows="$(file_row_count "$csv")"
    printf '%s|%s|%s\n' "${GENERATOR_CACHE_VERSION}:${key}:${csv}" "$csv" "$rows" >>"$tmp_file"
    shift
  done

  mv "$tmp_file" "$GENERATOR_CACHE_FILE"
}

run_generator() {
  local key="$1"
  local script="$2"
  shift 2
  local tracked_csvs=("$@")

  if [[ ! -f "$script" ]]; then
    echo "ERROR: generator not found: $script" >&2
    exit 1
  fi

  if should_skip_generator "$key" "${tracked_csvs[@]}"; then
    echo ""
    echo "=== Skipping ${script} (tracked CSV row counts unchanged) ==="
    return
  fi

  echo ""
  echo "=== Generating via ${script} ==="
  "$PY" "$script"
  update_generator_cache "$key" "${tracked_csvs[@]}"
}

generate_all() {
  run_generator "persons_transform" "persons_transform.py" "person_10000000.csv" "persons_transformed.csv"
  run_generator "departments" "departments.py" "departments.csv"

  if [[ -f "dis_dataset.csv" ]]; then
    run_generator "diseases" "diseases.py" "dis_dataset.csv" "diseases_unique.csv"
  else
    echo ""
    echo "=== Skipping diseases.py (missing dis_dataset.csv, using existing diseases_unique.csv) ==="
  fi

  run_generator "employe_patients" "Employe_Patients.py" "persons_transformed.csv" "patients.csv" "employees.csv" "patients_sample.csv"
  run_generator "doctors_nurses" "doctors_nurses.py" "employees.csv" "departments.csv" "doctors.csv" "nurses.csv"
  run_generator "rooms_bookings" "rooms_bookings.py" "departments.csv" "employees.csv" "nurses.csv" "patients_sample.csv" "stations.csv" "rooms.csv" "bookings.csv"
  run_generator "dose" "dose.py" "dose.csv" "dose_lookup.csv"
  run_generator "drug" "drug.py" "medicine.jsonl" "drugs.csv"
  run_generator "medication" "medication.py" "dose_lookup.csv" "drugs.csv" "medication.csv" "medication_sample.csv"
  run_generator "diagnosis" "diagnosis.py" "medication_sample.csv" "doctors.csv" "patients_sample.csv" "diseases_unique.csv" "diagnosis.csv"
}

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

batch_size_for_table() {
  local table="$1"
  case "$table" in
    person)
      echo "25000"
      ;;
    medication|diagnosis|bookings)
      echo "50000"
      ;;
    station|rooms|dose|drugs|employee|doctors|nurses|patient)
      echo "100000"
      ;;
    department)
      echo "50000"
      ;;
    *)
      echo "25000"
      ;;
  esac
}

copy_table_lines() {
  local root_dir="$1"
  cat <<EOF
\\copy public.person FROM '${root_dir}/persons_transformed.csv' WITH (FORMAT csv, HEADER true)
\\copy public.department FROM '${root_dir}/departments.csv' WITH (FORMAT csv, HEADER true)
\\copy public.station FROM '${root_dir}/stations.csv' WITH (FORMAT csv, HEADER true)
\\copy public.rooms FROM '${root_dir}/rooms.csv' WITH (FORMAT csv, HEADER true)
\\copy public.dose FROM '${root_dir}/dose.csv' WITH (FORMAT csv, HEADER true)
\\copy public.drugs FROM '${root_dir}/drugs.csv' WITH (FORMAT csv, HEADER true)
\\copy public.employee FROM '${root_dir}/employees.csv' WITH (FORMAT csv, HEADER true)
\\copy public.doctors FROM '${root_dir}/doctors.csv' WITH (FORMAT csv, HEADER true)
\\copy public.nurses FROM '${root_dir}/nurses.csv' WITH (FORMAT csv, HEADER true)
\\copy public.medication FROM '${root_dir}/medication.csv' WITH (FORMAT csv, HEADER true)
\\copy public.patient FROM '${root_dir}/patients.csv' WITH (FORMAT csv, HEADER true)
\\copy public.diagnosis FROM '${root_dir}/diagnosis.csv' WITH (FORMAT csv, HEADER true)
\\copy public.bookings FROM '${root_dir}/bookings.csv' WITH (FORMAT csv, HEADER true)
EOF
}

fast_import_all() {
  load_env
  ensure_psql

  local root_dir
  root_dir="$(pwd -P)"
  local sql_file
  sql_file="$(mktemp)"

  cat >"$sql_file" <<EOF
\set ON_ERROR_STOP on
SET synchronous_commit = off;
SET statement_timeout = 0;
TRUNCATE TABLE
  public.bookings,
  public.diagnosis,
  public.medication,
  public.nurses,
  public.doctors,
  public.employee,
  public.patient,
  public.drugs,
  public.dose,
  public.rooms,
  public.station,
  public.department,
  public.person
RESTART IDENTITY CASCADE;
EOF

  copy_table_lines "$root_dir" >>"$sql_file"

  echo ""
  echo "=== Fast importing all tables with psql \\copy ==="
  run_psql -f "$sql_file"
  rm -f "$sql_file"
}

read -r -p "Drop and rebuild the complete database schema from ${SCHEMA_SQL} before importing? [y/N] " rebuild_reply
if [[ "$rebuild_reply" =~ ^[Yy]([Ee][Ss])?$ ]]; then
  rebuild_database
fi

generate_all

check_csv persons_transformed.csv
check_csv departments.csv
check_csv stations.csv
check_csv rooms.csv
check_csv dose.csv
check_csv drugs.csv
check_csv employees.csv
check_csv doctors.csv
check_csv nurses.csv
check_csv medication.csv
check_csv patients.csv
check_csv diagnosis.csv
check_csv bookings.csv

fast_import_all
sync_sequences

echo ""
echo "All imports completed."
