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
GEN="rooms_bookings.py"
ENV_FILE=".env"
SCHEMA_SQL="DATABASE.sql"

if [[ ! -f "$IMP" ]]; then
  echo "ERROR: importer not found: $IMP" >&2
  exit 1
fi

if [[ ! -f "$GEN" ]]; then
  echo "ERROR: generator not found: $GEN" >&2
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

read -r -p "Drop and rebuild the complete database schema from ${SCHEMA_SQL} before importing? [y/N] " rebuild_reply
if [[ "$rebuild_reply" =~ ^[Yy]([Ee][Ss])?$ ]]; then
  rebuild_database
fi

echo "=== Generating stations, rooms, bookings and nurse station assignments ==="
"$PY" "$GEN"

run person persons_transformed.csv
run department departments.csv
run station stations.csv
run rooms rooms.csv
run dose dose.csv
run drugs drugs.csv
run employee employees.csv
run doctors doctors.csv
run nurses nurses.csv
run medication medication.csv
run patient patients.csv
run diagnosis diagnosis.csv
run bookings bookings.csv
sync_sequences

echo ""
echo "All imports completed."
