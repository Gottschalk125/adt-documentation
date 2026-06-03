#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=".env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "ERROR: psql is required." >&2
  exit 1
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "ERROR: DATABASE_URL must be set in .env or the shell." >&2
  exit 1
fi

psql "$DATABASE_URL" <<'SQL'
\pset pager off
\echo Active COPY progress:
SELECT
  pid,
  relid::regclass AS table_name,
  command,
  type,
  tuples_processed,
  pg_size_pretty(bytes_processed) AS bytes_done,
  CASE
    WHEN bytes_total > 0 THEN pg_size_pretty(bytes_total)
    ELSE NULL
  END AS bytes_total,
  CASE
    WHEN bytes_total > 0 THEN round(100.0 * bytes_processed / bytes_total, 1)
    ELSE NULL
  END AS pct_bytes,
  CASE relid::regclass::text
    WHEN 'person' THEN round(100.0 * tuples_processed / 10000000, 1)
    WHEN 'medication' THEN round(100.0 * tuples_processed / 10000000, 1)
    WHEN 'diagnosis' THEN round(100.0 * tuples_processed / 10000000, 1)
    WHEN 'dose' THEN round(100.0 * tuples_processed / 10000000, 1)
    WHEN 'bookings' THEN round(100.0 * tuples_processed / 8838070, 1)
    WHEN 'patient' THEN round(100.0 * tuples_processed / 9499421, 1)
    WHEN 'employee' THEN round(100.0 * tuples_processed / 500579, 1)
    WHEN 'rooms' THEN round(100.0 * tuples_processed / 177810, 1)
    WHEN 'doctors' THEN round(100.0 * tuples_processed / 150173, 1)
    WHEN 'nurses' THEN round(100.0 * tuples_processed / 350406, 1)
    WHEN 'department' THEN round(100.0 * tuples_processed / 27, 1)
    WHEN 'station' THEN round(100.0 * tuples_processed / 18, 1)
    WHEN 'drugs' THEN round(100.0 * tuples_processed / 699, 1)
    ELSE NULL
  END AS pct_rows_estimate
FROM pg_stat_progress_copy
ORDER BY pid;

\echo
\echo Database sessions for this database:
SELECT
  pid,
  application_name,
  client_addr,
  state,
  wait_event_type,
  wait_event,
  now() - query_start AS running_for,
  left(regexp_replace(query, '\s+', ' ', 'g'), 220) AS query
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
ORDER BY query_start;
SQL

echo
echo "Local importer psql processes:"
if command -v ps >/dev/null 2>&1; then
  ps -axo pid,ppid,state,etime,%cpu,%mem,command | awk '
    NR == 1 || ($0 ~ /psql .* -f .*tmp/ && $0 !~ /awk /) { print }
  '
else
  echo "ps not available"
fi

if command -v lsof >/dev/null 2>&1; then
  pids="$(
    ps -axo pid,command 2>/dev/null | awk '
      $0 ~ /psql .* -f .*tmp/ && $0 !~ /awk / { print $1 }
    '
  )"

  if [[ -n "$pids" ]]; then
    echo
    echo "Open CSV/network handles for importer psql:"
    for pid in $pids; do
      echo "PID $pid:"
      lsof -o -p "$pid" 2>/dev/null | awk '
        NR == 1 ||
        /persons_transformed\.csv|diagnosis\.csv|medication\.csv|dose\.csv|bookings\.csv|TCP/ { print }
      '
    done
  fi
else
  echo "lsof not available"
fi
