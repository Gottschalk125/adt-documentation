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
GENERATOR_CACHE_VERSION="v3"

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
SELECT setval(pg_get_serial_sequence('public.person', 'id'), COALESCE((SELECT MAX(id) FROM public.person), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.patient', 'id'), COALESCE((SELECT MAX(id) FROM public.patient), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.dose', 'id'), COALESCE((SELECT MAX(id) FROM public.dose), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.department', 'id'), COALESCE((SELECT MAX(id) FROM public.department), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.drugs', 'id'), COALESCE((SELECT MAX(id) FROM public.drugs), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.station', 'id'), COALESCE((SELECT MAX(id) FROM public.station), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.bookings', 'id'), COALESCE((SELECT MAX(id) FROM public.bookings), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.rooms', 'id'), COALESCE((SELECT MAX(id) FROM public.rooms), 0) + 1, false);
SELECT setval(pg_get_serial_sequence('public.employee', 'id'), COALESCE((SELECT MAX(id) FROM public.employee), 0) + 1, false);
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

drop_performance_index_lines() {
cat <<'EOF'
\echo Dropping non-constraint performance indexes before bulk import...
DROP INDEX IF EXISTS public.idx_medication_dosis;
DROP INDEX IF EXISTS public.idx_medication_drug_started;
DROP INDEX IF EXISTS public.idx_medication_started_id;
DROP INDEX IF EXISTS public.idx_diagnosis_patient_date_id;
DROP INDEX IF EXISTS public.idx_diagnosis_doctor_date_id;
DROP INDEX IF EXISTS public.idx_diagnosis_medication;
DROP INDEX IF EXISTS public.idx_diagnosis_date_id;
DROP INDEX IF EXISTS public.idx_diagnosis_disease_date;
DROP INDEX IF EXISTS public.idx_person_last_first_id;
DROP INDEX IF EXISTS public.idx_person_city_id;
DROP INDEX IF EXISTS public.idx_employee_department_id;
DROP INDEX IF EXISTS public.idx_doctors_type_id;
DROP INDEX IF EXISTS public.idx_doctors_work_phone;
DROP INDEX IF EXISTS public.idx_nurses_station_id;
DROP INDEX IF EXISTS public.idx_station_department_id;
DROP INDEX IF EXISTS public.idx_rooms_station_id;
DROP INDEX IF EXISTS public.idx_rooms_station_number;
DROP INDEX IF EXISTS public.idx_bookings_patient_from_id;
DROP INDEX IF EXISTS public.idx_bookings_room_from_until;
DROP INDEX IF EXISTS public.idx_bookings_state_from_id;
DROP INDEX IF EXISTS public.idx_bookings_from_id;
DROP INDEX IF EXISTS public.idx_dose_unit_frequency;
DROP INDEX IF EXISTS public.idx_drugs_type_id;
DROP INDEX IF EXISTS public.idx_drugs_name;
EOF
}

drop_foreign_key_lines() {
cat <<'EOF'
\echo Dropping foreign keys before bulk import...
ALTER TABLE public.bookings DROP CONSTRAINT IF EXISTS fk_bookings_patient_patient_id;
ALTER TABLE public.bookings DROP CONSTRAINT IF EXISTS fk_bookings_room_rooms_id;
ALTER TABLE public.diagnosis DROP CONSTRAINT IF EXISTS fk_diagnosis_diagnosed_by_doctors_id;
ALTER TABLE public.diagnosis DROP CONSTRAINT IF EXISTS fk_diagnosis_medication_medication_id;
ALTER TABLE public.diagnosis DROP CONSTRAINT IF EXISTS fk_diagnosis_diagnosed_patient_patient_id;
ALTER TABLE public.medication DROP CONSTRAINT IF EXISTS fk_medication_dosis_dose_id;
ALTER TABLE public.medication DROP CONSTRAINT IF EXISTS fk_medication_drug_drugs_id;
ALTER TABLE public.patient DROP CONSTRAINT IF EXISTS fk_patient_person_person_id;
ALTER TABLE public.employee DROP CONSTRAINT IF EXISTS fk_employee_department_department_id;
ALTER TABLE public.employee DROP CONSTRAINT IF EXISTS fk_employee_person_person_id;
ALTER TABLE public.doctors DROP CONSTRAINT IF EXISTS fk_doctors_id_employee_id;
ALTER TABLE public.nurses DROP CONSTRAINT IF EXISTS fk_nurses_id_employee_id;
ALTER TABLE public.nurses DROP CONSTRAINT IF EXISTS fk_nurses_station_station_id;
ALTER TABLE public.rooms DROP CONSTRAINT IF EXISTS fk_rooms_station_station_id;
ALTER TABLE public.station DROP CONSTRAINT IF EXISTS fk_station_department_department_id;
EOF
}

copy_table_lines() {
  local root_dir="$1"
cat <<EOF
\echo Importing person...
\\copy public.person (gender, first_name, last_name, plz, city, street, street_no, country, birthday, phone, email) FROM '${root_dir}/persons_transformed.csv' WITH (FORMAT csv, HEADER true)
\echo Importing department...
\\copy public.department (id, name, building) FROM '${root_dir}/departments.csv' WITH (FORMAT csv, HEADER true)
\echo Importing station...
\\copy public.station (id, name, department, rooms) FROM '${root_dir}/stations.csv' WITH (FORMAT csv, HEADER true)
\echo Importing rooms...
\\copy public.rooms (id, station, number, floor, beds) FROM '${root_dir}/rooms.csv' WITH (FORMAT csv, HEADER true)
\echo Importing dose...
\\copy public.dose (id, unit, amount, frequency, frequency_amount) FROM '${root_dir}/dose.csv' WITH (FORMAT csv, HEADER true)
\echo Importing drugs...
\\copy public.drugs (id, stock, name, active_ingredient, type) FROM '${root_dir}/drugs.csv' WITH (FORMAT csv, HEADER true)
\echo Importing employee...
\\copy public.employee (id, department, person) FROM '${root_dir}/employees.csv' WITH (FORMAT csv, HEADER true)
\echo Importing doctors...
\\copy public.doctors (id, work_phone, type) FROM '${root_dir}/doctors.csv' WITH (FORMAT csv, HEADER true)
\echo Importing nurses...
\\copy public.nurses (id, station) FROM '${root_dir}/nurses.csv' WITH (FORMAT csv, HEADER true)
\echo Importing medication...
\\copy public.medication (id, dosis, drug, started, ended) FROM '${root_dir}/medication.csv' WITH (FORMAT csv, HEADER true)
\echo Importing patient...
\\copy public.patient (id, person) FROM '${root_dir}/patients.csv' WITH (FORMAT csv, HEADER true)
\echo Importing diagnosis...
\\copy public.diagnosis (id, medication, disease, diagnosed_by, diagnosed_patient, diagnosed_at) FROM '${root_dir}/diagnosis.csv' WITH (FORMAT csv, HEADER true)
\echo Importing bookings...
\\copy public.bookings (id, "from", until, state, room, patient) FROM '${root_dir}/bookings.csv' WITH (FORMAT csv, HEADER true)
EOF
}

create_foreign_key_lines() {
cat <<'EOF'
\echo Recreating foreign keys after bulk import...
ALTER TABLE public.bookings
    ADD CONSTRAINT fk_bookings_patient_patient_id
    FOREIGN KEY (patient) REFERENCES public.patient (id) NOT VALID;

ALTER TABLE public.bookings
    ADD CONSTRAINT fk_bookings_room_rooms_id
    FOREIGN KEY (room) REFERENCES public.rooms (id) NOT VALID;

ALTER TABLE public.diagnosis
    ADD CONSTRAINT fk_diagnosis_diagnosed_by_doctors_id
    FOREIGN KEY (diagnosed_by) REFERENCES public.doctors (id) NOT VALID;

ALTER TABLE public.diagnosis
    ADD CONSTRAINT fk_diagnosis_medication_medication_id
    FOREIGN KEY (medication) REFERENCES public.medication (id) NOT VALID;

ALTER TABLE public.medication
    ADD CONSTRAINT fk_medication_dosis_dose_id
    FOREIGN KEY (dosis) REFERENCES public.dose (id) NOT VALID;

ALTER TABLE public.medication
    ADD CONSTRAINT fk_medication_drug_drugs_id
    FOREIGN KEY (drug) REFERENCES public.drugs (id) NOT VALID;

ALTER TABLE public.patient
    ADD CONSTRAINT fk_patient_person_person_id
    FOREIGN KEY (person) REFERENCES public.person (id) NOT VALID;

ALTER TABLE public.diagnosis
    ADD CONSTRAINT fk_diagnosis_diagnosed_patient_patient_id
    FOREIGN KEY (diagnosed_patient) REFERENCES public.patient (id) NOT VALID;

ALTER TABLE public.employee
    ADD CONSTRAINT fk_employee_department_department_id
    FOREIGN KEY (department) REFERENCES public.department (id) NOT VALID;

ALTER TABLE public.employee
    ADD CONSTRAINT fk_employee_person_person_id
    FOREIGN KEY (person) REFERENCES public.person (id) NOT VALID;

ALTER TABLE public.doctors
    ADD CONSTRAINT fk_doctors_id_employee_id
    FOREIGN KEY (id) REFERENCES public.employee (id) NOT VALID;

ALTER TABLE public.nurses
    ADD CONSTRAINT fk_nurses_id_employee_id
    FOREIGN KEY (id) REFERENCES public.employee (id) NOT VALID;

ALTER TABLE public.nurses
    ADD CONSTRAINT fk_nurses_station_station_id
    FOREIGN KEY (station) REFERENCES public.station (id) NOT VALID;

ALTER TABLE public.rooms
    ADD CONSTRAINT fk_rooms_station_station_id
    FOREIGN KEY (station) REFERENCES public.station (id) NOT VALID;

ALTER TABLE public.station
    ADD CONSTRAINT fk_station_department_department_id
    FOREIGN KEY (department) REFERENCES public.department (id) NOT VALID;
EOF
}

validate_foreign_key_lines() {
cat <<'EOF'
\echo Validating foreign keys...
ALTER TABLE public.bookings VALIDATE CONSTRAINT fk_bookings_patient_patient_id;
ALTER TABLE public.bookings VALIDATE CONSTRAINT fk_bookings_room_rooms_id;
ALTER TABLE public.diagnosis VALIDATE CONSTRAINT fk_diagnosis_diagnosed_by_doctors_id;
ALTER TABLE public.diagnosis VALIDATE CONSTRAINT fk_diagnosis_medication_medication_id;
ALTER TABLE public.diagnosis VALIDATE CONSTRAINT fk_diagnosis_diagnosed_patient_patient_id;
ALTER TABLE public.medication VALIDATE CONSTRAINT fk_medication_dosis_dose_id;
ALTER TABLE public.medication VALIDATE CONSTRAINT fk_medication_drug_drugs_id;
ALTER TABLE public.patient VALIDATE CONSTRAINT fk_patient_person_person_id;
ALTER TABLE public.employee VALIDATE CONSTRAINT fk_employee_department_department_id;
ALTER TABLE public.employee VALIDATE CONSTRAINT fk_employee_person_person_id;
ALTER TABLE public.doctors VALIDATE CONSTRAINT fk_doctors_id_employee_id;
ALTER TABLE public.nurses VALIDATE CONSTRAINT fk_nurses_id_employee_id;
ALTER TABLE public.nurses VALIDATE CONSTRAINT fk_nurses_station_station_id;
ALTER TABLE public.rooms VALIDATE CONSTRAINT fk_rooms_station_station_id;
ALTER TABLE public.station VALIDATE CONSTRAINT fk_station_department_department_id;
EOF
}

create_performance_index_lines() {
cat <<'EOF'
\echo Recreating performance indexes after bulk import...
CREATE INDEX "idx_medication_dosis"
    ON "public"."medication" ("dosis");

CREATE INDEX "idx_medication_drug_started"
    ON "public"."medication" ("drug", "started", "id");

CREATE INDEX "idx_medication_started_id"
    ON "public"."medication" ("started", "id");

CREATE INDEX "idx_diagnosis_patient_date_id"
    ON "public"."diagnosis" ("diagnosed_patient", "diagnosed_at" DESC, "id" DESC);

CREATE INDEX "idx_diagnosis_doctor_date_id"
    ON "public"."diagnosis" ("diagnosed_by", "diagnosed_at" DESC, "id" DESC);

CREATE INDEX "idx_diagnosis_medication"
    ON "public"."diagnosis" ("medication");

CREATE INDEX "idx_diagnosis_date_id"
    ON "public"."diagnosis" ("diagnosed_at", "id");

CREATE INDEX "idx_diagnosis_disease_date"
    ON "public"."diagnosis" ("disease", "diagnosed_at");

CREATE INDEX "idx_person_last_first_id"
    ON "public"."person" ("last_name", "first_name", "id");

CREATE INDEX "idx_person_city_id"
    ON "public"."person" ("city", "id");

CREATE INDEX "idx_employee_department_id"
    ON "public"."employee" ("department", "id");

CREATE INDEX "idx_doctors_type_id"
    ON "public"."doctors" ("type", "id");

CREATE UNIQUE INDEX "idx_doctors_work_phone"
    ON "public"."doctors" ("work_phone");

CREATE INDEX "idx_nurses_station_id"
    ON "public"."nurses" ("station", "id");

CREATE INDEX "idx_station_department_id"
    ON "public"."station" ("department", "id");

CREATE INDEX "idx_rooms_station_id"
    ON "public"."rooms" ("station", "id");

CREATE UNIQUE INDEX "idx_rooms_station_number"
    ON "public"."rooms" ("station", "number");

CREATE INDEX "idx_bookings_patient_from_id"
    ON "public"."bookings" ("patient", "from" DESC, "id" DESC);

CREATE INDEX "idx_bookings_room_from_until"
    ON "public"."bookings" ("room", "from", "until");

CREATE INDEX "idx_bookings_state_from_id"
    ON "public"."bookings" ("state", "from", "id");

CREATE INDEX "idx_bookings_from_id"
    ON "public"."bookings" ("from", "id");

CREATE INDEX "idx_dose_unit_frequency"
    ON "public"."dose" ("unit", "frequency");

CREATE INDEX "idx_drugs_type_id"
    ON "public"."drugs" ("type", "id");

CREATE INDEX "idx_drugs_name"
    ON "public"."drugs" ("name");
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
EOF

  drop_performance_index_lines >>"$sql_file"
  drop_foreign_key_lines >>"$sql_file"

  cat >>"$sql_file" <<EOF
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
  create_foreign_key_lines >>"$sql_file"
  if [[ "${VALIDATE_IMPORTED_FKS:-0}" == "1" ]]; then
    validate_foreign_key_lines >>"$sql_file"
  fi
  create_performance_index_lines >>"$sql_file"

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
