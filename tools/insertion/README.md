# CSV Insertion Toolkit

This folder contains a small CLI to inspect CSV files and import them into PostgreSQL with `.env`-based connection settings.

## 1. Install dependency

Use one of these:

```bash
pip install "psycopg[binary]"
```

or

```bash
pip install psycopg2-binary
```

## 2. Configure database connection

Create `tools/.env` (or pass `--env-file`) and set either:

- `DATABASE_URL=postgresql://user:password@host:5432/dbname`
- or `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

Template: [`insertion/.env.example`](/Users/david/cursor/thws/adt-documentation/tools/insertion/.env.example)

## 3. Commands

From `tools/`:

```bash
.venv/bin/python insertion/csv_importer.py list
```

Shows CSV files, inferred table, schema match, import order hint, and current DB row counts.

```bash
.venv/bin/python insertion/csv_importer.py preview persons_transformed.csv
```

Shows sample rows and per-column coverage stats.

```bash
.venv/bin/python insertion/csv_importer.py wizard
```

Interactive flow (table-first): choose DB table, choose CSV, inspect mismatches, dry-run, then import.

```bash
.venv/bin/python insertion/csv_importer.py import persons_transformed.csv --table person --dry-run
```

Dry-run only.

```bash
.venv/bin/python insertion/csv_importer.py import persons_transformed.csv --table person --on-conflict-do-nothing
```

Runs actual insert.

`import` now runs an FK precheck by default (disable with `--no-fk-precheck`).
If FK refs are missing, import is aborted early by default to avoid very slow row-by-row failures.

```bash
.venv/bin/python insertion/csv_importer.py mismatches --details
```

Shows exactly where matches are lost:

- unmapped CSV columns
- missing table columns
- sampled value/type issues (`invalid_uuid`, `invalid_date_iso`, enum mismatches, etc.)
- missing foreign key references (sampled distinct values)
- example bad values to fix in source CSVs

```bash
.venv/bin/python insertion/csv_importer.py links list
```

Show explicit table->CSV links from `insertion/table_links.json`.

```bash
.venv/bin/python insertion/csv_importer.py links set --table person --csv persons_transformed.csv
```

Force a table->CSV link so inference and mismatch checks use your manual pairing.

```bash
.venv/bin/python insertion/csv_importer.py links remove --table patient
```

Remove a wrong manual link.

## 4. Fine-grained mapping control

Override mapping columns:

```bash
.venv/bin/python insertion/csv_importer.py import patients.csv --table patient --map person_id=person
```

Manual-only mapping:

```bash
.venv/bin/python insertion/csv_importer.py import medication.csv --table medication --manual-map-only --map id=id --map dose=dosis --map drug=drug --map started=started --map ended=ended
```

Limit rows during iteration:

```bash
.venv/bin/python insertion/csv_importer.py import diagnosis.csv --table diagnosis --limit 1000 --dry-run
```

## Notes

- Relationship model: `diagnosis` now carries patient link (`diagnosed_patient` UUID), so one patient can have many diagnoses.
- `patients_with_diagnosis.csv` is an aggregated helper/report file and should not be imported into `patient`.
- The script auto-maps common aliases (for example `person_id -> person`, `birthdate -> birthday`, `dose -> dosis`, `uuid -> id`).
- Explicit links in `insertion/table_links.json` override auto-inference.
- Import checks now include FK readiness so you see missing referenced data (for example `employee.department` requires `department` rows first).
- Import uses multi-row INSERT batches for better throughput on larger CSVs.
- `dose.frequency` values are normalized from `x_daily`/`x_weekly` to schema enum values `x daily`/`x weekly`.
- `list` and `preview` can run without DB access if needed (`list --no-db`).
