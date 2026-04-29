#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
TOOLS_DIR = SCRIPT_DIR.parent
DEFAULT_ENV_FILE = TOOLS_DIR / ".env"
DEFAULT_SCHEMA = "public"


def parse_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def load_postgres_driver():
    try:
        import psycopg  # type: ignore

        return psycopg
    except ImportError:
        try:
            import psycopg2  # type: ignore

            return psycopg2
        except ImportError as exc:
            raise RuntimeError(
                "No PostgreSQL driver found. Install `psycopg[binary]` or `psycopg2-binary`."
            ) from exc


def build_dsn_from_env() -> str | None:
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    sslmode = os.getenv("PGSSLMODE")

    if not host or not dbname or not user:
        return None

    parts = [
        f"host={host}",
        f"port={port}",
        f"dbname={dbname}",
        f"user={user}",
    ]
    if password:
        parts.append(f"password={password}")
    if sslmode:
        parts.append(f"sslmode={sslmode}")
    return " ".join(parts)


def connect_db(env_file: Path):
    parse_dotenv(env_file)
    dsn = build_dsn_from_env()
    if not dsn:
        raise RuntimeError(
            f"Could not build DB connection. Set DATABASE_URL in {env_file} "
            "or define PGHOST/PGDATABASE/PGUSER (and optional PGPASSWORD/PGPORT)."
        )
    driver = load_postgres_driver()
    return driver.connect(dsn)


def truncate_text(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def render_ascii_table(headers: list[str], rows: list[list[str]], max_col_width: int = 42) -> str:
    if not headers:
        return "(no data)"
    widths = []
    for index, header in enumerate(headers):
        row_lens = [len(row[index]) for row in rows] if rows else []
        widths.append(min(max([len(header), *row_lens], default=len(header)), max_col_width))

    def format_row(cells: list[str]) -> str:
        padded = []
        for i, cell in enumerate(cells):
            padded.append(" " + truncate_text(cell, widths[i]).ljust(widths[i]) + " ")
        return "|" + "|".join(padded) + "|"

    sep = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    out = [sep, format_row(headers), sep]
    out.extend(format_row(row) for row in rows)
    out.append(sep)
    return "\n".join(out)


def fetch_patient(conn, schema: str, patient_id: int | None, person_id: str | None):
    if patient_id is not None:
        sql = f"""
            SELECT p.id, p.person, per.first_name, per.last_name, per.gender, per.birthday,
                   per.city, per.street, per.street_no
            FROM {schema}.patient p
            JOIN {schema}.person per ON per.id = p.person
            WHERE p.id = %s
            LIMIT 1
        """
        params = (patient_id,)
    elif person_id is not None:
        sql = f"""
            SELECT p.id, p.person, per.first_name, per.last_name, per.gender, per.birthday,
                   per.city, per.street, per.street_no
            FROM {schema}.patient p
            JOIN {schema}.person per ON per.id = p.person
            WHERE p.person::text = %s
            LIMIT 1
        """
        params = (person_id,)
    else:
        sql = f"""
            SELECT p.id, p.person, per.first_name, per.last_name, per.gender, per.birthday,
                   per.city, per.street, per.street_no
            FROM {schema}.patient p
            JOIN {schema}.person per ON per.id = p.person
            WHERE EXISTS (
                SELECT 1 FROM {schema}.diagnosis d WHERE d.diagnosed_patient = p.id
            )
            ORDER BY random()
            LIMIT 1
        """
        params = ()

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def fetch_diagnosis_rows(conn, schema: str, patient_id: int, limit: int):
    sql = f"""
        SELECT
            d.id,
            d.disease,
            d.diagnosed_at,
            COALESCE(doc_person.first_name || ' ' || doc_person.last_name, d.diagnosed_by::text, '-') AS doctor_name,
            COALESCE(dep.name, '-') AS department_name,
            m.id AS medication_id,
            COALESCE(dr.name, '-') AS drug_name,
            COALESCE(ds.amount::text, '-') AS dose_amount,
            COALESCE(ds.unit::text, '-') AS dose_unit,
            COALESCE(ds.frequency::text, '-') AS dose_frequency,
            COALESCE(ds.frequency_amount::text, '-') AS dose_frequency_amount
        FROM {schema}.diagnosis d
        LEFT JOIN {schema}.doctors doc ON doc.id = d.diagnosed_by
        LEFT JOIN {schema}.employee emp ON emp.id = doc.id
        LEFT JOIN {schema}.person doc_person ON doc_person.id = emp.person
        LEFT JOIN {schema}.department dep ON dep.id = emp.department
        LEFT JOIN {schema}.medication m ON m.id = d.medication
        LEFT JOIN {schema}.drugs dr ON dr.id = m.drug
        LEFT JOIN {schema}.dose ds ON ds.id = m.dosis
        WHERE d.diagnosed_patient = %s
        ORDER BY d.diagnosed_at DESC NULLS LAST, d.id DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (patient_id, limit))
        return cur.fetchall()


def format_frequency(freq: str, amount: str) -> str:
    if freq == "x daily":
        return f"{amount}x daily"
    if freq == "x weekly":
        return f"{amount}x weekly"
    if freq == "every_x_days":
        return f"every {amount} days"
    if freq == "every_x_hours":
        return f"every {amount} hours"
    if freq == "every_x_weeks":
        return f"every {amount} weeks"
    return f"{freq} ({amount})"


def as_text(value: Any) -> str:
    return "-" if value is None else str(value)


def print_showcase(patient_row, diagnosis_rows: list[tuple[Any, ...]]) -> None:
    (
        patient_id,
        patient_person_uuid,
        first_name,
        last_name,
        gender,
        birthday,
        city,
        street,
        street_no,
    ) = patient_row

    print("\nPatient Showcase")
    print(
        f"Patient #{patient_id}: {first_name} {last_name} "
        f"({gender}, born {as_text(birthday)})"
    )
    print(f"Person UUID: {patient_person_uuid}")
    print(f"Address: {street} {street_no}, {city}")

    if not diagnosis_rows:
        print("\nNo diagnoses found for this patient.")
        return

    first = diagnosis_rows[0]
    medication_text = f"{as_text(first[6])} ({as_text(first[7])} {as_text(first[8])})"
    freq_text = format_frequency(as_text(first[9]), as_text(first[10]))
    print("\nStory")
    print(
        f"{first_name} {last_name} was diagnosed with '{as_text(first[1])}' on {as_text(first[2])} "
        f"by Dr. {as_text(first[3])} in '{as_text(first[4])}'. "
        f"Medication: {medication_text}, schedule: {freq_text}."
    )

    headers = [
        "Diagnosis ID",
        "Date",
        "Disease",
        "Doctor",
        "Department",
        "Medication",
        "Schedule",
    ]
    rows = []
    for entry in diagnosis_rows:
        medication = f"{as_text(entry[6])} ({as_text(entry[7])} {as_text(entry[8])})"
        schedule = format_frequency(as_text(entry[9]), as_text(entry[10]))
        rows.append(
            [
                as_text(entry[0]),
                as_text(entry[2]),
                as_text(entry[1]),
                as_text(entry[3]),
                as_text(entry[4]),
                medication,
                schedule,
            ]
        )

    print("\nLinked Records")
    print(render_ascii_table(headers, rows, max_col_width=46))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Show one patient demo case with linked diagnosis, doctor, department, "
            "medication and dose data."
        )
    )
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE), help="Path to .env file.")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Target PostgreSQL schema.")
    parser.add_argument("--patient-id", type=int, help="Explicit patient.id to showcase.")
    parser.add_argument("--person-id", help="Explicit patient.person UUID to resolve and showcase.")
    parser.add_argument("--max-diagnoses", type=int, default=6, help="Max diagnosis rows to show.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.patient_id is not None and args.person_id:
        raise RuntimeError("Use either --patient-id or --person-id, not both.")

    env_file = Path(args.env_file).resolve()
    with connect_db(env_file) as conn:
        patient = fetch_patient(
            conn=conn,
            schema=args.schema,
            patient_id=args.patient_id,
            person_id=args.person_id,
        )
        if not patient:
            raise RuntimeError("No matching patient found.")

        diagnosis_rows = fetch_diagnosis_rows(
            conn=conn,
            schema=args.schema,
            patient_id=int(patient[0]),
            limit=args.max_diagnoses,
        )
        print_showcase(patient, diagnosis_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
