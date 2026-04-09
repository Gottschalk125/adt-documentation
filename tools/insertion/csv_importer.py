#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

SCRIPT_DIR = Path(__file__).resolve().parent
TOOLS_DIR = SCRIPT_DIR.parent
DEFAULT_ENV_FILE = TOOLS_DIR / ".env"
DEFAULT_CSV_DIR = TOOLS_DIR
DEFAULT_LINKS_FILE = SCRIPT_DIR / "table_links.json"
DEFAULT_SCHEMA = "public"

# Import order can help to reduce foreign key errors.
IMPORT_ORDER = [
    "person",
    "department",
    "station",
    "rooms",
    "dose",
    "drugs",
    "employee",
    "doctors",
    "nurses",
    "medication",
    "patient",
    "diagnosis",
    "bookings",
]

FILE_TO_TABLE_HINTS = {
    "persons_transformed.csv": "person",
    "patients.csv": "patient",
    "employees.csv": "employee",
    "departments.csv": "department",
    "doctors.csv": "doctors",
    "nurses.csv": "nurses",
    "drugs.csv": "drugs",
    "dose.csv": "dose",
    "medication.csv": "medication",
    "diagnosis.csv": "diagnosis",
}

COLUMN_ALIASES = {
    "person_id": "person",
    "department_id": "department",
    "diagnosis_id": "diagnosis",
    "drug_id": "drug",
    "dose": "dosis",
    "birthdate": "birthday",
    "uuid": "id",
}

VALUE_TRANSFORMS = {
    ("dose", "frequency"): {
        "x_daily": "x daily",
        "x_weekly": "x weekly",
    }
}


@dataclass
class CsvInfo:
    path: Path
    headers: list[str]
    row_count: int
    inferred_table: str | None
    match_score: float


@dataclass
class CsvMismatchReport:
    path: Path
    target_table: str | None
    inferred_score: float
    mapping: list[tuple[str, str]]
    table_columns_total: int
    unmapped_csv_columns: list[str]
    missing_table_columns: list[str]
    scanned_rows: int
    value_issue_counts: dict[tuple[str, str], int]
    value_issue_examples: dict[tuple[str, str], list[str]]
    normalized_value_counts: dict[str, int]
    fk_missing_counts: dict[str, int]
    fk_missing_examples: dict[str, list[str]]


def parse_dotenv(dotenv_path: Path) -> dict[str, str]:
    loaded: dict[str, str] = {}
    if not dotenv_path.exists():
        return loaded

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
            loaded[key] = value

    return loaded


def load_table_links(links_file: Path) -> dict[str, str]:
    if not links_file.exists():
        return {}
    try:
        raw = json.loads(links_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in links file {links_file}: {exc}") from exc

    if not isinstance(raw, dict):
        raise RuntimeError(f"Links file must be a JSON object: {links_file}")

    links: dict[str, str] = {}
    for table, csv_ref in raw.items():
        if not isinstance(table, str) or not isinstance(csv_ref, str):
            continue
        table = table.strip()
        csv_ref = csv_ref.strip()
        if table and csv_ref:
            links[table] = csv_ref
    return links


def save_table_links(links_file: Path, links: dict[str, str]) -> None:
    links_file.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(dict(sorted(links.items())), indent=2, ensure_ascii=True) + "\n"
    links_file.write_text(payload, encoding="utf-8")


def build_csv_to_table_links(links: dict[str, str]) -> dict[str, str]:
    csv_to_table: dict[str, str] = {}
    for table, csv_ref in links.items():
        csv_to_table[Path(csv_ref).name.lower()] = table
    return csv_to_table


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


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fetch_table_schema(conn, schema: str = DEFAULT_SCHEMA) -> tuple[dict[str, list[str]], dict[tuple[str, str], dict[str, str]]]:
    sql = """
        SELECT table_name, column_name, ordinal_position, data_type, udt_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
    """
    table_columns: dict[str, list[str]] = defaultdict(list)
    column_meta: dict[tuple[str, str], dict[str, str]] = {}

    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        for table_name, column_name, _ord, data_type, udt_name, is_nullable in cur.fetchall():
            table_columns[table_name].append(column_name)
            column_meta[(table_name, column_name)] = {
                "data_type": data_type,
                "udt_name": udt_name,
                "is_nullable": is_nullable,
            }

    return dict(table_columns), column_meta


def fetch_enum_labels(conn, schema: str = DEFAULT_SCHEMA) -> dict[str, set[str]]:
    sql = """
        SELECT t.typname, e.enumlabel
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = %s
        ORDER BY t.typname, e.enumsortorder
    """
    labels: dict[str, set[str]] = defaultdict(set)
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        for typname, enumlabel in cur.fetchall():
            labels[typname].add(enumlabel)
    return dict(labels)


def fetch_table_counts(conn, table_names: list[str], schema: str = DEFAULT_SCHEMA) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for table in table_names:
            sql = f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)}"
            cur.execute(sql)
            counts[table] = int(cur.fetchone()[0])
    return counts


def truncate_table(conn, schema: str, table: str) -> None:
    sql = (
        f"TRUNCATE TABLE {quote_ident(schema)}.{quote_ident(table)} "
        "RESTART IDENTITY CASCADE"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def fetch_foreign_keys(
    conn,
    schema: str = DEFAULT_SCHEMA,
) -> dict[str, list[dict[str, str]]]:
    sql = """
        SELECT
            tc.table_name AS table_name,
            kcu.column_name AS column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = %s
        ORDER BY tc.table_name, kcu.column_name
    """
    by_table: dict[str, list[dict[str, str]]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        for table_name, column_name, foreign_table_name, foreign_column_name in cur.fetchall():
            by_table[table_name].append(
                {
                    "column": column_name,
                    "foreign_table": foreign_table_name,
                    "foreign_column": foreign_column_name,
                }
            )
    return dict(by_table)


def list_csv_files(csv_dir: Path) -> list[Path]:
    return sorted(path for path in csv_dir.glob("*.csv") if path.is_file())


def read_csv_headers_and_count(csv_path: Path) -> tuple[list[str], int]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            headers = next(reader)
        except StopIteration:
            return [], 0

        row_count = 0
        for _ in reader:
            row_count += 1
    return [header.strip() for header in headers], row_count


def infer_table(
    csv_path: Path,
    headers: list[str],
    table_columns: dict[str, list[str]],
    csv_to_table_links: dict[str, str] | None = None,
) -> tuple[str | None, float]:
    csv_to_table_links = csv_to_table_links or {}
    linked_table = csv_to_table_links.get(csv_path.name.lower())
    if linked_table:
        if not table_columns or linked_table in table_columns:
            return linked_table, 1.0

    if not table_columns:
        hinted = FILE_TO_TABLE_HINTS.get(csv_path.name.lower())
        return hinted, 1.0 if hinted else 0.0

    lower_name = csv_path.name.lower()
    hinted = FILE_TO_TABLE_HINTS.get(lower_name)
    if hinted and hinted in table_columns:
        return hinted, 1.0

    stem = csv_path.stem.lower()
    candidates = [
        stem,
        stem.rstrip("s"),
        stem.replace("_with_diagnosis", ""),
        stem.replace("_transformed", ""),
    ]
    for candidate in candidates:
        if candidate in table_columns:
            return candidate, 0.95

    normalized_headers = []
    for header in headers:
        header = header.strip()
        normalized_headers.append(COLUMN_ALIASES.get(header, header))

    best_table = None
    best_score = 0.0
    for table_name, columns in table_columns.items():
        if not columns:
            continue
        matches = sum(1 for col in normalized_headers if col in columns)
        score = matches / len(columns)
        if score > best_score:
            best_score = score
            best_table = table_name

    if best_score <= 0:
        return None, 0.0
    return best_table, best_score


def build_default_mapping(headers: list[str], table_columns: list[str]) -> list[tuple[str, str]]:
    if not headers or not table_columns:
        return []

    assigned_targets: set[str] = set()
    mapping: list[tuple[str, str]] = []

    for csv_col in headers:
        candidates = [csv_col]
        aliased = COLUMN_ALIASES.get(csv_col)
        if aliased:
            candidates.append(aliased)
        if csv_col.endswith("_id"):
            candidates.append(csv_col[:-3])

        target = None
        for candidate in candidates:
            if candidate in table_columns and candidate not in assigned_targets:
                target = candidate
                break
        if target:
            mapping.append((csv_col, target))
            assigned_targets.add(target)

    return mapping


def parse_mapping_overrides(raw_items: list[str]) -> list[tuple[str, str]]:
    if not raw_items:
        return []

    pairs: list[tuple[str, str]] = []
    fragments: list[str] = []
    for item in raw_items:
        fragments.extend(part.strip() for part in item.split(",") if part.strip())

    for fragment in fragments:
        if "=" not in fragment:
            raise ValueError(f"Invalid mapping fragment '{fragment}'. Use csv_col=table_col.")
        left, right = fragment.split("=", 1)
        csv_col = left.strip()
        table_col = right.strip()
        if not csv_col or not table_col:
            raise ValueError(f"Invalid mapping fragment '{fragment}'.")
        pairs.append((csv_col, table_col))
    return pairs


def apply_mapping_overrides(
    base_mapping: list[tuple[str, str]],
    overrides: list[tuple[str, str]],
    headers: list[str],
    table_columns: list[str],
) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    seen_csv: set[str] = set()
    override_map = dict(overrides)

    for csv_col, target_col in base_mapping:
        if csv_col in override_map:
            target_col = override_map[csv_col]
        if csv_col in seen_csv:
            continue
        result.append((csv_col, target_col))
        seen_csv.add(csv_col)

    for csv_col, target_col in overrides:
        if csv_col in seen_csv:
            continue
        result.append((csv_col, target_col))
        seen_csv.add(csv_col)

    unknown_csv = [csv_col for csv_col, _ in result if csv_col not in headers]
    unknown_targets = [target for _, target in result if target not in table_columns]
    if unknown_csv:
        raise ValueError(f"Unknown CSV column(s): {', '.join(unknown_csv)}")
    if unknown_targets:
        raise ValueError(f"Unknown table column(s): {', '.join(unknown_targets)}")

    duplicates = find_duplicate_targets(result)
    if duplicates:
        joined = ", ".join(duplicates)
        raise ValueError(f"Duplicate target columns in mapping: {joined}")

    return result


def find_duplicate_targets(mapping: list[tuple[str, str]]) -> list[str]:
    counts: dict[str, int] = defaultdict(int)
    for _, target in mapping:
        counts[target] += 1
    return sorted(target for target, count in counts.items() if count > 1)


def clean_scalar(value: Any) -> Any:
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == "":
            return None
        return cleaned
    return value


def detect_value_issue(
    value: Any,
    col_meta: dict[str, str] | None,
    enum_labels: dict[str, set[str]],
) -> str | None:
    if value is None:
        return None
    if not col_meta:
        return None

    data_type = col_meta.get("data_type", "")
    udt_name = col_meta.get("udt_name", "")
    value_text = str(value)

    if data_type in {"smallint", "integer", "bigint"}:
        try:
            int(value_text)
        except (TypeError, ValueError):
            return "invalid_integer"
        return None

    if data_type == "uuid":
        try:
            uuid.UUID(value_text)
        except (TypeError, ValueError):
            return "invalid_uuid"
        return None

    if data_type == "date":
        try:
            date.fromisoformat(value_text)
        except (TypeError, ValueError):
            return "invalid_date_iso"
        return None

    if data_type == "USER-DEFINED" and udt_name in enum_labels:
        if value_text not in enum_labels[udt_name]:
            return f"invalid_enum:{udt_name}"
        return None

    return None


def check_fk_missing_values(
    conn,
    schema: str,
    table: str,
    fk_map: dict[str, list[dict[str, str]]],
    fk_values_by_column: dict[str, set[str]],
) -> tuple[dict[str, int], dict[str, list[str]]]:
    missing_counts: dict[str, int] = {}
    missing_examples: dict[str, list[str]] = {}

    for fk in fk_map.get(table, []):
        column = fk["column"]
        foreign_table = fk["foreign_table"]
        foreign_column = fk["foreign_column"]
        key = f"{column}->{foreign_table}.{foreign_column}"
        raw_values = fk_values_by_column.get(column, set())
        if not raw_values:
            continue

        sql = (
            f"SELECT {quote_ident(foreign_column)}::text "
            f"FROM {quote_ident(schema)}.{quote_ident(foreign_table)} "
            f"WHERE {quote_ident(foreign_column)}::text = ANY(%s)"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (list(raw_values),))
            existing = {row[0] for row in cur.fetchall()}

        missing = sorted(raw_values - existing)
        if not missing:
            continue

        missing_counts[key] = len(missing)
        missing_examples[key] = missing[:3]

    return missing_counts, missing_examples


def analyze_csv_mismatches(
    csv_path: Path,
    target_table: str | None,
    table_columns: dict[str, list[str]],
    column_meta: dict[tuple[str, str], dict[str, str]],
    enum_labels: dict[str, set[str]],
    sample_scan_rows: int,
    conn=None,
    schema: str = DEFAULT_SCHEMA,
    fk_map: dict[str, list[dict[str, str]]] | None = None,
    csv_to_table_links: dict[str, str] | None = None,
    mapping_overrides: list[tuple[str, str]] | None = None,
    manual_map_only: bool = False,
) -> CsvMismatchReport:
    mapping_overrides = mapping_overrides or []
    headers, _row_count = read_csv_headers_and_count(csv_path)
    inferred_table, inferred_score = infer_table(
        csv_path,
        headers,
        table_columns,
        csv_to_table_links=csv_to_table_links,
    )
    resolved_table = target_table or inferred_table

    resolved_mapping: list[tuple[str, str]] = []
    table_cols: list[str] = []

    if resolved_table and resolved_table in table_columns:
        table_cols = table_columns[resolved_table]
        base_mapping = [] if manual_map_only else build_default_mapping(headers, table_cols)
        resolved_mapping = apply_mapping_overrides(base_mapping, mapping_overrides, headers, table_cols)

    mapped_csv_columns = {csv_col for csv_col, _ in resolved_mapping}
    mapped_table_columns = {target for _, target in resolved_mapping}

    unmapped_csv_columns = [col for col in headers if col not in mapped_csv_columns]
    missing_table_columns = [col for col in table_cols if col not in mapped_table_columns]

    issue_counts: dict[tuple[str, str], int] = defaultdict(int)
    issue_examples: dict[tuple[str, str], list[str]] = defaultdict(list)
    normalized_counts: dict[str, int] = defaultdict(int)
    fk_values_by_column: dict[str, set[str]] = defaultdict(set)
    fk_columns = {
        item["column"]
        for item in (fk_map or {}).get(resolved_table or "", [])
    }

    scanned_rows = 0
    if resolved_table and resolved_mapping:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                scanned_rows += 1
                for csv_col, table_col in resolved_mapping:
                    raw_value = clean_scalar(row.get(csv_col))
                    normalized = normalize_value(resolved_table, table_col, raw_value)

                    if raw_value is not None and normalized != raw_value:
                        normalized_counts[table_col] += 1

                    col_meta = column_meta.get((resolved_table, table_col))
                    issue = detect_value_issue(normalized, col_meta, enum_labels)
                    if issue is None:
                        if table_col in fk_columns and normalized is not None:
                            fk_values_by_column[table_col].add(str(normalized))
                    else:
                        key = (table_col, issue)
                        issue_counts[key] += 1
                        display_value = "<empty>" if raw_value is None else str(raw_value)
                        if display_value not in issue_examples[key] and len(issue_examples[key]) < 3:
                            issue_examples[key].append(display_value)

                if scanned_rows >= sample_scan_rows:
                    break

    fk_missing_counts: dict[str, int] = {}
    fk_missing_examples: dict[str, list[str]] = {}
    if conn is not None and resolved_table and fk_map:
        fk_missing_counts, fk_missing_examples = check_fk_missing_values(
            conn=conn,
            schema=schema,
            table=resolved_table,
            fk_map=fk_map,
            fk_values_by_column=fk_values_by_column,
        )

    return CsvMismatchReport(
        path=csv_path,
        target_table=resolved_table,
        inferred_score=inferred_score,
        mapping=resolved_mapping,
        table_columns_total=len(table_cols),
        unmapped_csv_columns=unmapped_csv_columns,
        missing_table_columns=missing_table_columns,
        scanned_rows=scanned_rows,
        value_issue_counts=dict(issue_counts),
        value_issue_examples=dict(issue_examples),
        normalized_value_counts=dict(normalized_counts),
        fk_missing_counts=fk_missing_counts,
        fk_missing_examples=fk_missing_examples,
    )


def summarize_column_list(columns: list[str], max_items: int) -> str:
    if not columns:
        return "-"
    if len(columns) <= max_items:
        return ", ".join(columns)
    shown = ", ".join(columns[:max_items])
    return f"{shown} ... (+{len(columns) - max_items})"


def format_mapping_coverage(mapped_count: int, table_total: int) -> str:
    if table_total <= 0:
        return "-"
    pct = 100 * (mapped_count / table_total)
    return f"{mapped_count}/{table_total} ({pct:.1f}%)"


def print_mismatch_summary(reports: list[CsvMismatchReport], max_columns: int) -> None:
    headers = [
        "#",
        "CSV",
        "Target Table",
        "Mapped",
        "Unmapped CSV Cols",
        "Missing Table Cols",
        "Invalid Values",
        "Missing FK Refs",
    ]
    rows: list[list[str]] = []
    for index, report in enumerate(reports, start=1):
        mapped_count = len({target for _, target in report.mapping})
        invalid_total = sum(report.value_issue_counts.values())
        fk_total = sum(report.fk_missing_counts.values())
        rows.append(
            [
                str(index),
                report.path.name,
                report.target_table or "-",
                format_mapping_coverage(mapped_count, report.table_columns_total),
                str(len(report.unmapped_csv_columns)),
                str(len(report.missing_table_columns)),
                str(invalid_total),
                str(fk_total),
            ]
        )
    print(render_ascii_table(headers, rows))

    low_match_rows = [
        [
            report.path.name,
            summarize_column_list(report.unmapped_csv_columns, max_columns),
            summarize_column_list(report.missing_table_columns, max_columns),
        ]
        for report in reports
        if report.table_columns_total > 0
        and len({target for _, target in report.mapping}) / report.table_columns_total < 0.75
    ]
    if low_match_rows:
        print("\nLow mapping coverage details (<75%):")
        print(render_ascii_table(["CSV", "Unmapped CSV Columns", "Missing Table Columns"], low_match_rows))


def print_mismatch_details(report: CsvMismatchReport, top_issues: int, max_columns: int) -> None:
    print(f"\n=== {report.path.name} ===")
    if not report.target_table:
        print("No target table inferred. Use --table <name> for this CSV.")
        return

    mapped_count = len({target for _, target in report.mapping})
    print(f"Target table: {report.target_table}")
    print(f"Mapping coverage: {format_mapping_coverage(mapped_count, report.table_columns_total)}")
    print(f"Rows scanned for validation: {report.scanned_rows}")

    if report.mapping:
        print("Resolved mapping:")
        print_mapping(report.mapping)
    else:
        print("No mapped columns found.")

    print(f"Unmapped CSV columns ({len(report.unmapped_csv_columns)}):")
    print(f"  {summarize_column_list(report.unmapped_csv_columns, max_columns)}")
    print(f"Missing table columns ({len(report.missing_table_columns)}):")
    print(f"  {summarize_column_list(report.missing_table_columns, max_columns)}")

    if report.normalized_value_counts:
        norm_rows = [[column, str(count)] for column, count in sorted(report.normalized_value_counts.items())]
        print("Auto-normalized values detected:")
        print(render_ascii_table(["Table Column", "Rows"], norm_rows))

    if report.value_issue_counts:
        issue_rows = []
        sorted_items = sorted(report.value_issue_counts.items(), key=lambda item: item[1], reverse=True)
        for (column, issue), count in sorted_items[:top_issues]:
            examples = ", ".join(report.value_issue_examples.get((column, issue), [])) or "-"
            issue_rows.append([column, issue, str(count), examples])
        print("Sampled value/type issues:")
        print(render_ascii_table(["Table Column", "Issue", "Count", "Examples"], issue_rows, max_col_width=64))
    else:
        print("Sampled value/type issues: none found.")

    if report.fk_missing_counts:
        fk_rows = []
        for key, count in sorted(report.fk_missing_counts.items(), key=lambda item: item[1], reverse=True):
            examples = ", ".join(report.fk_missing_examples.get(key, [])) or "-"
            fk_rows.append([key, str(count), examples])
        print("Missing foreign key references (sampled distinct values):")
        print(render_ascii_table(["FK", "Missing Distinct Values", "Examples"], fk_rows, max_col_width=64))
    else:
        print("Missing foreign key references: none found in sampled rows.")


def truncate_text(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def render_ascii_table(headers: list[str], rows: list[list[str]], max_col_width: int = 48) -> str:
    if not headers:
        return "(no data)"

    safe_rows = [[str(cell) if cell is not None else "" for cell in row] for row in rows]
    widths = []
    for index, header in enumerate(headers):
        cell_lengths = [len(row[index]) for row in safe_rows] if safe_rows else []
        width = max([len(header), *cell_lengths], default=len(header))
        widths.append(min(width, max_col_width))

    def render_row(cells: list[str]) -> str:
        padded = []
        for i, cell in enumerate(cells):
            truncated = truncate_text(cell, widths[i])
            padded.append(" " + truncated.ljust(widths[i]) + " ")
        return "|" + "|".join(padded) + "|"

    separator = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    lines = [separator, render_row(headers), separator]
    for row in safe_rows:
        lines.append(render_row(row))
    lines.append(separator)
    return "\n".join(lines)


def render_ratio_bar(numerator: int, denominator: int, width: int = 24) -> str:
    if denominator <= 0:
        return "-" * width
    ratio = max(0.0, min(1.0, numerator / denominator))
    filled = int(round(ratio * width))
    return "#" * filled + "-" * (width - filled)


def resolve_csv_path(raw_name: str, csv_dir: Path) -> Path:
    candidate = Path(raw_name)
    if candidate.exists():
        return candidate.resolve()

    candidate = csv_dir / raw_name
    if candidate.exists():
        return candidate.resolve()

    raise FileNotFoundError(f"CSV file not found: {raw_name}")


def read_preview_rows(csv_path: Path, sample_rows: int) -> tuple[list[str], list[list[str]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return [], []
        headers = [header.strip() for header in reader.fieldnames]

        rows: list[list[str]] = []
        for row in reader:
            rows.append([(row.get(header) or "").strip() for header in headers])
            if len(rows) >= sample_rows:
                break
    return headers, rows


def profile_columns(csv_path: Path, sample_scan_rows: int = 2000) -> tuple[list[str], int, dict[str, int]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return [], 0, {}
        headers = [header.strip() for header in reader.fieldnames]
        missing = {header: 0 for header in headers}
        scanned = 0
        for row in reader:
            scanned += 1
            for header in headers:
                if (row.get(header) or "").strip() == "":
                    missing[header] += 1
            if scanned >= sample_scan_rows:
                break
    return headers, scanned, missing


def normalize_value(table: str, column: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == "":
            return None
    else:
        cleaned = value

    transform = VALUE_TRANSFORMS.get((table, column), {})
    if isinstance(cleaned, str) and cleaned in transform:
        return transform[cleaned]
    return cleaned


def iter_mapped_rows(
    csv_path: Path,
    table: str,
    mapping: list[tuple[str, str]],
    limit: int | None = None,
) -> Iterable[tuple[int, list[Any]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        produced = 0
        for row in reader:
            produced += 1
            values: list[Any] = []
            for csv_col, table_col in mapping:
                raw_value = row.get(csv_col)
                values.append(normalize_value(table, table_col, raw_value))
            yield reader.line_num, values
            if limit is not None and produced >= limit:
                return


def make_insert_prefix(schema: str, table: str, target_columns: list[str]) -> str:
    quoted_columns = ", ".join(quote_ident(col) for col in target_columns)
    return (
        f"INSERT INTO {quote_ident(schema)}.{quote_ident(table)} "
        f"({quoted_columns}) VALUES "
    )


def make_insert_sql(
    insert_prefix: str,
    row_count: int,
    column_count: int,
    on_conflict_do_nothing: bool,
) -> str:
    row_placeholder = "(" + ", ".join(["%s"] * column_count) + ")"
    values = ", ".join([row_placeholder] * row_count)
    sql = f"{insert_prefix}{values}"
    if on_conflict_do_nothing:
        sql += " ON CONFLICT DO NOTHING"
    return sql


def flatten_rows(rows: list[list[Any]]) -> list[Any]:
    params: list[Any] = []
    for row in rows:
        params.extend(row)
    return params


def execute_import(
    conn,
    schema: str,
    table: str,
    mapping: list[tuple[str, str]],
    csv_path: Path,
    batch_size: int,
    dry_run: bool,
    limit: int | None,
    on_conflict_do_nothing: bool,
) -> tuple[int, int, list[tuple[int, str]]]:
    target_columns = [target for _, target in mapping]
    row_source = iter_mapped_rows(csv_path, table, mapping, limit=limit)

    if dry_run:
        total = 0
        for _line_no, _values in row_source:
            total += 1
        return total, 0, []

    insert_prefix = make_insert_prefix(schema, table, target_columns)
    single_row_sql = make_insert_sql(
        insert_prefix=insert_prefix,
        row_count=1,
        column_count=len(target_columns),
        on_conflict_do_nothing=on_conflict_do_nothing,
    )
    inserted = 0
    failed = 0
    errors: list[tuple[int, str]] = []

    batch: list[tuple[int, list[Any]]] = []

    def flush(current_batch: list[tuple[int, list[Any]]]) -> tuple[int, int]:
        nonlocal errors
        if not current_batch:
            return 0, 0

        success = 0
        failed_local = 0
        payload = [values for _, values in current_batch]
        batch_sql = make_insert_sql(
            insert_prefix=insert_prefix,
            row_count=len(payload),
            column_count=len(target_columns),
            on_conflict_do_nothing=on_conflict_do_nothing,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(batch_sql, flatten_rows(payload))
            conn.commit()
            return len(current_batch), 0
        except Exception as batch_exc:
            conn.rollback()
            batch_message = str(batch_exc).lower()
            if (
                not on_conflict_do_nothing
                and "duplicate key value violates unique constraint" in batch_message
            ):
                failed_quick = len(current_batch)
                if len(errors) < 25:
                    first_line = current_batch[0][0]
                    errors.append(
                        (
                            first_line,
                            "duplicate key conflict in batch; rerun with --on-conflict-do-nothing "
                            "or load into an empty table",
                        )
                    )
                return 0, failed_quick
            # Fall back to row-by-row to identify bad rows.
            with conn.cursor() as cur:
                for line_no, values in current_batch:
                    try:
                        cur.execute("SAVEPOINT csv_import_row")
                        cur.execute(single_row_sql, values)
                        cur.execute("RELEASE SAVEPOINT csv_import_row")
                        success += 1
                    except Exception as row_exc:  # pylint: disable=broad-except
                        cur.execute("ROLLBACK TO SAVEPOINT csv_import_row")
                        cur.execute("RELEASE SAVEPOINT csv_import_row")
                        failed_local += 1
                        if len(errors) < 25:
                            message = str(row_exc).splitlines()[0]
                            errors.append((line_no, message))
            conn.commit()
            return success, failed_local

    for line_no, values in row_source:
        batch.append((line_no, values))
        if len(batch) >= batch_size:
            success, failed_local = flush(batch)
            inserted += success
            failed += failed_local
            batch.clear()

    if batch:
        success, failed_local = flush(batch)
        inserted += success
        failed += failed_local

    return inserted, failed, errors


def get_import_rank(table: str | None) -> str:
    if not table:
        return "-"
    try:
        return str(IMPORT_ORDER.index(table) + 1)
    except ValueError:
        return "-"


def gather_csv_info(
    csv_dir: Path,
    table_columns: dict[str, list[str]],
    csv_to_table_links: dict[str, str] | None = None,
) -> list[CsvInfo]:
    infos: list[CsvInfo] = []
    for csv_path in list_csv_files(csv_dir):
        headers, row_count = read_csv_headers_and_count(csv_path)
        inferred_table, score = infer_table(
            csv_path,
            headers,
            table_columns,
            csv_to_table_links=csv_to_table_links,
        )
        infos.append(
            CsvInfo(
                path=csv_path,
                headers=headers,
                row_count=row_count,
                inferred_table=inferred_table,
                match_score=score,
            )
        )
    return infos


def print_list_output(infos: list[CsvInfo], table_counts: dict[str, int] | None = None) -> None:
    headers = ["#", "CSV", "Rows", "Inferred Table", "Match", "Order", "Rows In Table"]
    rows: list[list[str]] = []

    for index, info in enumerate(infos, start=1):
        inferred = info.inferred_table or "-"
        rows_in_table = "-"
        if table_counts and info.inferred_table in table_counts:
            rows_in_table = str(table_counts[info.inferred_table])
        rows.append(
            [
                str(index),
                info.path.name,
                str(info.row_count),
                inferred,
                f"{info.match_score * 100:.0f}%",
                get_import_rank(info.inferred_table),
                rows_in_table,
            ]
        )
    print(render_ascii_table(headers, rows))


def print_preview_output(csv_path: Path, sample_rows: int, sample_scan_rows: int) -> None:
    headers, rows = read_preview_rows(csv_path, sample_rows)
    if not headers:
        print(f"{csv_path.name}: empty or unreadable CSV.")
        return

    print(f"Preview for {csv_path.name} ({sample_rows} rows):")
    print(render_ascii_table(headers, rows))

    prof_headers, scanned, missing = profile_columns(csv_path, sample_scan_rows=sample_scan_rows)
    if scanned == 0:
        print("No data rows found.")
        return

    stat_headers = ["Column", "Missing", "Coverage", "Coverage Bar"]
    stat_rows: list[list[str]] = []
    for column in prof_headers:
        miss = missing[column]
        filled = scanned - miss
        coverage = 100 * (filled / scanned)
        stat_rows.append(
            [
                column,
                f"{miss}/{scanned}",
                f"{coverage:.1f}%",
                render_ratio_bar(filled, scanned),
            ]
        )
    print(f"\nColumn profile (first {scanned} rows):")
    print(render_ascii_table(stat_headers, stat_rows, max_col_width=64))


def parse_selection(raw: str, max_index: int) -> list[int]:
    text = raw.strip().lower()
    if text == "all":
        return list(range(1, max_index + 1))

    selected: set[int] = set()
    for part in [p.strip() for p in text.split(",") if p.strip()]:
        if "-" in part:
            left, right = part.split("-", 1)
            start = int(left)
            end = int(right)
            for value in range(start, end + 1):
                selected.add(value)
        else:
            selected.add(int(part))

    invalid = [value for value in selected if value < 1 or value > max_index]
    if invalid:
        raise ValueError(f"Invalid index(es): {', '.join(str(v) for v in sorted(invalid))}")

    return sorted(selected)


def sort_tables_for_display(table_names: Iterable[str]) -> list[str]:
    table_set = list(table_names)
    ordered: list[str] = []
    for table in IMPORT_ORDER:
        if table in table_set:
            ordered.append(table)
    remaining = sorted(table for table in table_set if table not in ordered)
    return ordered + remaining


def choose_one(items: list[str], title: str, prompt: str) -> str:
    if not items:
        raise RuntimeError(f"No entries available for {title}.")

    rows = [[str(idx), item] for idx, item in enumerate(items, start=1)]
    print(f"\n{title}:")
    print(render_ascii_table(["#", "Name"], rows))

    while True:
        raw = input(prompt).strip()
        if raw.isdigit():
            index = int(raw)
            if 1 <= index <= len(items):
                return items[index - 1]
        for item in items:
            if raw == item:
                return item
        print("Invalid selection. Enter index or exact name.")


def ensure_tty_or_fail() -> None:
    if not sys.stdin.isatty():
        raise RuntimeError("Wizard mode requires an interactive terminal.")


def cmd_list(args: argparse.Namespace) -> int:
    csv_dir = Path(args.csv_dir).resolve()
    env_file = Path(args.env_file).resolve()
    links_file = Path(args.links_file).resolve()
    table_columns: dict[str, list[str]] = {}
    table_counts: dict[str, int] | None = None
    table_links = load_table_links(links_file)
    csv_to_table_links = build_csv_to_table_links(table_links)

    if not args.no_db:
        try:
            with connect_db(env_file) as conn:
                table_columns, _column_meta = fetch_table_schema(conn, schema=args.schema)
                if table_columns:
                    table_counts = fetch_table_counts(conn, sorted(table_columns.keys()), schema=args.schema)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Warning: DB connection failed, continuing without schema data: {exc}", file=sys.stderr)

    infos = gather_csv_info(csv_dir, table_columns, csv_to_table_links=csv_to_table_links)
    if not infos:
        print(f"No CSV files found in {csv_dir}")
        return 0

    print_list_output(infos, table_counts=table_counts)
    if table_links:
        link_rows = [[table, csv_ref] for table, csv_ref in sorted(table_links.items())]
        print(f"\nManual links from {links_file}:")
        print(render_ascii_table(["Table", "CSV"], link_rows))
    return 0


def cmd_preview(args: argparse.Namespace) -> int:
    csv_dir = Path(args.csv_dir).resolve()
    csv_path = resolve_csv_path(args.csv, csv_dir)
    print_preview_output(csv_path, sample_rows=args.sample_rows, sample_scan_rows=args.sample_scan_rows)
    return 0


def print_mapping(mapping: list[tuple[str, str]]) -> None:
    rows = [[csv_col, table_col] for csv_col, table_col in mapping]
    print(render_ascii_table(["CSV Column", "Table Column"], rows))


def print_error_samples(errors: list[tuple[int, str]]) -> None:
    if not errors:
        return
    grouped: dict[str, int] = defaultdict(int)
    for _line_no, message in errors:
        grouped[message] += 1

    rows = [[message, str(count)] for message, count in sorted(grouped.items(), key=lambda i: i[1], reverse=True)]
    print("Error summary (sampled):")
    print(render_ascii_table(["Message", "Count"], rows, max_col_width=96))
    print("Error lines (sampled):")
    for line_no, message in errors:
        print(f"  line {line_no}: {message}")


def cmd_mismatches(args: argparse.Namespace) -> int:
    csv_dir = Path(args.csv_dir).resolve()
    env_file = Path(args.env_file).resolve()
    links_file = Path(args.links_file).resolve()
    table_links = load_table_links(links_file)
    csv_to_table_links = build_csv_to_table_links(table_links)

    if args.csv is None and (args.table or args.map or args.manual_map_only):
        raise RuntimeError("--table/--map/--manual-map-only can only be used when a specific CSV is provided.")

    with connect_db(env_file) as conn:
        table_columns, column_meta = fetch_table_schema(conn, schema=args.schema)
        enum_labels = fetch_enum_labels(conn, schema=args.schema)
        fk_map = fetch_foreign_keys(conn, schema=args.schema)

        if args.csv:
            csv_paths = [resolve_csv_path(args.csv, csv_dir)]
            mapping_overrides = parse_mapping_overrides(args.map or [])
            forced_table = args.table
            manual_map_only = args.manual_map_only
        else:
            csv_paths = list_csv_files(csv_dir)
            mapping_overrides = []
            forced_table = None
            manual_map_only = False

        reports: list[CsvMismatchReport] = []
        for csv_path in csv_paths:
            report = analyze_csv_mismatches(
                csv_path=csv_path,
                target_table=forced_table,
                table_columns=table_columns,
                column_meta=column_meta,
                enum_labels=enum_labels,
                sample_scan_rows=args.sample_scan_rows,
                conn=conn,
                schema=args.schema,
                fk_map=fk_map,
                csv_to_table_links=csv_to_table_links,
                mapping_overrides=mapping_overrides,
                manual_map_only=manual_map_only,
            )
            reports.append(report)

    if not reports:
        print(f"No CSV files found in {csv_dir}")
        return 0

    print_mismatch_summary(reports, max_columns=args.max_columns)

    if args.details or args.csv:
        for report in reports:
            print_mismatch_details(report, top_issues=args.top_issues, max_columns=args.max_columns)
    else:
        print("\nUse `mismatches --details` to print per-file issue breakdown.")

    return 0


def cmd_links(args: argparse.Namespace) -> int:
    links_file = Path(args.links_file).resolve()
    links = load_table_links(links_file)

    if args.action == "list":
        if not links:
            print(f"No table links configured in {links_file}")
            return 0
        rows = [[table, csv_ref] for table, csv_ref in sorted(links.items())]
        print(render_ascii_table(["Table", "CSV"], rows))
        print(f"Links file: {links_file}")
        return 0

    if args.action == "set":
        if not args.table or not args.csv:
            raise RuntimeError("`links set` requires --table and --csv.")
        links[args.table] = args.csv
        save_table_links(links_file, links)
        print(f"Saved link in {links_file}: {args.table} -> {args.csv}")
        return 0

    if args.action == "remove":
        if not args.table:
            raise RuntimeError("`links remove` requires --table.")
        if args.table not in links:
            print(f"No link found for table '{args.table}' in {links_file}")
            return 0
        del links[args.table]
        save_table_links(links_file, links)
        print(f"Removed link for table '{args.table}' from {links_file}")
        return 0

    raise RuntimeError(f"Unknown links action: {args.action}")


def cmd_import(args: argparse.Namespace) -> int:
    csv_dir = Path(args.csv_dir).resolve()
    env_file = Path(args.env_file).resolve()
    links_file = Path(args.links_file).resolve()
    table_links = load_table_links(links_file)
    csv_to_table_links = build_csv_to_table_links(table_links)
    csv_path = resolve_csv_path(args.csv, csv_dir)

    with connect_db(env_file) as conn:
        table_columns, _column_meta = fetch_table_schema(conn, schema=args.schema)
        column_meta = _column_meta
        enum_labels = fetch_enum_labels(conn, schema=args.schema)
        fk_map = fetch_foreign_keys(conn, schema=args.schema)
        table_counts = fetch_table_counts(conn, sorted(table_columns.keys()), schema=args.schema)

        headers, _row_count = read_csv_headers_and_count(csv_path)
        if not headers:
            raise RuntimeError(f"{csv_path.name} has no headers.")

        inferred_table, _score = infer_table(
            csv_path,
            headers,
            table_columns,
            csv_to_table_links=csv_to_table_links,
        )
        target_table = args.table or inferred_table
        if not target_table:
            raise RuntimeError(
                f"Could not infer target table for {csv_path.name}. Use --table <name>."
            )
        if target_table not in table_columns:
            known_tables = ", ".join(sorted(table_columns.keys()))
            raise RuntimeError(f"Unknown table '{target_table}'. Known tables: {known_tables}")

        base_mapping = [] if args.manual_map_only else build_default_mapping(headers, table_columns[target_table])
        overrides = parse_mapping_overrides(args.map or [])
        mapping = apply_mapping_overrides(base_mapping, overrides, headers, table_columns[target_table])

        if not mapping:
            raise RuntimeError(
                "No mapped columns. Use --map csv_col=table_col (repeatable) "
                "or remove --manual-map-only."
            )

        print(f"CSV: {csv_path.name}")
        print(f"Target table: {target_table}")
        print("Resolved column mapping:")
        print_mapping(mapping)

        if args.drop and not args.dry_run:
            print(f"Dropping existing data in {target_table} (TRUNCATE ... CASCADE).")
            truncate_table(conn, schema=args.schema, table=target_table)

        precheck_report = None
        if args.fk_precheck:
            refs = fk_map.get(target_table, [])
            if refs:
                warn_rows = []
                for fk in refs:
                    ref_table = fk["foreign_table"]
                    warn_rows.append(
                        [
                            fk["column"],
                            f"{ref_table}.{fk['foreign_column']}",
                            str(table_counts.get(ref_table, 0)),
                        ]
                    )
                print("Foreign key dependencies:")
                print(render_ascii_table(["Column", "References", "Rows In Ref Table"], warn_rows))

            precheck_scan = args.limit if args.limit is not None else args.fk_precheck_rows
            precheck_report = analyze_csv_mismatches(
                csv_path=csv_path,
                target_table=target_table,
                table_columns=table_columns,
                column_meta=column_meta,
                enum_labels=enum_labels,
                sample_scan_rows=precheck_scan,
                conn=conn,
                schema=args.schema,
                fk_map=fk_map,
                mapping_overrides=overrides,
                manual_map_only=args.manual_map_only,
            )
            if precheck_report.fk_missing_counts:
                print("FK precheck warnings:")
                for key, count in sorted(precheck_report.fk_missing_counts.items(), key=lambda i: i[1], reverse=True):
                    examples = ", ".join(precheck_report.fk_missing_examples.get(key, []))
                    print(f"  {key}: missing distinct values={count}; examples={examples}")
                if not args.dry_run:
                    raise RuntimeError(
                        "FK precheck found missing referenced values. "
                        "Import aborted to avoid slow full-row failures. "
                        "Load parent tables first or rerun with --no-fk-precheck."
                    )

        inserted, failed, errors = execute_import(
            conn=conn,
            schema=args.schema,
            table=target_table,
            mapping=mapping,
            csv_path=csv_path,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            limit=args.limit,
            on_conflict_do_nothing=args.on_conflict_do_nothing,
        )

        mode = "Dry run" if args.dry_run else "Import"
        print(f"{mode} finished. Rows processed: {inserted}")
        if failed:
            print(f"Failed rows: {failed}")
            print_error_samples(errors)
            return 2

    return 0


def cmd_wizard(args: argparse.Namespace) -> int:
    ensure_tty_or_fail()

    csv_dir = Path(args.csv_dir).resolve()
    env_file = Path(args.env_file).resolve()
    links_file = Path(args.links_file).resolve()
    table_links = load_table_links(links_file)
    csv_files = list_csv_files(csv_dir)
    if not csv_files:
        print(f"No CSV files found in {csv_dir}")
        return 0

    csv_names = [path.name for path in csv_files]
    csv_lookup = {path.name: path for path in csv_files}
    links_changed = False

    with connect_db(env_file) as conn:
        table_columns, column_meta = fetch_table_schema(conn, schema=args.schema)
        enum_labels = fetch_enum_labels(conn, schema=args.schema)
        fk_map = fetch_foreign_keys(conn, schema=args.schema)
        table_counts = fetch_table_counts(conn, sorted(table_columns.keys()), schema=args.schema)
        table_names = sort_tables_for_display(table_columns.keys())

        imported_total = 0
        failed_total = 0

        while True:
            target_table = choose_one(
                table_names,
                title="Database Tables",
                prompt="Select target table (index or name): ",
            )

            print("\nAvailable CSV files:")
            print(render_ascii_table(["#", "CSV"], [[str(i), name] for i, name in enumerate(csv_names, start=1)]))
            linked_csv = table_links.get(target_table)

            while True:
                suffix = f" [{linked_csv}]" if linked_csv else ""
                raw_csv = input(f"Select CSV (index or name){suffix}: ").strip()
                if raw_csv == "" and linked_csv and linked_csv in csv_lookup:
                    selected_csv = linked_csv
                    break
                if raw_csv.isdigit():
                    idx = int(raw_csv)
                    if 1 <= idx <= len(csv_names):
                        selected_csv = csv_names[idx - 1]
                        break
                if raw_csv in csv_lookup:
                    selected_csv = raw_csv
                    break
                print("Invalid CSV selection. Enter index or exact file name.")

            csv_path = csv_lookup[selected_csv]
            print(f"\nSelected pair: table={target_table}, csv={selected_csv}")

            refs = fk_map.get(target_table, [])
            if refs:
                fk_warn_rows: list[list[str]] = []
                for fk in refs:
                    ref_table = fk["foreign_table"]
                    count = table_counts.get(ref_table, 0)
                    fk_warn_rows.append(
                        [
                            fk["column"],
                            f"{ref_table}.{fk['foreign_column']}",
                            str(count),
                            "yes" if count > 0 else "NO",
                        ]
                    )
                print("Foreign key dependencies:")
                print(
                    render_ascii_table(
                        ["Column", "References", "Rows In Ref Table", "Ready"],
                        fk_warn_rows,
                    )
                )

            save_link_default = "y" if linked_csv != selected_csv else "n"
            raw_save = input(
                f"Save this table->CSV link to {links_file}? [y/N] (default {save_link_default}): "
            ).strip().lower()
            save_link = raw_save in {"y", "yes"} or (raw_save == "" and save_link_default == "y")
            if save_link:
                table_links[target_table] = selected_csv
                save_table_links(links_file, table_links)
                links_changed = True
                print(f"Saved link: {target_table} -> {selected_csv}")

            print_preview_output(csv_path, sample_rows=args.sample_rows, sample_scan_rows=args.sample_scan_rows)

            headers, _row_count = read_csv_headers_and_count(csv_path)
            base_mapping = build_default_mapping(headers, table_columns[target_table])
            if base_mapping:
                print("Suggested mapping:")
                print_mapping(base_mapping)
            else:
                print("No automatic mapping found.")

            raw_map = input(
                "Optional mapping overrides (csv_col=table_col,comma-separated) [Enter to keep]: "
            ).strip()
            overrides = parse_mapping_overrides([raw_map]) if raw_map else []
            try:
                mapping = apply_mapping_overrides(base_mapping, overrides, headers, table_columns[target_table])
            except ValueError as exc:
                print(f"Mapping error: {exc}")
                retry = input("Choose a new pair? [Y/n]: ").strip().lower()
                if retry in {"n", "no"}:
                    break
                continue

            report = analyze_csv_mismatches(
                csv_path=csv_path,
                target_table=target_table,
                table_columns=table_columns,
                column_meta=column_meta,
                enum_labels=enum_labels,
                sample_scan_rows=args.sample_scan_rows,
                conn=conn,
                schema=args.schema,
                fk_map=fk_map,
                mapping_overrides=overrides,
            )
            print_mismatch_details(report, top_issues=args.top_issues, max_columns=args.max_columns)

            if not mapping:
                print("No mapped columns. Skipping import.")
            else:
                if report.fk_missing_counts:
                    proceed = input(
                        "FK precheck found missing references; import will likely fail and be slow. Continue anyway? [y/N]: "
                    ).strip().lower()
                    if proceed not in {"y", "yes"}:
                        print("Import skipped due to FK precheck warnings.")
                        again = input("Configure another table/CSV pair? [y/N]: ").strip().lower()
                        if again not in {"y", "yes"}:
                            break
                        continue

                raw_limit = input("Row limit [all]: ").strip()
                limit = int(raw_limit) if raw_limit else None

                dry_run_inserted, dry_run_failed, dry_run_errors = execute_import(
                    conn=conn,
                    schema=args.schema,
                    table=target_table,
                    mapping=mapping,
                    csv_path=csv_path,
                    batch_size=args.batch_size,
                    dry_run=True,
                    limit=limit,
                    on_conflict_do_nothing=args.on_conflict_do_nothing,
                )
                print(f"Dry run rows: {dry_run_inserted}")
                if dry_run_failed:
                    print(f"Dry run failures: {dry_run_failed}")
                    print_error_samples(dry_run_errors)

                confirm = input("Execute import now? [y/N]: ").strip().lower()
                if confirm in {"y", "yes"}:
                    inserted, failed, errors = execute_import(
                        conn=conn,
                        schema=args.schema,
                        table=target_table,
                        mapping=mapping,
                        csv_path=csv_path,
                        batch_size=args.batch_size,
                        dry_run=False,
                        limit=limit,
                        on_conflict_do_nothing=args.on_conflict_do_nothing,
                    )
                    imported_total += inserted
                    failed_total += failed
                    table_counts[target_table] = table_counts.get(target_table, 0) + inserted
                    print(f"Imported rows: {inserted}")
                    if failed:
                        print(f"Failed rows: {failed}")
                        print_error_samples(errors)
                else:
                    print("Import skipped.")

            again = input("Configure another table/CSV pair? [y/N]: ").strip().lower()
            if again not in {"y", "yes"}:
                break

        print("\nWizard summary:")
        print(f"  imported rows: {imported_total}")
        print(f"  failed rows:   {failed_total}")
        if links_changed:
            print(f"  links updated: {links_file}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect, preview and import CSV files into PostgreSQL. "
            "Connection is read from .env (DATABASE_URL or PG* variables)."
        )
    )
    parser.add_argument("--csv-dir", default=str(DEFAULT_CSV_DIR), help="Directory that contains CSV files.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE), help="Path to .env file.")
    parser.add_argument(
        "--links-file",
        default=str(DEFAULT_LINKS_FILE),
        help="Path to JSON file with explicit table->CSV links.",
    )
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Target PostgreSQL schema.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List CSV files with inferred target tables.")
    list_parser.add_argument("--no-db", action="store_true", help="Skip DB schema introspection.")
    list_parser.set_defaults(func=cmd_list)

    links_parser = subparsers.add_parser("links", help="Manage explicit table->CSV links.")
    links_parser.add_argument("action", choices=["list", "set", "remove"], help="Link action.")
    links_parser.add_argument("--table", help="Database table name.")
    links_parser.add_argument("--csv", help="CSV file name/path to link.")
    links_parser.set_defaults(func=cmd_links)

    preview_parser = subparsers.add_parser("preview", help="Show sample rows and column profile of one CSV.")
    preview_parser.add_argument("csv", help="CSV file name or path.")
    preview_parser.add_argument("--sample-rows", type=int, default=8, help="How many rows to print.")
    preview_parser.add_argument(
        "--sample-scan-rows",
        type=int,
        default=2000,
        help="How many rows to scan for column coverage stats.",
    )
    preview_parser.set_defaults(func=cmd_preview)

    mismatches_parser = subparsers.add_parser(
        "mismatches",
        help="Show mapping gaps and sampled value/type mismatches.",
    )
    mismatches_parser.add_argument(
        "csv",
        nargs="?",
        help="CSV file name or path. If omitted, analyze all CSV files in --csv-dir.",
    )
    mismatches_parser.add_argument("--table", help="Force target table (single-CSV mode only).")
    mismatches_parser.add_argument(
        "--map",
        action="append",
        help="Override mapping: csv_col=table_col. Repeat or comma-separate (single-CSV mode only).",
    )
    mismatches_parser.add_argument(
        "--manual-map-only",
        action="store_true",
        help="Disable auto-mapping and only use --map pairs (single-CSV mode only).",
    )
    mismatches_parser.add_argument(
        "--sample-scan-rows",
        type=int,
        default=2000,
        help="Rows to scan for value/type mismatch checks per CSV.",
    )
    mismatches_parser.add_argument(
        "--top-issues",
        type=int,
        default=12,
        help="How many issue groups to show in detailed output.",
    )
    mismatches_parser.add_argument(
        "--max-columns",
        type=int,
        default=10,
        help="Maximum number of columns to print in compact lists before truncating.",
    )
    mismatches_parser.add_argument(
        "--details",
        action="store_true",
        help="Print detailed mismatch breakdown for each analyzed CSV.",
    )
    mismatches_parser.set_defaults(func=cmd_mismatches)

    import_parser = subparsers.add_parser("import", help="Import one CSV into a target table.")
    import_parser.add_argument("csv", help="CSV file name or path.")
    import_parser.add_argument("--table", help="Target table. If omitted, inferred from CSV.")
    import_parser.add_argument(
        "--map",
        action="append",
        help="Override mapping: csv_col=table_col. Repeat or comma-separate.",
    )
    import_parser.add_argument(
        "--manual-map-only",
        action="store_true",
        help="Disable auto-mapping and only use --map pairs.",
    )
    import_parser.add_argument("--batch-size", type=int, default=2000, help="Insert batch size.")
    import_parser.add_argument("--limit", type=int, help="Only process first N rows.")
    import_parser.add_argument("--dry-run", action="store_true", help="Validate and count rows without insert.")
    import_parser.add_argument(
        "--drop",
        action="store_true",
        help="TRUNCATE target table (RESTART IDENTITY CASCADE) before inserting.",
    )
    import_parser.add_argument(
        "--fk-precheck",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Check foreign key readiness and sampled missing references before import.",
    )
    import_parser.add_argument(
        "--fk-precheck-rows",
        type=int,
        default=2000,
        help="How many rows to scan for FK precheck if --limit is not set.",
    )
    import_parser.add_argument(
        "--on-conflict-do-nothing",
        action="store_true",
        help="Append ON CONFLICT DO NOTHING to insert statement.",
    )
    import_parser.set_defaults(func=cmd_import)

    wizard_parser = subparsers.add_parser("wizard", help="Interactive CSV selection and import flow.")
    wizard_parser.add_argument("--batch-size", type=int, default=2000, help="Insert batch size.")
    wizard_parser.add_argument("--sample-rows", type=int, default=5, help="Rows shown in previews.")
    wizard_parser.add_argument(
        "--sample-scan-rows",
        type=int,
        default=500,
        help="Rows scanned for null/coverage profile in wizard previews.",
    )
    wizard_parser.add_argument(
        "--on-conflict-do-nothing",
        action="store_true",
        help="Append ON CONFLICT DO NOTHING to insert statement.",
    )
    wizard_parser.add_argument(
        "--top-issues",
        type=int,
        default=12,
        help="How many issue groups to show in mismatch details.",
    )
    wizard_parser.add_argument(
        "--max-columns",
        type=int,
        default=10,
        help="Maximum columns to print in compact mismatch lists.",
    )
    wizard_parser.set_defaults(func=cmd_wizard)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
