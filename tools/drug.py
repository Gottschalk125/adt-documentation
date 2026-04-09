import csv
import json
import random
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "medicine.jsonl"
OUTPUT_FILE = BASE_DIR / "drugs.csv"

DRUG_TYPES = [
    "tablet",
    "capsule",
    "syrup",
    "injection",
    "infusion",
    "ointment",
    "cream",
    "drops",
    "spray",
    "suppository",
]


def safe_json_load(line: str):
    try:
        return json.loads(line)
    except json.JSONDecodeError as exc:
        print(f"Skipped malformed JSON line: {exc}")
        return None


def generate_stock() -> int:
    return int(random.triangular(0, 5000, 1500))


def extract_active_ingredient(name: str) -> str:
    cleaned = re.sub(r"\b(tablet|capsule|syrup|mg|ml)\b", "", name, flags=re.IGNORECASE).strip()
    parts = cleaned.split()
    return parts[0] if parts else "unknown"


def infer_type(_name: str) -> str:
    return random.choice(DRUG_TYPES)


def write_rows(rows: list[list[str | int]]) -> None:
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["id", "stock", "name", "active_ingredient", "type"])
        writer.writerows(rows)


def build_from_jsonl() -> list[list[str | int]]:
    by_id: dict[int, list[str | int]] = {}
    with INPUT_FILE.open("r", encoding="utf-8") as jsonl_file:
        for line in jsonl_file:
            data = safe_json_load(line)
            if not data:
                continue
            name = str(data.get("medicine_name", "")).strip()
            drug_id = data.get("id")
            if drug_id in (None, ""):
                continue
            try:
                numeric_id = int(drug_id)
            except (TypeError, ValueError):
                continue

            by_id.setdefault(
                numeric_id,
                [
                    numeric_id,
                    generate_stock(),
                    name or f"medicine_{numeric_id}",
                    extract_active_ingredient(name),
                    infer_type(name),
                ],
            )
    return [by_id[key] for key in sorted(by_id.keys())]


def main() -> None:
    if not INPUT_FILE.exists():
        raise RuntimeError(f"{INPUT_FILE} not found. This generator now requires medicine.jsonl.")

    rows = build_from_jsonl()
    if not rows:
        raise RuntimeError(f"No valid medicine rows found in {INPUT_FILE}.")

    write_rows(rows)
    print(f"{OUTPUT_FILE.name} created with {len(rows)} rows (source: {INPUT_FILE.name}).")


if __name__ == "__main__":
    main()
