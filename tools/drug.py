import csv
import json
import random
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "medicine.jsonl"
MEDICATION_FILE = BASE_DIR / "medication.csv"
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


def load_required_drug_ids_from_medication() -> list[int]:
    required: set[int] = set()
    if not MEDICATION_FILE.exists():
        return []
    with MEDICATION_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            value = (row.get("drug") or "").strip()
            if not value:
                continue
            try:
                required.add(int(value))
            except ValueError:
                continue
    return sorted(required)


def write_rows(rows: list[list[str | int]]) -> None:
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["id", "stock", "name", "active_ingredient", "type"])
        writer.writerows(rows)


def build_from_jsonl() -> list[list[str | int]]:
    rows: list[list[str | int]] = []
    with INPUT_FILE.open("r", encoding="utf-8") as jsonl_file:
        for line in jsonl_file:
            data = safe_json_load(line)
            if not data:
                continue
            name = str(data.get("medicine_name", "")).strip()
            drug_id = data.get("id")
            if drug_id in (None, ""):
                continue
            rows.append(
                [
                    int(drug_id),
                    generate_stock(),
                    name or f"Drug {drug_id}",
                    extract_active_ingredient(name),
                    infer_type(name),
                ]
            )
    return rows


def build_fallback_rows() -> list[list[str | int]]:
    required_ids = load_required_drug_ids_from_medication()
    if not required_ids:
        required_ids = list(range(1, 701))

    rows: list[list[str | int]] = []
    for drug_id in required_ids:
        name = f"Drug {drug_id}"
        rows.append(
            [
                drug_id,
                generate_stock(),
                name,
                f"ingredient_{drug_id}",
                random.choice(DRUG_TYPES),
            ]
        )
    return rows


def main() -> None:
    if INPUT_FILE.exists():
        rows = build_from_jsonl()
        source = INPUT_FILE.name
    else:
        rows = build_fallback_rows()
        source = "fallback"

    write_rows(rows)
    print(f"{OUTPUT_FILE.name} created with {len(rows)} rows (source: {source}).")


if __name__ == "__main__":
    main()
