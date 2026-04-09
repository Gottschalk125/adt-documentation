import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DOSE_FILE = BASE_DIR / "dose.csv"
DRUG_FILE = BASE_DIR / "drugs.csv"
OUTPUT_FILE = BASE_DIR / "medication.csv"
ROW_COUNT = 10000

TYPE_TO_UNITS = {
    "tablet": ["tablet"],
    "capsule": ["capsule"],
    "syrup": ["ml"],
    "injection": ["ml"],
    "infusion": ["ml", "l"],
    "ointment": ["g"],
    "cream": ["g"],
    "drops": ["drop"],
    "spray": ["puff"],
    "suppository": ["unit"],
}


def load_doses() -> dict[str, list[str]]:
    doses_by_unit: dict[str, list[str]] = {}
    with DOSE_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            unit = (row.get("unit") or "").strip()
            dose_id = (row.get("id") or "").strip()
            if not unit or not dose_id:
                continue
            doses_by_unit.setdefault(unit, []).append(dose_id)
    return doses_by_unit


def load_drugs() -> list[dict[str, str]]:
    drugs: list[dict[str, str]] = []
    with DRUG_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            drugs.append(row)
    return drugs


def random_date_range() -> tuple[str, str]:
    start = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 2000))
    duration_days = random.randint(1, 180)
    end = start + timedelta(days=duration_days)
    return start.date().isoformat(), end.date().isoformat()


def get_matching_dose(drug_type: str, doses_by_unit: dict[str, list[str]]) -> str | None:
    valid_units = TYPE_TO_UNITS.get(drug_type, [])
    possible_doses: list[str] = []
    for unit in valid_units:
        possible_doses.extend(doses_by_unit.get(unit, []))
    if not possible_doses:
        return None
    return random.choice(possible_doses)


def main() -> None:
    doses_by_unit = load_doses()
    drugs = load_drugs()

    if not doses_by_unit:
        raise RuntimeError(f"No doses found in {DOSE_FILE}. Run dose.py first.")
    if not drugs:
        raise RuntimeError(f"No drugs found in {DRUG_FILE}.")

    all_doses = [dose_id for items in doses_by_unit.values() for dose_id in items]

    with OUTPUT_FILE.open(mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "dose", "drug", "started", "ended"])

        for medication_id in range(1, ROW_COUNT + 1):
            drug = random.choice(drugs)
            drug_id = (drug.get("id") or "").strip()
            drug_type = (drug.get("type") or "").strip()

            dose_id = get_matching_dose(drug_type, doses_by_unit) or random.choice(all_doses)
            started, ended = random_date_range()

            writer.writerow([medication_id, dose_id, drug_id, started, ended])

    print(f"{OUTPUT_FILE.name} created with {ROW_COUNT} rows.")


if __name__ == "__main__":
    main()
