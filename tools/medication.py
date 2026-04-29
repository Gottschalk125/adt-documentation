import csv
import random
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DOSE_FILE = BASE_DIR / "dose.csv"
DOSE_LOOKUP_FILE = BASE_DIR / "dose_lookup.csv"
DRUG_FILE = BASE_DIR / "drugs.csv"
OUTPUT_FILE = BASE_DIR / "medication.csv"
MEDICATION_SAMPLE_FILE = BASE_DIR / "medication_sample.csv"

ROW_COUNT = 10000000
PROGRESS_EVERY = 100_000
MEDICATION_SAMPLE_SIZE = 200_000
WRITE_BUFFER_ROWS = 20_000

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


def load_doses_from_lookup() -> dict[str, list[str]]:
    doses_by_unit: dict[str, list[str]] = {}
    with DOSE_LOOKUP_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            unit = (row.get("unit") or "").strip()
            dose_id = (row.get("id") or "").strip()
            if not unit or not dose_id:
                continue
            doses_by_unit.setdefault(unit, []).append(dose_id)
            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} sampled dose rows loaded...")
    return doses_by_unit


def load_doses_from_csv() -> dict[str, list[str]]:
    doses_by_unit: dict[str, list[str]] = {}
    with DOSE_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            unit = (row.get("unit") or "").strip()
            dose_id = (row.get("id") or "").strip()
            if not unit or not dose_id:
                continue
            doses_by_unit.setdefault(unit, []).append(dose_id)
            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} dose rows loaded...")
    return doses_by_unit


def load_doses() -> dict[str, list[str]]:
    if DOSE_LOOKUP_FILE.exists():
        print(f"Loading sampled dose lookup from {DOSE_LOOKUP_FILE.name}...")
        return load_doses_from_lookup()

    print(f"Loading doses from {DOSE_FILE.name}...")
    return load_doses_from_csv()


def load_drugs() -> list[tuple[str, str]]:
    drugs: list[tuple[str, str]] = []
    with DRUG_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            drug_id = (row.get("id") or "").strip()
            drug_type = (row.get("type") or "").strip()
            if not drug_id:
                continue
            drugs.append((drug_id, drug_type))
            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} drug rows loaded...")
    return drugs


def build_dose_choices(doses_by_unit: dict[str, list[str]]) -> tuple[dict[str, list[str]], list[str]]:
    doses_by_drug_type: dict[str, list[str]] = {}
    all_doses = [dose_id for items in doses_by_unit.values() for dose_id in items]

    for drug_type, valid_units in TYPE_TO_UNITS.items():
        choices: list[str] = []
        for unit in valid_units:
            choices.extend(doses_by_unit.get(unit, []))
        if choices:
            doses_by_drug_type[drug_type] = choices

    return doses_by_drug_type, all_doses


def build_date_strings() -> tuple[list[str], int]:
    base_start = date(2020, 1, 1)
    today = date.today()
    max_start_offset = (today - base_start).days
    date_strings = [
        (base_start + timedelta(days=offset)).isoformat()
        for offset in range(max_start_offset + 181)
    ]
    return date_strings, max_start_offset


def add_medication_sample(
    samples: list[tuple[int, str, str]],
    seen_count: int,
    medication_id: int,
    started: str,
    ended: str,
) -> None:
    payload = (medication_id, started, ended)
    if len(samples) < MEDICATION_SAMPLE_SIZE:
        samples.append(payload)
        return

    replace_index = random.randint(1, seen_count)
    if replace_index <= MEDICATION_SAMPLE_SIZE:
        samples[replace_index - 1] = payload


def write_medication_samples(samples: list[tuple[int, str, str]]) -> None:
    with MEDICATION_SAMPLE_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "started", "ended"])
        for medication_id, started, ended in samples:
            writer.writerow([medication_id, started, ended])


def main() -> None:
    doses_by_unit = load_doses()
    print(f"Loading drugs from {DRUG_FILE.name}...")
    drugs = load_drugs()

    if not doses_by_unit:
        raise RuntimeError(f"No doses found in {DOSE_FILE}. Run dose.py first.")
    if not drugs:
        raise RuntimeError(f"No drugs found in {DRUG_FILE}.")

    doses_by_drug_type, all_doses = build_dose_choices(doses_by_unit)
    date_strings, max_start_offset = build_date_strings()
    samples: list[tuple[int, str, str]] = []
    rand_choice = random.choice
    rand_random = random.random
    rand_randint = random.randint
    buffer: list[str] = []

    with OUTPUT_FILE.open(mode="w", encoding="utf-8", newline="") as f:
        f.write("id,dosis,drug,started,ended\n")

        for medication_id in range(1, ROW_COUNT + 1):
            drug_id, drug_type = rand_choice(drugs)
            dose_choices = doses_by_drug_type.get(drug_type, all_doses)
            dose_id = rand_choice(dose_choices)
            start_offset = rand_randint(0, max_start_offset)
            started = date_strings[start_offset]
            if rand_random() < 0.2:
                ended_value = ""
            else:
                ended_value = date_strings[start_offset + rand_randint(1, 180)]
            add_medication_sample(samples, medication_id, medication_id, started, ended_value)
            buffer.append(f"{medication_id},{dose_id},{drug_id},{started},{ended_value}\n")

            if len(buffer) >= WRITE_BUFFER_ROWS:
                f.writelines(buffer)
                buffer.clear()

            if medication_id % PROGRESS_EVERY == 0:
                print(f"{medication_id} rows written...")

        if buffer:
            f.writelines(buffer)

    write_medication_samples(samples)
    print(f"{MEDICATION_SAMPLE_FILE.name} created with {len(samples)} sampled medications.")
    print(f"{OUTPUT_FILE.name} created with {ROW_COUNT} rows.")


if __name__ == "__main__":
    main()
