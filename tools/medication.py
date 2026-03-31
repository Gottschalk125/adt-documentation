import csv
import uuid
import random
from datetime import datetime, timedelta

DOSE_FILE = "tools/dose.csv"
DRUG_FILE = "tools/drugs.csv"
OUTPUT_FILE = "tools/medication.csv"

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
    "suppository": ["unit"]
}

def load_doses():
    doses_by_unit = {}

    with open(DOSE_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            unit = row["unit"]
            doses_by_unit.setdefault(unit, []).append(row["uuid"])

    return doses_by_unit

def load_drugs():
    drugs = []
    with open(DRUG_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            drugs.append(row)
    return drugs

def random_date_range():
    start = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 2000))
    duration_days = random.randint(1, 180)

    end = start + timedelta(days=duration_days)

    return start.date(), end.date()

def get_matching_dose(drug_type, doses_by_unit):
    valid_units = TYPE_TO_UNITS.get(drug_type, [])

    possible_doses = []
    for unit in valid_units:
        possible_doses.extend(doses_by_unit.get(unit, []))

    if not possible_doses:
        return None

    return random.choice(possible_doses)

def main():
    doses_by_unit = load_doses()
    drugs = load_drugs()

    with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "dose", "drug", "started", "ended"])
        #amount of rows needed
        for _ in range(10000):
            drug = random.choice(drugs)
            drug_id = drug["id"]
            drug_type = drug["type"]

            dose_uuid = get_matching_dose(drug_type, doses_by_unit)

            # Fallback (falls nichts passt)
            if not dose_uuid:
                all_doses = [d for doses in doses_by_unit.values() for d in doses]
                dose_uuid = random.choice(all_doses)

            started, ended = random_date_range()

            writer.writerow([
                str(uuid.uuid4()),
                dose_uuid,
                drug_id,
                started,
                ended
            ])

    print(f"{OUTPUT_FILE} wurde erfolgreich erstellt.")

if __name__ == "__main__":
    main()