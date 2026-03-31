import csv
import random
from collections import defaultdict

INPUT_FILE = "employees.csv"
DOCTORS_FILE = "doctors.csv"
NURSES_FILE = "nurses.csv"

DOCTOR_RATIO = 0.30
RANDOM_SEED = 42

PHONE_PREFIX = "49931201"

DOCTOR_TYPES = [
    "assistant_physician",
    "senior_physician",
    "chief_physician",
    "consultant",
    "resident",
    "attending_physician",
    "head_of_department",
]

NON_HEAD_DOCTOR_TYPES = [
    "assistant_physician",
    "senior_physician",
    "chief_physician",
    "consultant",
    "resident",
    "attending_physician",
]


def generate_unique_work_phone(used_numbers):
    """
    Erzeugt eine eindeutige Telefonnummer mit Prefix 49931201.
    """
    while True:
        suffix = "".join(str(random.randint(0, 9)) for _ in range(6))
        phone_number = PHONE_PREFIX + suffix
        if phone_number not in used_numbers:
            used_numbers.add(phone_number)
            return phone_number


def read_input_csv(file_path):
    """
    Liest eine CSV ein.
    Erwartet:
    - 1. Spalte = UUID
    - 2. Spalte = department_id

    Unterstützt CSVs mit oder ohne Header.
    """
    rows = []

    with open(file_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        all_rows = list(reader)

    if not all_rows:
        return rows

    start_index = 0

    if len(all_rows[0]) >= 2:
        try:
            int(all_rows[0][1].strip())
        except ValueError:
            start_index = 1

    for row in all_rows[start_index:]:
        if len(row) < 2:
            continue

        person_id = row[0].strip()
        department_id = row[1].strip()

        if not person_id or not department_id:
            continue

        rows.append({
            "id": person_id,
            "department_id": department_id
        })

    return rows


def write_doctors_csv(file_path, doctors):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "work_phone", "type"])

        for doctor in doctors:
            writer.writerow([
                doctor["id"],
                doctor["work_phone"],
                doctor["type"]
            ])


def write_nurses_csv(file_path, nurses):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id"])

        for nurse_id in nurses:
            writer.writerow([nurse_id])


def main():
    random.seed(RANDOM_SEED)

    input_rows = read_input_csv(INPUT_FILE)

    if not input_rows:
        print("Keine gültigen Daten gefunden.")
        return

    # Nach Department gruppieren
    rows_by_department = defaultdict(list)
    for row in input_rows:
        rows_by_department[row["department_id"]].append(row)

    all_departments = sorted(rows_by_department.keys(), key=lambda x: int(x))
    department_count = len(all_departments)

    # Zielanzahl Doctors anhand Ratio
    target_doctor_count = int(len(input_rows) * DOCTOR_RATIO)

    # Damit jedes Department einen Head bekommen kann,
    # müssen mindestens so viele Doctors existieren wie Departments.
    if target_doctor_count < department_count:
        print(
            f"Hinweis: {target_doctor_count} Doctors laut Ratio reichen nicht für "
            f"{department_count} Departments. Doctor-Anzahl wird auf {department_count} erhöht."
        )
        target_doctor_count = department_count

    # Sicherheitscheck: Falls ein Department leer wäre
    empty_departments = [dept for dept, rows in rows_by_department.items() if not rows]
    if empty_departments:
        print(f"Fehler: Leere Departments gefunden: {empty_departments}")
        return

    # 1. Pro Department genau eine Person als Head auswählen
    selected_doctor_ids = set()
    doctors = []
    used_phone_numbers = set()

    for department_id in all_departments:
        candidate = random.choice(rows_by_department[department_id])
        selected_doctor_ids.add(candidate["id"])

        doctors.append({
            "id": candidate["id"],
            "work_phone": generate_unique_work_phone(used_phone_numbers),
            "type": "head_of_department"
        })

    # 2. Restliche Personen sammeln, die noch keine Heads sind
    remaining_candidates = [
        row for row in input_rows if row["id"] not in selected_doctor_ids
    ]
    random.shuffle(remaining_candidates)

    remaining_doctor_slots = target_doctor_count - len(doctors)

    additional_doctor_rows = remaining_candidates[:remaining_doctor_slots]

    for row in additional_doctor_rows:
        doctors.append({
            "id": row["id"],
            "work_phone": generate_unique_work_phone(used_phone_numbers),
            "type": random.choice(NON_HEAD_DOCTOR_TYPES)
        })
        selected_doctor_ids.add(row["id"])

    # 3. Alle anderen werden Nurses
    nurses = [
        row["id"] for row in input_rows
        if row["id"] not in selected_doctor_ids
    ]

    write_doctors_csv(DOCTORS_FILE, doctors)
    write_nurses_csv(NURSES_FILE, nurses)

    print("Fertig.")
    print(f"Doctors CSV erstellt: {DOCTORS_FILE} ({len(doctors)} Einträge)")
    print(f"Nurses CSV erstellt: {NURSES_FILE} ({len(nurses)} Einträge)")
    print(f"Anzahl Departments: {department_count}")
    print(f"Head of Department vergeben: {department_count}")


if __name__ == "__main__":
    main()