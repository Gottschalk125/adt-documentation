import csv
import random
import uuid

INPUT_FILE = "persons_transformed.csv"
PATIENTS_OUTPUT_FILE = "patients.csv"
EMPLOYEES_OUTPUT_FILE = "employees.csv"

random.seed(42)


def read_first_column_uuids(file_path):
    uuids = []

    with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            if not row:
                continue

            first_value = row[0].strip()

            if not first_value or first_value.lower() == "id":
                continue

            uuids.append(first_value)

    return uuids


def main():
    source_uuids = read_first_column_uuids(INPUT_FILE)
    random.shuffle(source_uuids)

    split_index = int(len(source_uuids) * 0.95)

    patient_uuids = source_uuids[:split_index]
    employee_uuids = source_uuids[split_index:]

    with open(PATIENTS_OUTPUT_FILE, "w", newline="", encoding="utf-8") as patients_file:
        writer = csv.writer(patients_file)
        writer.writerow(["id", "person_id"])

        for patient_id, person_uuid in enumerate(patient_uuids, start=1):
            writer.writerow([patient_id, person_uuid])

    with open(EMPLOYEES_OUTPUT_FILE, "w", newline="", encoding="utf-8") as employees_file:
        writer = csv.writer(employees_file)
        writer.writerow(["id", "department_id", "person_id", "currently_working"])

        for person_uuid in employee_uuids:
            currently_working = random.random() < 0.85
            writer.writerow([
                str(uuid.uuid4()),
                random.randint(1, 27),
                person_uuid,
                str(currently_working).lower(),
            ])

    print(f"Patienten-Datei erstellt: {PATIENTS_OUTPUT_FILE}")
    print(f"Mitarbeiter-Datei erstellt: {EMPLOYEES_OUTPUT_FILE}")
    print(f"Anzahl Patients: {len(patient_uuids)}")
    print(f"Anzahl Employees: {len(employee_uuids)}")


if __name__ == "__main__":
    main()
