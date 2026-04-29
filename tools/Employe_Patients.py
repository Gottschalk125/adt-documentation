import csv
import random
import uuid

INPUT_FILE = "persons_transformed.csv"
PATIENTS_OUTPUT_FILE = "patients.csv"
EMPLOYEES_OUTPUT_FILE = "employees.csv"
PATIENTS_SAMPLE_FILE = "patients_sample.csv"
PROGRESS_EVERY = 100_000
EMPLOYEE_RATIO = 0.05
PATIENT_SAMPLE_SIZE = 200_000

random.seed(42)

def add_patient_sample(
    samples: list[tuple[int, str]],
    seen_count: int,
    patient_id: int,
    person_uuid: str,
) -> None:
    if len(samples) < PATIENT_SAMPLE_SIZE:
        samples.append((patient_id, person_uuid))
        return

    replace_index = random.randint(1, seen_count)
    if replace_index <= PATIENT_SAMPLE_SIZE:
        samples[replace_index - 1] = (patient_id, person_uuid)


def write_patient_samples(samples: list[tuple[int, str]]) -> None:
    with open(PATIENTS_SAMPLE_FILE, "w", newline="", encoding="utf-8") as patients_file:
        writer = csv.writer(patients_file)
        writer.writerow(["id", "person"])
        for patient_id, person_uuid in samples:
            writer.writerow([patient_id, person_uuid])


def main():
    patient_count = 0
    employee_count = 0
    samples: list[tuple[int, str]] = []

    with open(INPUT_FILE, "r", newline="", encoding="utf-8") as source_file, open(
        PATIENTS_OUTPUT_FILE, "w", newline="", encoding="utf-8"
    ) as patients_file, open(
        EMPLOYEES_OUTPUT_FILE, "w", newline="", encoding="utf-8"
    ) as employees_file:
        reader = csv.DictReader(source_file)
        patient_writer = csv.writer(patients_file)
        employee_writer = csv.writer(employees_file)

        patient_writer.writerow(["id", "person"])
        employee_writer.writerow(["id", "department", "person"])

        for row_index, row in enumerate(reader, start=1):
            person_uuid = (row.get("id") or "").strip()
            if not person_uuid:
                continue

            if random.random() < EMPLOYEE_RATIO:
                employee_count += 1
                employee_writer.writerow([
                    str(uuid.uuid4()),
                    random.randint(1, 27),
                    person_uuid,
                ])

                if employee_count % PROGRESS_EVERY == 0:
                    print(f"{employee_count} employee rows written...")
            else:
                patient_count += 1
                patient_writer.writerow([patient_count, person_uuid])
                add_patient_sample(samples, patient_count, patient_count, person_uuid)

                if patient_count % PROGRESS_EVERY == 0:
                    print(f"{patient_count} patient rows written...")

            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} person rows processed...")

    write_patient_samples(samples)

    print(f"Patienten-Datei erstellt: {PATIENTS_OUTPUT_FILE}")
    print(f"Mitarbeiter-Datei erstellt: {EMPLOYEES_OUTPUT_FILE}")
    print(f"Patienten-Sample erstellt: {PATIENTS_SAMPLE_FILE} ({len(samples)} Eintraege)")
    print(f"Anzahl Patients: {patient_count}")
    print(f"Anzahl Employees: {employee_count}")


if __name__ == "__main__":
    main()
