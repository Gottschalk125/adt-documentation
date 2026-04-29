import csv
import random
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
MEDICATION_FILE = BASE_DIR / "medication.csv"
MEDICATION_SAMPLE_FILE = BASE_DIR / "medication_sample.csv"
DOCTORS_FILE = BASE_DIR / "doctors.csv"
PATIENTS_FILE = BASE_DIR / "patients.csv"
PATIENTS_SAMPLE_FILE = BASE_DIR / "patients_sample.csv"
DISEASES_FILE = BASE_DIR / "diseases_unique.csv"
OUTPUT_FILE = BASE_DIR / "diagnosis.csv"

ROW_COUNT = 10000000
PROGRESS_EVERY = 100_000
WRITE_BUFFER_ROWS = 20_000


def csv_escape(value: str) -> str:
    if any(char in value for char in [",", "\"", "\n", "\r"]):
        return '"' + value.replace('"', '""') + '"'
    return value


def load_medications() -> list[tuple[str, int]]:
    medications: list[tuple[str, int]] = []
    source_file = MEDICATION_SAMPLE_FILE if MEDICATION_SAMPLE_FILE.exists() else MEDICATION_FILE

    with source_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            med_id = (row.get("id") or "").strip()
            started = (row.get("started") or "").strip()
            ended = (row.get("ended") or "").strip()

            if not med_id or not started:
                continue

            start_date = date.fromisoformat(started)
            medications.append((med_id, start_date.toordinal()))

            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} medication rows loaded for diagnosis...")

    return medications


def load_doctors() -> list[str]:
    doctors: list[str] = []
    with DOCTORS_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            doctor_id = (row.get("id") or "").strip()
            if doctor_id:
                doctors.append(doctor_id)
            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} doctor rows loaded...")
    return doctors


def load_diseases() -> list[str]:
    diseases: list[str] = []
    with DISEASES_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            disease = (row.get("name") or "").strip()
            if disease:
                diseases.append(disease)
    return diseases


def load_patients() -> list[str]:
    patients: list[str] = []
    source_file = PATIENTS_SAMPLE_FILE if PATIENTS_SAMPLE_FILE.exists() else PATIENTS_FILE
    with source_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            patient_person = (row.get("person") or "").strip()
            if patient_person:
                patients.append(patient_person)
            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} patient rows loaded for diagnosis...")
    return patients


def main() -> None:
    print("Loading medications for diagnosis...")
    medications = load_medications()
    print("Loading doctors for diagnosis...")
    doctors = load_doctors()
    print("Loading patients for diagnosis...")
    patients = load_patients()
    diseases = load_diseases()

    if not medications:
        raise RuntimeError(f"No medications found in {MEDICATION_FILE}. Run medication.py first.")
    if not doctors:
        raise RuntimeError(f"No doctors found in {DOCTORS_FILE}.")
    if not patients:
        raise RuntimeError(f"No patients found in {PATIENTS_FILE}. Run Employe_Patients.py first.")
    if not diseases:
        raise RuntimeError(f"No diseases found in {DISEASES_FILE}.")

    ord_to_iso = {}
    rand_choice = random.choice
    rand_randint = random.randint
    buffer: list[str] = []

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        f.write("id,medication,disease,diagnosed_by,diagnosed_patient,diagnosed_at\n")
        for diagnosis_id in range(1, ROW_COUNT + 1):
            medication_id, start_ordinal = rand_choice(medications)
            diagnosed_at_ordinal = start_ordinal - rand_randint(0, 3)
            diagnosed_at = ord_to_iso.get(diagnosed_at_ordinal)
            if diagnosed_at is None:
                diagnosed_at = date.fromordinal(diagnosed_at_ordinal).isoformat()
                ord_to_iso[diagnosed_at_ordinal] = diagnosed_at

            disease = rand_choice(diseases)
            doctor = rand_choice(doctors)
            patient = rand_choice(patients)
            buffer.append(
                f"{diagnosis_id},{medication_id},{csv_escape(disease)},{doctor},{patient},{diagnosed_at}\n"
            )

            if len(buffer) >= WRITE_BUFFER_ROWS:
                f.writelines(buffer)
                buffer.clear()

            # Fortschritt anzeigen (wichtig bei 10 Mio)
            if diagnosis_id % PROGRESS_EVERY == 0:
                print(f"{diagnosis_id} rows written...")

        if buffer:
            f.writelines(buffer)

    print(
        f"{OUTPUT_FILE.name} created with {ROW_COUNT} rows "
        f"({len(diseases)} diseases available)."
    )


if __name__ == "__main__":
    main()
