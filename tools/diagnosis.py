import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
MEDICATION_FILE = BASE_DIR / "medication.csv"
DOCTORS_FILE = BASE_DIR / "doctors.csv"
PATIENTS_FILE = BASE_DIR / "patients.csv"
DISEASES_FILE = BASE_DIR / "diseases_unique.csv"
OUTPUT_FILE = BASE_DIR / "diagnosis.csv"
#CURRENTLY 10.000.000 Million Diagnosis, can be changed here
ROW_COUNT = 10000000


def load_medications() -> list[dict[str, datetime, datetime]]:
    medications: list[dict[str, datetime, datetime]] = []
    with MEDICATION_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            med_id = (row.get("id") or "").strip()
            started = (row.get("started") or "").strip()
            ended = (row.get("ended") or "").strip()
            if not med_id or not started:
                continue
            medications.append(
                {
                    "id": med_id,
                    "start_date": datetime.strptime(started, "%Y-%m-%d"),
                    "end_date": datetime.strptime(ended, "%Y-%m-%d"), 
                }
            )
    return medications


def load_doctors() -> list[str]:
    doctors: list[str] = []
    with DOCTORS_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doctor_id = (row.get("id") or "").strip()
            if doctor_id:
                doctors.append(doctor_id)
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
    with PATIENTS_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            patient_person = (row.get("person_id") or "").strip()
            if patient_person:
                patients.append(patient_person)
    return patients


def main() -> None:
    medications = load_medications()
    doctors = load_doctors()
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

    diagnoses: list[dict[str, str]] = []
    for diagnosis_id in range(1, ROW_COUNT + 1):
        med = random.choice(medications)
        disease = random.choice(diseases)
        doctor = random.choice(doctors)
        patient = random.choice(patients)
        diagnosed_at = med["start_date"] - timedelta(days=random.randint(0, 3))
        diagnosed_end = med["end_date"] - timedelta(days = random.randint(-3, 5))
        diagnoses.append(
            {
                "id": str(diagnosis_id),
                "medication": med["id"],
                "disease": disease,
                "diagnosed_by": doctor,
                "diagnosed_patient": patient,
                "diagnosed_at": diagnosed_at.strftime("%Y-%m-%d"),
                "diagnosed_end": diagnosed_end.strftime("%Y-%m-%d")
            }
        )

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        fieldnames = ["id", "medication", "disease", "diagnosed_by", "diagnosed_patient", "diagnosed_at", "diagnosed_end"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(diagnoses)

    print(
        f"{OUTPUT_FILE.name} created with {ROW_COUNT} rows "
        f"({len(diseases)} diseases available)."
    )


if __name__ == "__main__":
    main()
