import csv
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PATIENTS_FILE = BASE_DIR / "patients.csv"
DIAGNOSIS_FILE = BASE_DIR / "diagnosis.csv"
OUTPUT_FILE = BASE_DIR / "patients_with_diagnosis.csv"


def load_patients() -> list[dict[str, str]]:
    patients: list[dict[str, str]] = []
    with PATIENTS_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            patient_id = (row.get("id") or "").strip()
            person_id = (row.get("person") or "").strip()
            if patient_id and person_id:
                patients.append({"id": patient_id, "person_id": person_id})
    return patients


def load_diagnosis_by_patient() -> dict[str, list[str]]:
    diagnosis_by_patient: dict[str, list[str]] = defaultdict(list)
    with DIAGNOSIS_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            diagnosis_id = (row.get("id") or "").strip()
            diagnosed_patient = (row.get("diagnosed_patient") or "").strip()
            if diagnosis_id and diagnosed_patient:
                diagnosis_by_patient[diagnosed_patient].append(diagnosis_id)
    return diagnosis_by_patient


def main() -> None:
    patients = load_patients()
    diagnosis_by_patient = load_diagnosis_by_patient()

    if not patients:
        raise RuntimeError(f"No patients found in {PATIENTS_FILE}.")

    result_rows: list[dict[str, str]] = []
    for patient in patients:
        diagnosis_ids = diagnosis_by_patient.get(patient["id"], [])
        result_rows.append(
            {
                "id": patient["id"],
                "person_id": patient["person_id"],
                "diagnosis_ids": str(diagnosis_ids),
                "diagnosis_count": str(len(diagnosis_ids)),
            }
        )

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        fieldnames = ["id", "person_id", "diagnosis_ids", "diagnosis_count"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(result_rows)

    print(f"{OUTPUT_FILE.name} created with {len(result_rows)} rows.")


if __name__ == "__main__":
    main()
