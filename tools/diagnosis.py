import csv
import uuid
import random
from datetime import datetime, timedelta

medications = []
with open('tools/medication.csv', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        medications.append({
            'id': row['id'],
            'start_date': datetime.strptime(row['started'], '%Y-%m-%d')
        })

doctors = []
with open('tools/doctors.csv', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        doctors.append(row['id'])

diseases = []
with open('tools/diseases_unique.csv', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        diseases.append(row['name'])

def main():
    num_entries = 10000
    diagnoses = []
    for _ in range(num_entries):
        med = random.choice(medications)
        disease = random.choice(diseases)
        doctor = random.choice(doctors)
        diagnosed_at = med['start_date'] - timedelta(days=random.randint(0, 3))
        diagnoses.append({
            'id': str(uuid.uuid4()),
            'medication': med['id'],
            'disease': disease,
            'diagnosed_by': doctor,
            'diagnosed_at': diagnosed_at.strftime('%Y-%m-%d')
        })

    with open('tools/diagnosis.csv', 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['id', 'medication', 'disease', 'diagnosed_by', 'diagnosed_at']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(diagnoses)

    print(f"Diagnosis.csv mit {num_entries} Einträgen erstellt ({len(diseases)} verschiedene Krankheiten verfügbar).")

if __name__ == "__main__":
    main()