import csv
import random

PATIENTS_FILE = 'tools/patients.csv'
DIAGNOSIS_FILE = 'tools/diagnosis.csv'
OUTPUT_FILE = 'tools/patients_with_diagnosis.csv'

patients = []
with open(PATIENTS_FILE, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        patients.append(row)


diagnosis_pool = []
with open(DIAGNOSIS_FILE, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        diagnosis_pool.append(row['id'])

random.shuffle(diagnosis_pool)


result_rows = []
diag_index = 0

for patient in patients:
    count = random.randint(1, 3)
    assigned = []
    for _ in range(count):
        diag_id = diagnosis_pool[diag_index % len(diagnosis_pool)]
        diag_index += 1
        assigned.append(diag_id)

    result_rows.append({
        'id': patient['id'],
        'person_id': patient['person_id'],
        'diagnosis_ids': assigned 
    })


with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['id', 'person_id', 'diagnosis_ids']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(result_rows)

print(f"Fertig! {len(result_rows)} Patienten in '{OUTPUT_FILE}' geschrieben.")