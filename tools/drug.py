import json
import csv
import random
import re

input_file = "medicine.jsonl"
output_file = "drugs.csv"

def safe_json_load(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError as e:
        print(f"Fehler in Zeile, übersprungen:  {e}")
        return None

def generate_stock():
    return int(random.triangular(0, 5000, 1500))

def extract_active_ingredient(name):
    cleaned = re.sub(r"\b(tablet|capsule|syrup|mg|ml)\b", "", name, flags=re.IGNORECASE)
    return cleaned.strip().split()[0]

def infer_type(name):
    types = ['tablet', 'capsule', 'syrup', 'injection', 'infusion',
             'ointment', 'cream', 'drops', 'spray', 'suppository']
    return random.choice(types)

def main():
    with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['id', 'stock', 'name', 'active_ingredient', 'type'])

        with open(input_file, mode='r', encoding='utf-8') as jsonl_file:
            for i, line in enumerate(jsonl_file, start=1):
                data = safe_json_load(line)
                if not data:
                    continue

                name = data.get("medicine_name", "").strip()

                writer.writerow([
                    data.get("id"),
                    generate_stock(),
                    name,
                    extract_active_ingredient(name),
                    infer_type(name)
                ])

    print(f"{output_file} wurde erfolgreich erstellt.")

if __name__ == "__main__":
    main()