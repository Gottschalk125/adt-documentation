import csv

INPUT_FILE = "tools/dis_dataset.csv"
OUTPUT_FILE = "tools/diseases_unique.csv"

seen = set()
unique = []

with open(INPUT_FILE, newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)  
    for row in reader:
        if not row:
            continue
        name = row[0].strip()
        if name not in seen:
            seen.add(name)
            unique.append(name)

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'name'])
    for i, name in enumerate(unique, 1):
        writer.writerow([i, name])

print(f"Fertig! {len(unique)} einzigartige Einträge in '{OUTPUT_FILE}' geschrieben.")