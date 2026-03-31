import csv
import uuid
import random

# Ziel-Datei
filename = "dose.csv"

# Mögliche Werte
units = ['mg', 'g', 'mcg', 'ml', 'l', 'tablet', 'capsule', 'drop', 'puff', 'unit']
frequencies = ['every_x_days', 'x_daily', 'every_x_hours', 'x_weekly', 'every_x_weeks']

def generate_amount(unit):
    if unit == 'mg':
        return round(random.uniform(5, 1000), 1)
    elif unit == 'g':
        return round(random.uniform(0.1, 5), 2)
    elif unit == 'mcg':
        return random.randint(10, 1000)
    elif unit == 'ml':
        return round(random.uniform(1, 50), 1)
    elif unit == 'l':
        return round(random.uniform(0.1, 2), 2)
    elif unit in ['tablet', 'capsule']:
        return random.randint(1, 3)
    elif unit == 'drop':
        return random.randint(1, 20)
    elif unit == 'puff':
        return random.randint(1, 4)
    elif unit == 'unit':
        return random.randint(1, 100)
    return 1

def generate_frequency_amount(freq):
    if freq == 'every_x_days':
        return random.randint(1, 30)
    elif freq == 'x_daily':
        return random.randint(1, 4)
    elif freq == 'every_x_hours':
        return random.randint(4, 24)
    elif freq == 'x_weekly':
        return random.randint(1, 7)
    elif freq == 'every_x_weeks':
        return random.randint(1, 12)
    return 1

def main():

 with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    
    writer.writerow(['uuid', 'unit', 'amount', 'frequency', 'frequency_amount'])
    #Hier ändern für die Anzahl der Zeilen
    for _ in range(10000):
        unit = random.choice(units)
        freq = random.choice(frequencies)
        
        row = [
            str(uuid.uuid4()),
            unit,
            generate_amount(unit),
            freq,
            generate_frequency_amount(freq)
        ]
        
        writer.writerow(row)

print(f"{filename} Done with that stuff")

if __name__ == "__main__":
    main()