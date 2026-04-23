import csv
import random
from pathlib import Path

OUTPUT_FILE = Path(__file__).resolve().parent / "dose.csv"
#Changed also to 10.000.000
ROW_COUNT = 10000000

UNITS = ["mg", "g", "mcg", "ml", "l", "tablet", "capsule", "drop", "puff", "unit"]
FREQUENCIES = ["every_x_days", "x_daily", "every_x_hours", "x_weekly", "every_x_weeks"]


def generate_amount(unit: str) -> int:
    if unit == "mg":
        return random.randint(5, 1000)
    if unit == "g":
        return random.randint(1, 5)
    if unit == "mcg":
        return random.randint(10, 1000)
    if unit == "ml":
        return random.randint(1, 50)
    if unit == "l":
        return random.randint(1, 2)
    if unit in {"tablet", "capsule"}:
        return random.randint(1, 3)
    if unit == "drop":
        return random.randint(1, 20)
    if unit == "puff":
        return random.randint(1, 4)
    if unit == "unit":
        return random.randint(1, 100)
    return 1


def generate_frequency_amount(freq: str) -> int:
    if freq == "every_x_days":
        return random.randint(1, 30)
    if freq == "x_daily":
        return random.randint(1, 4)
    if freq == "every_x_hours":
        return random.randint(4, 24)
    if freq == "x_weekly":
        return random.randint(1, 7)
    if freq == "every_x_weeks":
        return random.randint(1, 12)
    return 1


def main() -> None:
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "unit", "amount", "frequency", "frequency_amount"])

        for dose_id in range(1, ROW_COUNT + 1):
            unit = random.choice(UNITS)
            freq = random.choice(FREQUENCIES)
            writer.writerow(
                [
                    dose_id,
                    unit,
                    generate_amount(unit),
                    freq,
                    generate_frequency_amount(freq),
                ]
            )

    print(f"{OUTPUT_FILE.name} created with {ROW_COUNT} rows.")


if __name__ == "__main__":
    main()
