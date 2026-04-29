import csv
import random
from pathlib import Path

OUTPUT_FILE = Path(__file__).resolve().parent / "dose.csv"
LOOKUP_FILE = Path(__file__).resolve().parent / "dose_lookup.csv"
ROW_COUNT = 10000000
PROGRESS_EVERY = 100_000
SAMPLE_PER_UNIT = 50_000

UNITS = ["mg", "g", "mcg", "ml", "l", "tablet", "capsule", "drop", "puff", "unit"]
FREQUENCIES = ["every_x_days", "x daily", "every_x_hours", "x weekly", "every_x_weeks"]


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
    if freq == "x daily":
        return random.randint(1, 4)
    if freq == "every_x_hours":
        return random.randint(4, 24)
    if freq == "x weekly":
        return random.randint(1, 7)
    if freq == "every_x_weeks":
        return random.randint(1, 12)
    return 1


def add_reservoir_sample(
    samples: dict[str, list[int]],
    seen_counts: dict[str, int],
    unit: str,
    dose_id: int,
) -> None:
    current_sample = samples.setdefault(unit, [])
    current_seen = seen_counts.get(unit, 0) + 1
    seen_counts[unit] = current_seen

    if len(current_sample) < SAMPLE_PER_UNIT:
        current_sample.append(dose_id)
        return

    replace_index = random.randint(1, current_seen)
    if replace_index <= SAMPLE_PER_UNIT:
        current_sample[replace_index - 1] = dose_id


def write_lookup_file(samples: dict[str, list[int]]) -> None:
    with LOOKUP_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["unit", "id"])
        for unit in UNITS:
            for dose_id in samples.get(unit, []):
                writer.writerow([unit, dose_id])


def main() -> None:
    samples: dict[str, list[int]] = {}
    seen_counts: dict[str, int] = {}

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "unit", "amount", "frequency", "frequency_amount"])

        for dose_id in range(1, ROW_COUNT + 1):
            unit = random.choice(UNITS)
            freq = random.choice(FREQUENCIES)
            add_reservoir_sample(samples, seen_counts, unit, dose_id)
            writer.writerow(
                [
                    dose_id,
                    unit,
                    generate_amount(unit),
                    freq,
                    generate_frequency_amount(freq),
                ]
            )

            if dose_id % PROGRESS_EVERY == 0:
                print(f"{dose_id} rows written...")

    write_lookup_file(samples)
    print(f"{LOOKUP_FILE.name} created with up to {SAMPLE_PER_UNIT} sampled dose IDs per unit.")
    print(f"{OUTPUT_FILE.name} created with {ROW_COUNT} rows.")


if __name__ == "__main__":
    main()
