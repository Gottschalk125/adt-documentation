import csv
import random
import uuid
from datetime import date, timedelta

INPUT_FILE = "person_10000000.csv"
OUTPUT_FILE = "persons_transformed.csv"
PROGRESS_EVERY = 100_000

random.seed(42)


def generate_birthdate_from_age(age_value: str) -> str:
    today = date.today()

    try:
        age = int(age_value)
    except (TypeError, ValueError):
        age = random.randint(0, 100)

    birth_year = max(1920, today.year - age)

    start_of_year = date(birth_year, 1, 1)
    end_of_year = date(birth_year, 12, 31)

    delta_days = (end_of_year - start_of_year).days
    random_offset = random.randint(0, delta_days)

    birthdate = start_of_year + timedelta(days=random_offset)
    return birthdate.strftime("%Y-%m-%d")

def generate_fake_email(first_name: str, last_name: str) -> str:
    domains = [
        "@pronton.me",
        "@freenet.de",
        "@telekom.de",
        "@gmx.net",
        "@mail.de",
        "@google.com",
        "@microsoft.com",
        "@yahoo.com",
        "@icloud.com",
    ]

    domain = random.choice(domains)

    first = (first_name or "").strip().lower()
    last = (last_name or "").strip().lower()

    if first and last:
        local_part = f"{first}.{last}"
    else:
        local_part = f"user{random.randint(1000,9999)}"

    return local_part + domain

def parse_int(value: str):
    if value is None:
        return ""

    value = value.strip()
    if not value:
        return ""

    try:
        return int(value)
    except ValueError:
        return ""


def map_gender(value: str) -> str:
    value = (value or "").strip().lower()

    if value == "female":
        return "f"
    if value == "male":
        return "m"
    if value == "other":
        return "d"

    return ""


def generate_fake_phone_number() -> str:
    """
    Erzeugt eine zufällige Fake-Mobilnummer im deutschen Stil.
    Beispiel: 0151 12345678
    """
    prefixes = ["0151", "0152", "0157", "0160", "0162", "0170", "0171", "0175", "0176"]
    prefix = random.choice(prefixes)
    number = random.randint(1000000, 99999999)
    return f"{prefix}{number}"


def main():
    written_rows = 0

    with open(INPUT_FILE, "r", encoding="utf-8-sig", newline="") as infile, open(
        OUTPUT_FILE, "w", encoding="utf-8", newline=""
    ) as outfile:
        fieldnames = [
            "id",
            "gender",
            "first_name",
            "last_name",
            "plz",
            "city",
            "street",
            "street_no",
            "country",
            "birthday",
            "phone",
            "email",
        ]
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for written_rows, row in enumerate(reader, start=1):
            writer.writerow(
                {
                    "id": str(uuid.uuid4()),
                    "gender": map_gender(row.get("gender")),
                    "first_name": (row.get("firstname") or "").strip(),
                    "last_name": (row.get("lastname") or "").strip(),
                    "plz": parse_int(row.get("postalcode")),
                    "city": (row.get("city") or "").strip(),
                    "street": (row.get("street") or "").strip(),
                    "street_no": parse_int(row.get("streetnumber")),
                    "country": "USA",
                    "birthday": generate_birthdate_from_age(row.get("age")),
                    "phone": generate_fake_phone_number(),
                    "email": generate_fake_email(row.get("firstname"), row.get("lastname")),
                }
            )

            if written_rows % PROGRESS_EVERY == 0:
                print(f"{written_rows} rows transformed...")

    print(f"{written_rows} Datensätze wurden nach '{OUTPUT_FILE}' exportiert.")


if __name__ == "__main__":
    main()
