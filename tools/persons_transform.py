import csv
import os
import random
import hmac as hmac_lib
import hashlib
import base64
from datetime import date, timedelta
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

INPUT_FILE = "person_10000000.csv"
OUTPUT_FILE = "persons_transformed.csv"
PROGRESS_EVERY = 100_000

# Set via environment variables before running:
#   export ENCRYPTION_KEY=<64-hex-char-key>
#   export ENCRYPTION_SALT=<salt-string>
ENCRYPTION_KEY_HEX = os.environ.get("ENCRYPTION_KEY", "")
ENCRYPTION_SALT = os.environ.get("ENCRYPTION_SALT", "")

random.seed(42)


def hex_to_bytes(hex_string: str) -> bytes:
    return bytes.fromhex(hex_string)


def derive_key_hkdf(ikm: bytes, salt: bytes, info: str) -> bytes:
    # Simplified HKDF-SHA256 matching the Kotlin backend CryptoUtility
    if not salt:
        salt = bytes(32)
    prk = hmac_lib.new(salt, ikm, hashlib.sha256).digest()
    info_bytes = info.encode("utf-8")
    hash1 = hmac_lib.new(prk, info_bytes + bytes([0x01]), hashlib.sha256).digest()
    return hash1[:32]


def setup_crypto() -> tuple[bytes, bytes]:
    if not ENCRYPTION_KEY_HEX or len(ENCRYPTION_KEY_HEX) != 64:
        raise ValueError(
            "ENCRYPTION_KEY env var must be set with exactly 64 hex characters (256-bit key)"
        )
    master_key = hex_to_bytes(ENCRYPTION_KEY_HEX)
    salt = ENCRYPTION_SALT.encode("utf-8")
    hash_key = derive_key_hkdf(master_key, salt, "BLIND_INDEX_KEY")
    return master_key, hash_key


def encrypt(plaintext: str, aesgcm: AESGCM) -> str:
    if not plaintext:
        return ""
    iv = os.urandom(12)
    ciphertext_with_tag = aesgcm.encrypt(iv, plaintext.encode("utf-8"), None)
    return base64.b64encode(iv + ciphertext_with_tag).decode("utf-8")


def blind_index(plaintext: str, hash_key: bytes) -> str:
    if not plaintext:
        return ""
    mac = hmac_lib.new(hash_key, plaintext.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


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
    return (start_of_year + timedelta(days=random_offset)).strftime("%Y-%m-%d")


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
        local_part = f"user{random.randint(1000, 9999)}"
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
    master_key, hash_key = setup_crypto()
    aesgcm = AESGCM(master_key)

    written_rows = 0

    with open(INPUT_FILE, "r", encoding="utf-8-sig", newline="") as infile, open(
        OUTPUT_FILE, "w", encoding="utf-8", newline=""
    ) as outfile:
        fieldnames = [
            "gender",
            "first_name_encrypted",
            "first_name_hash",
            "last_name_encrypted",
            "last_name_hash",
            "plz_encrypted",
            "plz_hash",
            "city_encrypted",
            "city_hash",
            "street_encrypted",
            "street_hash",
            "street_no",
            "country",
            "birthday_encrypted",
            "birthday_hash",
            "phone_encrypted",
            "phone_hash",
            "email_encrypted",
            "email_hash",
        ]
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for written_rows, row in enumerate(reader, start=1):
            first_name = (row.get("firstname") or "").strip()
            last_name = (row.get("lastname") or "").strip()
            plz = str(parse_int(row.get("postalcode")))
            city = (row.get("city") or "").strip()
            street = (row.get("street") or "").strip()
            birthday = generate_birthdate_from_age(row.get("age"))
            phone = generate_fake_phone_number()
            email = generate_fake_email(row.get("firstname"), row.get("lastname"))

            writer.writerow(
                {
                    "gender": map_gender(row.get("gender")),
                    "first_name_encrypted": encrypt(first_name, aesgcm),
                    "first_name_hash": blind_index(first_name, hash_key),
                    "last_name_encrypted": encrypt(last_name, aesgcm),
                    "last_name_hash": blind_index(last_name, hash_key),
                    "plz_encrypted": encrypt(plz, aesgcm),
                    "plz_hash": blind_index(plz, hash_key),
                    "city_encrypted": encrypt(city, aesgcm),
                    "city_hash": blind_index(city, hash_key),
                    "street_encrypted": encrypt(street, aesgcm),
                    "street_hash": blind_index(street, hash_key),
                    "street_no": parse_int(row.get("streetnumber")),
                    "country": "USA",
                    "birthday_encrypted": encrypt(birthday, aesgcm),
                    "birthday_hash": blind_index(birthday, hash_key),
                    "phone_encrypted": encrypt(phone, aesgcm),
                    "phone_hash": blind_index(phone, hash_key),
                    "email_encrypted": encrypt(email, aesgcm),
                    "email_hash": blind_index(email, hash_key),
                }
            )

            if written_rows % PROGRESS_EVERY == 0:
                print(f"{written_rows} rows transformed...")

    print(f"{written_rows} Datensätze wurden nach '{OUTPUT_FILE}' exportiert.")


if __name__ == "__main__":
    main()
