import csv
import os
import random
import hmac as hmac_lib
import hashlib
import base64
from datetime import date
from pathlib import Path
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

BASE_DIR = Path(__file__).resolve().parent
MEDICATION_FILE = BASE_DIR / "medication.csv"
MEDICATION_SAMPLE_FILE = BASE_DIR / "medication_sample.csv"
DOCTORS_FILE = BASE_DIR / "doctors.csv"
PATIENTS_FILE = BASE_DIR / "patients.csv"
PATIENTS_SAMPLE_FILE = BASE_DIR / "patients_sample.csv"
DISEASES_FILE = BASE_DIR / "diseases_unique.csv"
OUTPUT_FILE = BASE_DIR / "diagnosis.csv"

ROW_COUNT = 10000000
PROGRESS_EVERY = 100_000
WRITE_BUFFER_ROWS = 20_000

# Set via environment variables before running:
#   export ENCRYPTION_KEY=<64-hex-char-key>
#   export ENCRYPTION_SALT=<salt-string>
ENCRYPTION_KEY_HEX = os.environ.get("ENCRYPTION_KEY", "")
ENCRYPTION_SALT = os.environ.get("ENCRYPTION_SALT", "")


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


def csv_escape(value: str) -> str:
    if any(char in value for char in [",", '"', "\n", "\r"]):
        return '"' + value.replace('"', '""') + '"'
    return value


def load_medications() -> list[tuple[str, int]]:
    medications: list[tuple[str, int]] = []
    source_file = MEDICATION_SAMPLE_FILE if MEDICATION_SAMPLE_FILE.exists() else MEDICATION_FILE

    with source_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            med_id = (row.get("id") or "").strip()
            started = (row.get("started") or "").strip()
            ended = (row.get("ended") or "").strip()
            if not med_id or not started:
                continue
            start_date = date.fromisoformat(started)
            medications.append((med_id, start_date.toordinal()))
            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} medication rows loaded for diagnosis...")

    return medications


def load_doctors() -> list[str]:
    doctors: list[str] = []
    with DOCTORS_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            doctor_id = (row.get("id") or "").strip()
            if doctor_id:
                doctors.append(doctor_id)
            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} doctor rows loaded...")
    return doctors


def load_diseases() -> list[str]:
    diseases: list[str] = []
    with DISEASES_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            disease = (row.get("name") or "").strip()
            if disease:
                diseases.append(disease)
    return diseases


def load_patients() -> list[str]:
    patients: list[str] = []
    source_file = PATIENTS_SAMPLE_FILE if PATIENTS_SAMPLE_FILE.exists() else PATIENTS_FILE
    with source_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            patient_id = (row.get("id") or "").strip()
            if patient_id:
                patients.append(patient_id)
            if row_index % PROGRESS_EVERY == 0:
                print(f"{row_index} patient rows loaded for diagnosis...")
    return patients


def main() -> None:
    master_key, hash_key = setup_crypto()
    aesgcm = AESGCM(master_key)

    print("Loading medications for diagnosis...")
    medications = load_medications()
    print("Loading doctors for diagnosis...")
    doctors = load_doctors()
    print("Loading patients for diagnosis...")
    patients = load_patients()
    diseases = load_diseases()

    if not medications:
        raise RuntimeError(f"No medications found in {MEDICATION_FILE}. Run medication.py first.")
    if not doctors:
        raise RuntimeError(f"No doctors found in {DOCTORS_FILE}.")
    if not patients:
        raise RuntimeError(f"No patients found in {PATIENTS_FILE}. Run Employe_Patients.py first.")
    if not diseases:
        raise RuntimeError(f"No diseases found in {DISEASES_FILE}.")

    ord_to_iso = {}
    rand_choice = random.choice
    rand_randint = random.randint
    buffer: list[str] = []

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        f.write("id,medication,disease,disease_encrypted,disease_hash,diagnosed_by,diagnosed_patient,diagnosed_at\n")

        for diagnosis_id in range(1, ROW_COUNT + 1):
            medication_id, start_ordinal = rand_choice(medications)
            diagnosed_at_ordinal = start_ordinal - rand_randint(0, 3)
            diagnosed_at = ord_to_iso.get(diagnosed_at_ordinal)
            if diagnosed_at is None:
                diagnosed_at = date.fromordinal(diagnosed_at_ordinal).isoformat()
                ord_to_iso[diagnosed_at_ordinal] = diagnosed_at

            disease = rand_choice(diseases)
            doctor = rand_choice(doctors)
            patient = rand_choice(patients)

            disease_encrypted = encrypt(disease, aesgcm)
            disease_hash = blind_index(disease, hash_key)

            buffer.append(
                f"{diagnosis_id},{medication_id},{csv_escape(disease)},"
                f"{csv_escape(disease_encrypted)},{csv_escape(disease_hash)},"
                f"{doctor},{patient},{diagnosed_at}\n"
            )

            if len(buffer) >= WRITE_BUFFER_ROWS:
                f.writelines(buffer)
                buffer.clear()

            if diagnosis_id % PROGRESS_EVERY == 0:
                print(f"{diagnosis_id} rows written...")

        if buffer:
            f.writelines(buffer)

    print(
        f"{OUTPUT_FILE.name} created with {ROW_COUNT} rows "
        f"({len(diseases)} diseases available)."
    )


if __name__ == "__main__":
    main()
