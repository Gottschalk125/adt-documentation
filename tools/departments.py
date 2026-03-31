import csv
import random

OUTPUT_FILE = "departments.csv"

random.seed(42)

DEPARTMENT_NAMES = [
    "Klinik und Poliklinik für Anästhesiologie, Intensivmedizin, Notfallmedizin und Schmerztherapie",
    "Institut für Klinische Transfusionsmedizin und Hämotherapie",
    "Zentrallabor - Gerinnungsambulanz",
    "Augenklinik und Poliklinik",
    "Klinik und Poliklinik für Hals-Nasen-Ohrenheilkunde, Kopf- und Hals-Chirurgie",
    "Neurochirurgische Klinik und Poliklinik",
    "Neurologische Klinik und Poliklinik",
    "Klinik und Poliklinik für Strahlentherapie und Radioonkologie/ Interdisziplinäres Zentrum Palliativmedizin",
    "Institut für Diagnostische und Interventionelle Neuroradiologie",
    "Frauenklinik und Poliklinik",
    "Klinik und Poliklinik für Dermatologie, Venerologie und Allergologie",
    "Klinik und Poliklinik für Allgemein-, Viszeral-, Transplantations-, Gefäß- und Kinderchirurgie (Chirurgische Klinik I)",
    "Kinderklinik und Poliklinik",
    "Klinik und Poliklinik für Psychiatrie, Psychosomatik und Psychotherapie",
    "Klinik und Poliklinik für Kinder- und Jugendpsychiatrie, Psychosomatik und Psychotherapie",
    "Klinik und Poliklinik für Mund-, Kiefer- und Plastische Gesichtschirurgie, Kopf- und Hals-Chirurgie",
    "Poliklinik für Kieferorthopädie",
    "Poliklinik für Zahnärztliche Prothetik",
    "Poliklinik für Zahnerhaltung und Parodontologie",
    "Abteilung für Parodontologie",
    "Klinik und Poliklinik für Unfall-, Hand-, Plastische und Wiederherstellungschirurgie (Chirurgische Klinik II)",
    "Klinik und Poliklinik für Thorax-, Herz- und Thorakale Gefäßchirurgie",
    "Klinik und Poliklinik für Urologie und Kinderurologie",
    "Medizinische Klinik und Poliklinik I",
    "Medizinische Klinik und Poliklinik II",
    "Klinik und Poliklinik für Nuklearmedizin",
    "Institut für Diagnostische und Interventionelle Radiologie",
]

BUILDINGS = [
    "1A", "1B", "1C",
    "2A", "2B", "2C",
    "3A", "3B", "3C",
    "4A", "4B", "4C",
    "5A", "5B", "5C"
]


def main():
    rows = []

    for department_id, department_name in enumerate(DEPARTMENT_NAMES, start=1):
        row = {
            "id": department_id,
            "name": department_name,
            "building": random.choice(BUILDINGS)
        }
        rows.append(row)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["id", "name", "building"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV-Datei wurde erstellt: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()