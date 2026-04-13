#!/usr/bin/env python3
from __future__ import annotations

import csv
import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DEPARTMENTS_CSV = BASE_DIR / "departments.csv"
EMPLOYEES_CSV = BASE_DIR / "employees.csv"
NURSES_CSV = BASE_DIR / "nurses.csv"
PATIENTS_CSV = BASE_DIR / "patients.csv"
STATIONS_CSV = BASE_DIR / "stations.csv"
ROOMS_CSV = BASE_DIR / "rooms.csv"
BOOKINGS_CSV = BASE_DIR / "bookings.csv"

RANDOM_SEED = 42
LOOKBACK_DAYS = 180
LOOKAHEAD_DAYS = 14


@dataclass(frozen=True)
class StationProfile:
    station_name: str
    ward_type: str
    beds_per_nurse: float


INPATIENT_PROFILES: dict[int, StationProfile] = {
    1: StationProfile("Interdisziplinaere Intensivstation", "icu", 0.85),
    4: StationProfile("Augenstation", "general", 1.20),
    5: StationProfile("HNO-Station", "general", 1.30),
    6: StationProfile("Neurochirurgische Station", "surgery", 1.30),
    7: StationProfile("Neurologische Station", "internal", 1.30),
    8: StationProfile("Onkologische und Palliativstation", "oncology", 1.25),
    10: StationProfile("Frauenklinik Station", "general", 1.40),
    11: StationProfile("Dermatologische Station", "general", 1.30),
    12: StationProfile("Viszeralchirurgische Station", "surgery", 1.30),
    13: StationProfile("Paediatrische Station", "pediatrics", 1.30),
    14: StationProfile("Psychiatrische Akutstation", "psych", 1.80),
    15: StationProfile("Kinder- und Jugendpsychiatrie", "psych", 1.60),
    16: StationProfile("MKG-Station", "surgery", 1.20),
    21: StationProfile("Unfallchirurgische Station", "surgery", 1.30),
    22: StationProfile("Thorax- und Herzchirurgie Station", "surgery", 1.10),
    23: StationProfile("Urologische Station", "general", 1.30),
    24: StationProfile("Innere Medizin I Station", "internal", 1.30),
    25: StationProfile("Innere Medizin II Station", "internal", 1.30),
}

ROOM_SINGLE_SHARE = {
    "icu": 1.00,
    "general": 0.20,
    "surgery": 0.20,
    "internal": 0.25,
    "oncology": 0.30,
    "pediatrics": 0.35,
    "psych": 0.30,
}

LENGTH_OF_STAY = {
    "icu": (2, 6, 14),
    "general": (1, 3, 7),
    "surgery": (2, 5, 10),
    "internal": (3, 6, 14),
    "oncology": (4, 8, 16),
    "pediatrics": (1, 4, 8),
    "psych": (10, 21, 45),
}

GAP_DAYS = {
    "icu": (0, 0, 1),
    "general": (0, 1, 2),
    "surgery": (0, 1, 2),
    "internal": (0, 1, 2),
    "oncology": (0, 1, 3),
    "pediatrics": (0, 1, 2),
    "psych": (0, 1, 4),
}


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_floor(building: str) -> int:
    for char in building:
        if char.isdigit():
            return int(char)
    return 1


def build_room_sizes(total_beds: int, ward_type: str, rng: random.Random) -> list[int]:
    if ward_type == "icu":
        return [1] * total_beds

    room_sizes: list[int] = []
    remaining = total_beds
    single_share = ROOM_SINGLE_SHARE[ward_type]

    while remaining > 0:
        if remaining == 1:
            size = 1
        elif remaining == 2:
            size = 2
        else:
            size = 1 if rng.random() < single_share else 2
        room_sizes.append(size)
        remaining -= size

    return room_sizes


def sample_duration(profile: tuple[int, int, int], rng: random.Random) -> int:
    low, mode, high = profile
    return max(1, round(rng.triangular(low, high, mode)))


def choose_state(admission: date, discharge: date, today: date, rng: random.Random) -> str:
    if admission <= today < discharge:
        return "checked_in"
    if admission > today:
        return "confirmed" if (admission - today).days <= 7 or rng.random() < 0.70 else "pending"
    return "checked_out_early" if rng.random() < 0.08 else "completed"


def build_hospital_layout(
    departments: list[dict[str, str]],
    employees: list[dict[str, str]],
    nurses: list[dict[str, str]],
    rng: random.Random,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    dict[int, str],
]:
    employee_department = {row["id"]: int(row["department_id"]) for row in employees}
    nurse_counts = Counter(employee_department[row["id"]] for row in nurses if row["id"] in employee_department)
    departments_by_id = {int(row["id"]): row for row in departments}

    stations: list[dict[str, object]] = []
    rooms: list[dict[str, object]] = []
    nurses_with_station: list[dict[str, object]] = []
    room_types: dict[int, str] = {}

    next_room_id = 1
    station_ids = sorted(INPATIENT_PROFILES)
    station_by_department = {department_id: department_id for department_id in station_ids}

    for department_id in station_ids:
        profile = INPATIENT_PROFILES[department_id]
        department = departments_by_id[department_id]
        nurse_count = nurse_counts.get(department_id, 0)
        bed_count = max(6, round(nurse_count * profile.beds_per_nurse))
        room_sizes = build_room_sizes(bed_count, profile.ward_type, rng)
        stations.append(
            {
                "id": department_id,
                "name": profile.station_name,
                "department": department_id,
                "rooms": len(room_sizes),
            }
        )

        floor = parse_floor(department["building"])
        for room_index, beds in enumerate(room_sizes, start=1):
            room_number = floor * 1000 + department_id * 10 + room_index
            rooms.append(
                {
                    "id": next_room_id,
                    "station": department_id,
                    "number": room_number,
                    "floor": floor,
                    "beds": beds,
                }
            )
            room_types[next_room_id] = profile.ward_type
            next_room_id += 1

    for nurse in nurses:
        department_id = employee_department.get(nurse["id"])
        station_id = station_by_department.get(department_id, "")
        nurses_with_station.append(
            {
                "id": nurse["id"],
                "station": station_id,
            }
        )

    return stations, rooms, nurses_with_station, room_types


def build_patient_pool(patients: list[dict[str, str]], rng: random.Random) -> list[int]:
    patient_ids = [int(row["id"]) for row in patients]
    rng.shuffle(patient_ids)
    return patient_ids


def generate_occupied_bookings(
    rooms: list[dict[str, object]],
    room_types: dict[int, str],
    patient_pool: list[int],
    today: date,
    rng: random.Random,
) -> list[dict[str, object]]:
    booking_rows: list[dict[str, object]] = []
    horizon_start = today - timedelta(days=LOOKBACK_DAYS)
    horizon_end = today + timedelta(days=LOOKAHEAD_DAYS)

    for room in rooms:
        room_id = int(room["id"])
        room_beds = int(room["beds"])
        ward_type = room_types[room_id]
        los_profile = LENGTH_OF_STAY[ward_type]
        gap_profile = GAP_DAYS[ward_type]

        for _bed_index in range(room_beds):
            cursor = horizon_start - timedelta(days=sample_duration(los_profile, rng))
            while True:
                cursor += timedelta(days=sample_duration(gap_profile, rng))
                admission = cursor
                if admission > horizon_end:
                    break

                discharge = admission + timedelta(days=sample_duration(los_profile, rng))
                if discharge <= horizon_start:
                    cursor = discharge
                    continue

                if not patient_pool:
                    raise RuntimeError("Not enough patients available to build bookings.")

                booking_rows.append(
                    {
                        "from": admission.isoformat(),
                        "until": discharge.isoformat(),
                        "state": choose_state(admission, discharge, today, rng),
                        "room": room_id,
                        "patient": patient_pool.pop(),
                    }
                )
                cursor = discharge

    return booking_rows


def generate_non_occupancy_events(
    rooms: list[dict[str, object]],
    patient_pool: list[int],
    occupied_booking_count: int,
    today: date,
    rng: random.Random,
) -> list[dict[str, object]]:
    event_rows: list[dict[str, object]] = []
    event_count = max(24, round(occupied_booking_count * 0.03))

    for _ in range(event_count):
        if not patient_pool:
            break

        room_id = int(rng.choice(rooms)["id"])
        if rng.random() < 0.55:
            admission = today + timedelta(days=rng.randint(1, LOOKAHEAD_DAYS + 21))
            state = "cancelled"
        else:
            admission = today - timedelta(days=rng.randint(5, LOOKBACK_DAYS))
            state = "no_show" if rng.random() < 0.65 else "cancelled"

        discharge = admission + timedelta(days=rng.randint(1, 3))
        event_rows.append(
            {
                "from": admission.isoformat(),
                "until": discharge.isoformat(),
                "state": state,
                "room": room_id,
                "patient": patient_pool.pop(),
            }
        )

    return event_rows


def add_booking_ids(bookings: list[dict[str, object]]) -> list[dict[str, object]]:
    sorted_rows = sorted(
        bookings,
        key=lambda row: (
            row["from"],
            row["room"],
            row["patient"],
            row["state"],
        ),
    )
    with_ids: list[dict[str, object]] = []
    for booking_id, row in enumerate(sorted_rows, start=1):
        payload = {"id": booking_id}
        payload.update(row)
        with_ids.append(payload)
    return with_ids


def compute_occupancy_stats(bookings: list[dict[str, object]], today: date) -> tuple[int, int]:
    delta_by_day: dict[date, int] = defaultdict(int)

    for booking in bookings:
        if booking["state"] in {"cancelled", "no_show"}:
            continue
        admission = date.fromisoformat(str(booking["from"]))
        discharge = date.fromisoformat(str(booking["until"]))
        delta_by_day[admission] += 1
        delta_by_day[discharge] -= 1

    if not delta_by_day:
        return 0, 0

    current = 0
    current_day = min(delta_by_day)
    peak = 0
    occupied_today = 0
    final_day = max(delta_by_day)

    while current_day <= final_day:
        current += delta_by_day.get(current_day, 0)
        peak = max(peak, current)
        if current_day == today:
            occupied_today = current
        current_day += timedelta(days=1)

    return occupied_today, peak


def main() -> None:
    rng = random.Random(RANDOM_SEED)
    today = date.today()

    departments = read_rows(DEPARTMENTS_CSV)
    employees = read_rows(EMPLOYEES_CSV)
    nurses = read_rows(NURSES_CSV)
    patients = read_rows(PATIENTS_CSV)

    stations, rooms, nurses_with_station, room_types = build_hospital_layout(
        departments=departments,
        employees=employees,
        nurses=nurses,
        rng=rng,
    )

    patient_pool = build_patient_pool(patients, rng)
    occupied_bookings = generate_occupied_bookings(
        rooms=rooms,
        room_types=room_types,
        patient_pool=patient_pool,
        today=today,
        rng=rng,
    )
    event_bookings = generate_non_occupancy_events(
        rooms=rooms,
        patient_pool=patient_pool,
        occupied_booking_count=len(occupied_bookings),
        today=today,
        rng=rng,
    )
    bookings = add_booking_ids(occupied_bookings + event_bookings)

    write_rows(STATIONS_CSV, ["id", "name", "department", "rooms"], stations)
    write_rows(ROOMS_CSV, ["id", "station", "number", "floor", "beds"], rooms)
    write_rows(NURSES_CSV, ["id", "station"], nurses_with_station)
    write_rows(BOOKINGS_CSV, ["id", "from", "until", "state", "room", "patient"], bookings)

    staffed_beds = sum(int(room["beds"]) for room in rooms)
    occupied_today, peak_occupancy = compute_occupancy_stats(bookings, today)

    print(f"Generated {len(stations)} stations, {len(rooms)} rooms and {staffed_beds} staffed beds.")
    print(f"Generated {len(bookings)} bookings for the last {LOOKBACK_DAYS} days and next {LOOKAHEAD_DAYS} days.")
    print(f"Current occupied beds: {occupied_today}")
    print(f"Peak occupied beds in generated window: {peak_occupancy}")


if __name__ == "__main__":
    main()
