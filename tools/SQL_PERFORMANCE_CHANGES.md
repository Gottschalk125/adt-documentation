# SQL Performance Changes

## Added Indexes

| Table/column | Change | Helps |
| --- | --- | --- |
| `medication(dosis)` | Added FK index | Joins from `medication` to `dose`. |
| `medication(drug, started, id)` | Added composite FK/date index | Drug treatment history and date-range analysis by drug. |
| `medication(started, id)` | Added date/id index | Time-range scans and keyset-style paging within treatment timelines. |
| `diagnosis(diagnosed_patient, diagnosed_at DESC, id DESC)` | Added composite FK/date index | Patient diagnosis timelines and latest diagnoses per patient. |
| `diagnosis(diagnosed_by, diagnosed_at DESC, id DESC)` | Added composite FK/date index | Doctor workload/history queries. |
| `diagnosis(medication)` | Added FK index | Joins from diagnoses to medications. |
| `diagnosis(diagnosed_at, id)` | Added date/id index | Diagnosis time-series analysis. |
| `diagnosis(disease, diagnosed_at)` | Added composite category/date index | Disease incidence over time. |
| `person(last_name, first_name, id)` | Added lookup index | Name search and stable ordered browsing. |
| `person(city, id)` | Added lookup index | Geographic filtering by city. |
| `employee(department, id)` | Added FK/composite index | Department staffing joins and paging employees per department. |
| `doctors(type, id)` | Added composite index | Filtering doctors by type. |
| `doctors(work_phone)` | Added unique lookup index | Fast doctor lookup by work phone. |
| `nurses(station, id)` | Added FK/composite index | Station staffing joins. |
| `station(department, id)` | Added FK/composite index | Department-to-station joins. |
| `rooms(station, id)` | Added FK/composite index | Station-to-room joins. |
| `rooms(station, number)` | Added unique composite index | Room lookup within a station. |
| `bookings(patient, "from" DESC, id DESC)` | Added composite FK/date index | Patient booking history and latest bookings. |
| `bookings(room, "from", "until")` | Added composite FK/date index | Room occupancy/range queries. |
| `bookings(state, "from", id)` | Added composite status/date index | Operational queues by booking state over time. |
| `bookings("from", id)` | Added date/id index | Booking time-series scans and date-window pagination. |
| `dose(unit, frequency)` | Added composite category index | Dose distribution analysis by unit/frequency. |
| `drugs(type, id)` | Added composite category index | Filtering drugs by type. |
| `drugs(name)` | Added lookup index | Drug name lookups. |

## Added Or Corrected Foreign Keys

No new logical foreign key relationships were added. Existing relationships were preserved with matching `bigint` PK/FK types, and mandatory relationship columns were marked `NOT NULL`.

## Join Improvements

FK-side indexes were added for high-volume joins between `diagnosis`, `patient`, `doctors`, `medication`, `dose`, `drugs`, `employee`, `department`, `station`, `rooms`, and `bookings`.

## Pagination Improvements

All large tables keep integer primary keys for `WHERE id > last_seen_id ORDER BY id LIMIT n`. Composite indexes with trailing `id` were added for filtered keyset pagination by patient, doctor, department, station, status, and date.

## Analytical / Time-Range Improvements

Date indexes were added on `diagnosis.diagnosed_at`, `medication.started`, and `bookings.from`, including composite category/date indexes for disease trends, drug treatment timelines, room occupancy, and booking state analysis.
