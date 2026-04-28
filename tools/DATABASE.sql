DROP SCHEMA IF EXISTS "public" CASCADE;
CREATE SCHEMA "public";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Enums
CREATE TYPE "public"."doctors_type" AS ENUM (
    'assistant_physician',
    'senior_physician',
    'chief_physician',
    'consultant',
    'resident',
    'attending_physician',
    'head_of_department'
);

CREATE TYPE "public"."drugs_type" AS ENUM (
    'tablet',
    'capsule',
    'syrup',
    'injection',
    'infusion',
    'ointment',
    'cream',
    'drops',
    'spray',
    'suppository'
);

CREATE TYPE "public"."dose_unit" AS ENUM (
    'mg',
    'g',
    'mcg',
    'ml',
    'l',
    'tablet',
    'capsule',
    'drop',
    'puff',
    'unit'
);

CREATE TYPE "public"."dose_frequency" AS ENUM (
    'every_x_days',
    'x daily',
    'every_x_hours',
    'x weekly',
    'every_x_weeks'
);

CREATE TYPE "public"."booking_state_enum" AS ENUM (
    'pending',
    'confirmed',
    'checked_in',
    'completed',
    'cancelled',
    'no_show',
    'checked_out_early'
);

CREATE TABLE "public"."medication" (
    "id" bigserial NOT NULL,
    "dosis" bigint,
    "drug" bigint,
    "started" date,
    "ended" date,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."diagnosis" (
    "id" bigserial NOT NULL,
    "medication" bigint,
    "disease" text,
    "diagnosed_by" uuid,
    "diagnosed_patient" uuid,
    "diagnosed_at" date,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."person" (
    "id" uuid NOT NULL DEFAULT gen_random_uuid(),
    "gender" text,
    "first_name" text,
    "last_name" text,
    "plz" int,
    "city" text,
    "street" text,
    "street_no" int,
    "country" text,
    "birthday" date,
    "phone" text,
    "email" text,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."patient" (
    "id" bigserial NOT NULL,
    "person" uuid,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."dose" (
    "id" bigserial NOT NULL,
    "unit" "public"."dose_unit",
    "amount" bigint,
    "frequency" "public"."dose_frequency",
    "frequency_amount" bigint,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."department" (
    "id" bigserial NOT NULL,
    "name" text,
    "building" text,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."drugs" (
    "id" bigserial NOT NULL,
    "stock" bigint,
    "name" text,
    "active_ingredient" text,
    "type" "public"."drugs_type",
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."doctors" (
    "id" uuid NOT NULL,
    "work_phone" text,
    "type" "public"."doctors_type",
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."nurses" (
    "id" uuid NOT NULL,
    "station" bigint,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."station" (
    "id" bigserial NOT NULL,
    "name" text,
    "department" bigint,
    "rooms" bigint,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."bookings" (
    "id" bigserial NOT NULL,
    "from" date,
    "until" date,
    "state" "public"."booking_state_enum",
    "room" bigint,
    "patient" bigint,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."rooms" (
    "id" bigserial NOT NULL,
    "station" bigint,
    "number" bigint,
    "floor" bigint,
    "beds" bigint,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."employee" (
    "id" uuid NOT NULL DEFAULT gen_random_uuid(),
    "department" bigint,
    "person" uuid,
    PRIMARY KEY ("id")
);

-- Foreign key constraints
ALTER TABLE "public"."bookings"
    ADD CONSTRAINT "fk_bookings_patient_patient_id"
    FOREIGN KEY ("patient") REFERENCES "public"."patient" ("id");

ALTER TABLE "public"."bookings"
    ADD CONSTRAINT "fk_bookings_room_rooms_id"
    FOREIGN KEY ("room") REFERENCES "public"."rooms" ("id");

ALTER TABLE "public"."diagnosis"
    ADD CONSTRAINT "fk_diagnosis_diagnosed_by_doctors_id"
    FOREIGN KEY ("diagnosed_by") REFERENCES "public"."doctors" ("id");

ALTER TABLE "public"."diagnosis"
    ADD CONSTRAINT "fk_diagnosis_medication_medication_id"
    FOREIGN KEY ("medication") REFERENCES "public"."medication" ("id");

ALTER TABLE "public"."medication"
    ADD CONSTRAINT "fk_medication_dosis_dose_id"
    FOREIGN KEY ("dosis") REFERENCES "public"."dose" ("id");

ALTER TABLE "public"."medication"
    ADD CONSTRAINT "fk_medication_drug_drugs_id"
    FOREIGN KEY ("drug") REFERENCES "public"."drugs" ("id");

ALTER TABLE "public"."patient"
    ADD CONSTRAINT "fk_patient_person_person_id"
    FOREIGN KEY ("person") REFERENCES "public"."person" ("id");

ALTER TABLE "public"."patient"
    ADD CONSTRAINT "uq_patient_person"
    UNIQUE ("person");

ALTER TABLE "public"."diagnosis"
    ADD CONSTRAINT "fk_diagnosis_diagnosed_patient_patient_person"
    FOREIGN KEY ("diagnosed_patient") REFERENCES "public"."patient" ("person");

ALTER TABLE "public"."employee"
    ADD CONSTRAINT "fk_employee_department_department_id"
    FOREIGN KEY ("department") REFERENCES "public"."department" ("id");

ALTER TABLE "public"."employee"
    ADD CONSTRAINT "fk_employee_person_person_id"
    FOREIGN KEY ("person") REFERENCES "public"."person" ("id");

ALTER TABLE "public"."doctors"
    ADD CONSTRAINT "fk_doctors_id_employee_id"
    FOREIGN KEY ("id") REFERENCES "public"."employee" ("id");

ALTER TABLE "public"."nurses"
    ADD CONSTRAINT "fk_nurses_id_employee_id"
    FOREIGN KEY ("id") REFERENCES "public"."employee" ("id");

ALTER TABLE "public"."nurses"
    ADD CONSTRAINT "fk_nurses_station_station_id"
    FOREIGN KEY ("station") REFERENCES "public"."station" ("id");

ALTER TABLE "public"."rooms"
    ADD CONSTRAINT "fk_rooms_station_station_id"
    FOREIGN KEY ("station") REFERENCES "public"."station" ("id");

ALTER TABLE "public"."station"
    ADD CONSTRAINT "fk_station_department_department_id"
    FOREIGN KEY ("department") REFERENCES "public"."department" ("id");
