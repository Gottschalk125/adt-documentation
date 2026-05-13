DROP SCHEMA IF EXISTS "public" CASCADE;
CREATE SCHEMA "public";

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
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "dosis" bigint NOT NULL,
    "drug" bigint NOT NULL,
    "started" date NOT NULL,
    "ended" date,
    CONSTRAINT "chk_medication_date_range"
        CHECK ("ended" IS NULL OR "ended" >= "started"),
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."diagnosis" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "medication" bigint NOT NULL,
    "disease" text NOT NULL,
    "diagnosed_by" bigint NOT NULL,
    "diagnosed_patient" bigint NOT NULL,
    "diagnosed_at" date NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."person" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
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
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "person" bigint NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."dose" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "unit" "public"."dose_unit" NOT NULL,
    "amount" bigint NOT NULL,
    "frequency" "public"."dose_frequency" NOT NULL,
    "frequency_amount" bigint NOT NULL,
    CONSTRAINT "chk_dose_amount_positive"
        CHECK ("amount" > 0),
    CONSTRAINT "chk_dose_frequency_amount_positive"
        CHECK ("frequency_amount" > 0),
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."department" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "name" text NOT NULL,
    "building" text NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."drugs" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "stock" bigint NOT NULL,
    "name" text NOT NULL,
    "active_ingredient" text,
    "type" "public"."drugs_type" NOT NULL,
    CONSTRAINT "chk_drugs_stock_nonnegative"
        CHECK ("stock" >= 0),
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."doctors" (
    "id" bigint NOT NULL,
    "work_phone" text NOT NULL,
    "type" "public"."doctors_type" NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."nurses" (
    "id" bigint NOT NULL,
    "station" bigint,
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."station" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "name" text NOT NULL,
    "department" bigint NOT NULL,
    "rooms" bigint NOT NULL,
    CONSTRAINT "chk_station_rooms_nonnegative"
        CHECK ("rooms" >= 0),
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."bookings" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "from" date NOT NULL,
    "until" date NOT NULL,
    "state" "public"."booking_state_enum" NOT NULL,
    "room" bigint NOT NULL,
    "patient" bigint NOT NULL,
    CONSTRAINT "chk_bookings_date_range"
        CHECK ("until" >= "from"),
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."rooms" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "station" bigint NOT NULL,
    "number" bigint NOT NULL,
    "floor" bigint NOT NULL,
    "beds" bigint NOT NULL,
    CONSTRAINT "chk_rooms_floor_nonnegative"
        CHECK ("floor" >= 0),
    CONSTRAINT "chk_rooms_beds_positive"
        CHECK ("beds" > 0),
    PRIMARY KEY ("id")
);

CREATE TABLE "public"."employee" (
    "id" bigint GENERATED ALWAYS AS IDENTITY,
    "department" bigint NOT NULL,
    "person" bigint NOT NULL,
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
    ADD CONSTRAINT "fk_diagnosis_diagnosed_patient_patient_id"
    FOREIGN KEY ("diagnosed_patient") REFERENCES "public"."patient" ("id");

ALTER TABLE "public"."employee"
    ADD CONSTRAINT "fk_employee_department_department_id"
    FOREIGN KEY ("department") REFERENCES "public"."department" ("id");

ALTER TABLE "public"."employee"
    ADD CONSTRAINT "fk_employee_person_person_id"
    FOREIGN KEY ("person") REFERENCES "public"."person" ("id");

ALTER TABLE "public"."employee"
    ADD CONSTRAINT "uq_employee_person"
    UNIQUE ("person");

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

-- Indexes for foreign key joins, operational lookups, keyset pagination, and analytics.
CREATE INDEX "idx_medication_dosis"
    ON "public"."medication" ("dosis");

CREATE INDEX "idx_medication_drug_started"
    ON "public"."medication" ("drug", "started", "id");

CREATE INDEX "idx_medication_started_id"
    ON "public"."medication" ("started", "id");

CREATE INDEX "idx_diagnosis_patient_date_id"
    ON "public"."diagnosis" ("diagnosed_patient", "diagnosed_at" DESC, "id" DESC);

CREATE INDEX "idx_diagnosis_doctor_date_id"
    ON "public"."diagnosis" ("diagnosed_by", "diagnosed_at" DESC, "id" DESC);

CREATE INDEX "idx_diagnosis_medication"
    ON "public"."diagnosis" ("medication");

CREATE INDEX "idx_diagnosis_date_id"
    ON "public"."diagnosis" ("diagnosed_at", "id");

CREATE INDEX "idx_diagnosis_disease_date"
    ON "public"."diagnosis" ("disease", "diagnosed_at");

CREATE INDEX "idx_person_last_first_id"
    ON "public"."person" ("last_name", "first_name", "id");

CREATE INDEX "idx_person_city_id"
    ON "public"."person" ("city", "id");

CREATE INDEX "idx_employee_department_id"
    ON "public"."employee" ("department", "id");

CREATE INDEX "idx_doctors_type_id"
    ON "public"."doctors" ("type", "id");

CREATE UNIQUE INDEX "idx_doctors_work_phone"
    ON "public"."doctors" ("work_phone");

CREATE INDEX "idx_nurses_station_id"
    ON "public"."nurses" ("station", "id");

CREATE INDEX "idx_station_department_id"
    ON "public"."station" ("department", "id");

CREATE INDEX "idx_rooms_station_id"
    ON "public"."rooms" ("station", "id");

CREATE UNIQUE INDEX "idx_rooms_station_number"
    ON "public"."rooms" ("station", "number");

CREATE INDEX "idx_bookings_patient_from_id"
    ON "public"."bookings" ("patient", "from" DESC, "id" DESC);

CREATE INDEX "idx_bookings_room_from_until"
    ON "public"."bookings" ("room", "from", "until");

CREATE INDEX "idx_bookings_state_from_id"
    ON "public"."bookings" ("state", "from", "id");

CREATE INDEX "idx_bookings_from_id"
    ON "public"."bookings" ("from", "id");

CREATE INDEX "idx_dose_unit_frequency"
    ON "public"."dose" ("unit", "frequency");

CREATE INDEX "idx_drugs_type_id"
    ON "public"."drugs" ("type", "id");

CREATE INDEX "idx_drugs_name"
    ON "public"."drugs" ("name");
