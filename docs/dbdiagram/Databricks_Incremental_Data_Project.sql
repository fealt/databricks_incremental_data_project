CREATE TABLE "fact_bookings" (
  "booking_id" "VARCHAR(50)" PRIMARY KEY,
  "passenger_id" "VARCHAR(50)" NOT NULL,
  "flight_id" "VARCHAR(50)" NOT NULL,
  "airport_id" "VARCHAR(50)" NOT NULL,
  "amount" "DECIMAL(10,2)",
  "booking_date" DATE
);

CREATE TABLE "dim_passengers" (
  "passenger_id" "VARCHAR(50)" PRIMARY KEY,
  "name" "VARCHAR(100)",
  "gender" "VARCHAR(10)",
  "nationality" "VARCHAR(50)"
);

CREATE TABLE "dim_flights" (
  "flight_id" "VARCHAR(50)" PRIMARY KEY,
  "airline" "VARCHAR(100)",
  "origin" "VARCHAR(100)",
  "destination" "VARCHAR(100)",
  "flight_date" DATE
);

CREATE TABLE "dim_airports" (
  "airport_id" "VARCHAR(50)" PRIMARY KEY,
  "airport_name" "VARCHAR(100)",
  "city" "VARCHAR(50)",
  "country" "VARCHAR(50)"
);

ALTER TABLE "fact_bookings" ADD FOREIGN KEY ("passenger_id") REFERENCES "dim_passengers" ("passenger_id");

ALTER TABLE "fact_bookings" ADD FOREIGN KEY ("flight_id") REFERENCES "dim_flights" ("flight_id");

ALTER TABLE "fact_bookings" ADD FOREIGN KEY ("airport_id") REFERENCES "dim_airports" ("airport_id");
