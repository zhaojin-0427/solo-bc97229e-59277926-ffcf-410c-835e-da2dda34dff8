-- Fails regardless of CSV content: the script always inserts the seeded
-- sample_id again, colliding with the row already present in the baseline.
-- Whether staging holds 0 rows or 40 rows, the same PRIMARY KEY error is
-- raised, proving the fault lives in the script/baseline, not the CSV.
DROP TABLE IF EXISTS staging_snapshot;
CREATE TABLE staging_snapshot AS SELECT * FROM staging;
INSERT INTO instrument_readings (sample_id, reading) VALUES ('S-DEFAULT', '0');
