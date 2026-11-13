DROP TABLE IF EXISTS instrument_readings;
CREATE TABLE instrument_readings (
    sample_id TEXT PRIMARY KEY,
    reading   TEXT
);
INSERT INTO instrument_readings (sample_id, reading)
SELECT sample_id, reading FROM staging ORDER BY rowid;
