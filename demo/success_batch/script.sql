-- Deterministic ingestion script.
-- staging(sample_id TEXT, reading TEXT) is (re)created by the service
-- before every probe, with the candidate rows in original order.
DROP TABLE IF EXISTS instrument_readings;
CREATE TABLE instrument_readings (
    sample_id TEXT PRIMARY KEY,
    reading   TEXT NOT NULL CHECK (reading GLOB '[0-9]*' AND reading NOT GLOB '*[^0-9]*')
);
INSERT INTO instrument_readings (sample_id, reading)
SELECT sample_id, reading FROM staging ORDER BY rowid;
