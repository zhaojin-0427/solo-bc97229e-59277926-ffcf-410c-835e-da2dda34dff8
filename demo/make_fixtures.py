#!/usr/bin/env python3
"""Generate demo fixtures under demo/.

Scenarios
---------
1. check_constraint/  : a single malformed reading violates a CHECK
   constraint -> expected minimal sample: 1 row.
2. unique_duplicate/  : only the pair of duplicate rows violates a UNIQUE
   constraint -> expected minimal sample: 2 rows.
3. success_batch/     : all rows are valid; the full batch succeeds, so
   the service must explain and refuse to reduce.
4. empty_reproduces/  : the script itself raises on an empty staging table
   (UNIQUE on a seeded default row), so even 0 CSV rows fail with the same
   error -> explain, do not reduce.

Run:  python3 demo/make_fixtures.py
"""

import os
import sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))

INGEST_SQL = """\
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
"""

UNIQUE_SQL = """\
DROP TABLE IF EXISTS instrument_readings;
CREATE TABLE instrument_readings (
    sample_id TEXT PRIMARY KEY,
    reading   TEXT
);
INSERT INTO instrument_readings (sample_id, reading)
SELECT sample_id, reading FROM staging ORDER BY rowid;
"""

EMPTY_FAIL_SQL = """\
-- Fails regardless of CSV content: the script always inserts the seeded
-- sample_id again, colliding with the row already present in the baseline.
-- Whether staging holds 0 rows or 40 rows, the same PRIMARY KEY error is
-- raised, proving the fault lives in the script/baseline, not the CSV.
DROP TABLE IF EXISTS staging_snapshot;
CREATE TABLE staging_snapshot AS SELECT * FROM staging;
INSERT INTO instrument_readings (sample_id, reading) VALUES ('S-DEFAULT', '0');
"""

TIMEOUT_SQL = """\
-- Expensive recursive CTE: with the 2-second per-probe limit every probe
-- is interrupted and the job must be marked incomplete.
WITH RECURSIVE cnt(x) AS (
    SELECT 1
    UNION ALL
    SELECT x + 1 FROM cnt
    CROSS JOIN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
                UNION ALL SELECT 4)
    WHERE x < 18000000
)
SELECT count(*) FROM cnt;
"""


def write_baseline(path, seed_default=False):
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute(
        "INSERT INTO schema_meta VALUES ('baseline_version', 'demo-1')"
    )
    if seed_default:
        conn.execute(
            "CREATE TABLE instrument_readings (sample_id TEXT PRIMARY KEY, reading TEXT)"
        )
        conn.execute("INSERT INTO instrument_readings VALUES ('S-DEFAULT', '0')")
    conn.commit()
    conn.close()


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("sample_id,reading\n")
        for sid, reading in rows:
            fh.write(f"{sid},{reading}\n")


def make_scenario(name, sql, rows, *, seed_default=False):
    d = os.path.join(HERE, name)
    os.makedirs(d, exist_ok=True)
    write_baseline(os.path.join(d, "baseline.sqlite3"), seed_default=seed_default)
    write_csv(os.path.join(d, "input.csv"), rows)
    with open(os.path.join(d, "script.sql"), "w", encoding="utf-8") as fh:
        fh.write(sql)


def main():
    # 1. CHECK constraint: 39 valid rows + one malformed reading (40 total).
    rows = [(f"S-{i:03d}", str(1000 + i)) for i in range(39)]
    rows.insert(17, ("S-BAD", "N/A"))  # malformed, keeps its position
    make_scenario("check_constraint", INGEST_SQL, rows)

    # 2. UNIQUE constraint: duplicate sample_id buried among valid rows.
    rows2 = [(f"U-{i:03d}", str(2000 + i)) for i in range(20)]
    rows2.insert(9, ("U-005", "9999"))  # duplicate of an earlier sample_id
    make_scenario("unique_duplicate", UNIQUE_SQL, rows2)

    # 3. All-valid batch: no error expected.
    rows3 = [(f"OK-{i:03d}", str(3000 + i)) for i in range(10)]
    make_scenario("success_batch", INGEST_SQL, rows3)

    # 4. Script fails even on an empty staging table.
    rows4 = [("E-001", "42"), ("E-002", "43"), ("E-003", "44")]
    make_scenario(
        "empty_reproduces", EMPTY_FAIL_SQL, rows4, seed_default=True
    )

    # 5. Timeout scenario: every probe exceeds the 2-second budget.
    rows5 = [(f"T-{i:03d}", str(i)) for i in range(8)]
    make_scenario("timeout_probe", TIMEOUT_SQL, rows5)

    print("Demo fixtures written under", HERE)
    for name in sorted(os.listdir(HERE)):
        p = os.path.join(HERE, name)
        if os.path.isdir(p):
            print(" -", name, ":", sorted(os.listdir(p)))


if __name__ == "__main__":
    main()
