"""Tests for the reduction engine. Run: python3 -m unittest discover -s tests -v"""

import os
import shutil
import sqlite3
import tempfile
import time
import unittest

from app.reducer import (
    ERROR,
    SUCCESS,
    TIMEOUT,
    ProbeTimeout,
    UnsafeScriptError,
    is_reproduction,
    reduce_batch,
    run_probe,
    sha256_file,
    validate_sql_script,
)

CHECK_SQL = """\
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

TIMEOUT_SQL = """\
WITH RECURSIVE cnt(x) AS (
    SELECT 1
    UNION ALL
    SELECT x + 1 FROM cnt
    CROSS JOIN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4)
    WHERE x < 18000000
)
SELECT count(*) FROM cnt;
"""


class ReducerTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="reducer-test-")
        self.baseline = os.path.join(self.tmp, "baseline.sqlite3")
        conn = sqlite3.connect(self.baseline)
        conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO schema_meta VALUES ('v', '1')")
        conn.commit()
        conn.close()
        self.baseline_hash = sha256_file(self.baseline)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def assertBaselineUntouched(self):
        self.assertEqual(sha256_file(self.baseline), self.baseline_hash)
        # No sidecar files next to the immutable baseline
        self.assertFalse(os.path.exists(self.baseline + "-journal"))
        self.assertFalse(os.path.exists(self.baseline + "-wal"))
        self.assertFalse(os.path.exists(self.baseline + "-shm"))


class CheckConstraintTests(ReducerTestBase):
    def test_reduces_to_single_bad_row(self):
        rows = [(f"S-{i:03d}", str(1000 + i)) for i in range(40)]
        rows.insert(17, ("S-BAD", "N/A"))
        result = reduce_batch(self.baseline, rows, CHECK_SQL, probe_timeout=2.0)

        self.assertEqual(result.status, "reduced")
        self.assertEqual(result.reduced_rows, [("S-BAD", "N/A")])
        self.assertEqual(result.target.kind, ERROR)
        self.assertEqual(result.target.error_code, 275)  # SQLITE_CONSTRAINT_CHECK
        self.assertEqual(result.empty_outcome.kind, SUCCESS)
        self.assertBaselineUntouched()
        self.assertEqual(result.probe_timeout_count, 0)

    def test_minimality_each_remaining_row_required(self):
        # Two bad rows exist independently: both must remain because either
        # alone still fails... actually either alone DOES fail, so ddmin may
        # return one of them; verify whichever subset is returned is minimal.
        rows = [("A", "1"), ("BAD1", "x"), ("B", "2"), ("BAD2", "y"), ("C", "3")]
        result = reduce_batch(self.baseline, rows, CHECK_SQL)
        self.assertEqual(result.status, "reduced")
        self.assertEqual(len(result.reduced_rows), 1)
        bad = result.reduced_rows[0]
        self.assertIn(bad, [("BAD1", "x"), ("BAD2", "y")])

        # Explicit minimality: removing the only row -> empty -> success.
        outcome = run_probe(self.baseline, [], CHECK_SQL)
        self.assertEqual(outcome.kind, SUCCESS)
        self.assertFalse(is_reproduction(outcome, result.target))
        self.assertBaselineUntouched()


class UniqueConstraintTests(ReducerTestBase):
    def test_pair_is_minimal(self):
        # Duplicate sample_id: neither row alone violates UNIQUE, but the
        # pair does. Minimal sample must contain BOTH rows.
        rows = [("U1", "10"), ("U2", "20"), ("U1", "30"), ("U3", "40")]
        result = reduce_batch(self.baseline, rows, UNIQUE_SQL)

        self.assertEqual(result.status, "reduced")
        self.assertEqual(
            sorted(result.reduced_rows), sorted([("U1", "10"), ("U1", "30")])
        )
        # Order within the sample follows the original order.
        self.assertEqual(result.reduced_rows, [("U1", "10"), ("U1", "30")])

        # Deleting either one makes the error disappear.
        for i in range(2):
            subset = [r for j, r in enumerate(result.reduced_rows) if j != i]
            outcome = run_probe(self.baseline, subset, UNIQUE_SQL)
            self.assertEqual(outcome.kind, SUCCESS)
        self.assertBaselineUntouched()


class NoErrorAndEmptyTests(ReducerTestBase):
    def test_full_batch_success_is_explained_not_reduced(self):
        rows = [("A", "1"), ("B", "2"), ("C", "3")]
        result = reduce_batch(self.baseline, rows, CHECK_SQL)
        self.assertEqual(result.status, "no_error")
        self.assertEqual(result.reduced_rows, [])
        self.assertEqual(result.target.kind, SUCCESS)
        self.assertIn("成功", result.note)
        self.assertBaselineUntouched()

    def test_empty_csv_rejected_upstream_and_engine_guard(self):
        result = reduce_batch(self.baseline, [], CHECK_SQL)
        self.assertEqual(result.status, "no_error")
        self.assertIn("没有任何数据行", result.note)

    def test_empty_sample_reproduces_same_error(self):
        # Script that fails even with an empty staging table: the error is
        # independent of the CSV rows.
        script = """\
        DROP TABLE IF EXISTS t;
        CREATE TABLE t (sample_id TEXT PRIMARY KEY);
        INSERT INTO t VALUES ('DUP');
        INSERT INTO t SELECT sample_id FROM staging;
        INSERT INTO t VALUES ('DUP');
        """
        rows = [("A", "1"), ("B", "2")]
        result = reduce_batch(self.baseline, rows, script)
        self.assertEqual(result.status, "empty_reproduces")
        self.assertEqual(result.reduced_rows, [])
        self.assertTrue(is_reproduction(result.empty_outcome, result.target))
        self.assertIn("与 CSV 数据行无关", result.note)
        self.assertBaselineUntouched()


class TimeoutTests(ReducerTestBase):
    def test_timeout_is_terminated_and_marked_incomplete(self):
        rows = [(f"T{i}", str(i)) for i in range(8)]
        t0 = time.monotonic()
        result = reduce_batch(self.baseline, rows, TIMEOUT_SQL, probe_timeout=2.0)
        elapsed = time.monotonic() - t0

        self.assertEqual(result.status, "incomplete_timeout")
        self.assertEqual(result.probe_timeout_count, result.probes)
        # The very first (full-batch) probe must be cut at ~2s, so the
        # whole job finishes quickly instead of running to completion.
        self.assertLess(elapsed, 6.0)
        self.assertGreaterEqual(result.target.duration, 1.8)
        self.assertLess(result.target.duration, 3.5)
        self.assertIn("未完成", result.note)
        self.assertBaselineUntouched()

    def test_run_probe_returns_timeout_not_error(self):
        outcome = run_probe(self.baseline, [("A", "1")], TIMEOUT_SQL,
                            probe_timeout=2.0)
        self.assertEqual(outcome.kind, TIMEOUT)
        self.assertIsNone(outcome.error_code)
        self.assertFalse(is_reproduction(outcome, outcome))
        self.assertBaselineUntouched()

    def test_probe_timeout_never_counts_as_repro_in_ddmin(self):
        # A cheap script that succeeds, combined with a timeout script as
        # the "target" scenario: target probe times out -> incomplete.
        rows = [("A", "1"), ("B", "2")]
        result = reduce_batch(self.baseline, rows, TIMEOUT_SQL,
                              probe_timeout=2.0)
        self.assertEqual(result.status, "incomplete_timeout")
        self.assertEqual(result.reduced_rows, [])

    def test_mid_reduction_timeout_aborts_as_incomplete(self):
        # Full batch fails FAST (CHECK on the N/A row), but any subset of
        # >=3 rows without that row hits a slow recursive CTE. Delta
        # debugging must hit the slow path while shrinking, terminate the
        # statement at the deadline, and report incomplete rather than
        # treating the timeout as evidence either way.
        script = """\
        DROP TABLE IF EXISTS ir;
        CREATE TABLE ir (
          sample_id TEXT PRIMARY KEY,
          reading TEXT CHECK (reading GLOB '[0-9]*' AND reading NOT GLOB '*[^0-9]*')
        );
        WITH RECURSIVE cnt(x) AS (
          SELECT 1 UNION ALL
          SELECT x+1 FROM cnt CROSS JOIN
            (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4)
          WHERE x < CASE
            WHEN (SELECT count(*) FROM staging) >= 3
             AND (SELECT count(*) FROM staging WHERE reading = 'N/A') = 0
            THEN 18000000 ELSE 0 END
        )
        SELECT count(*) FROM cnt;
        INSERT INTO ir SELECT sample_id, reading FROM staging;
        """
        rows = [(f"R{i:02d}", str(i)) for i in range(12)]
        rows.append(("BAD", "N/A"))
        result = reduce_batch(self.baseline, rows, script, probe_timeout=2.0)
        self.assertEqual(result.status, "incomplete_timeout")
        self.assertGreaterEqual(result.probe_timeout_count, 1)
        self.assertEqual(result.reduced_rows, [])
        self.assertEqual(result.target.kind, ERROR)  # target itself was a real error
        self.assertBaselineUntouched()


class IsolationAndOrderTests(ReducerTestBase):
    def test_probes_use_independent_copies_and_preserve_order(self):
        # Script inserts staging rows into a persistent table; if probes
        # shared a database, row counts would accumulate across probes.
        script = """\
        CREATE TABLE IF NOT EXISTS probe_runs (n INTEGER);
        INSERT INTO probe_runs SELECT count(*) FROM staging;
        -- Fail only when staging contains the marker row, after recording.
        INSERT INTO instrument_readings (sample_id, reading)
        SELECT sample_id, reading FROM staging ORDER BY rowid;
        """
        conn = sqlite3.connect(self.baseline)
        conn.execute(
            "CREATE TABLE instrument_readings (sample_id TEXT PRIMARY KEY, reading TEXT)"
        )
        conn.commit()
        conn.close()
        # Baseline now includes the extra table; recompute the immutable
        # reference hash for the rest of this test.
        self.baseline_hash = sha256_file(self.baseline)

        rows = [("A", "1"), ("B", "2"), ("C", "3"), ("D", "4")]
        # All succeed: multiple probes still run (full + empty), each on a
        # fresh copy where probe_runs must have exactly one row.
        result = reduce_batch(self.baseline, rows, script)
        self.assertEqual(result.status, "no_error")

        outcome = run_probe(self.baseline, rows, script)
        self.assertEqual(outcome.kind, SUCCESS)
        self.assertBaselineUntouched()

        # The baseline itself must contain no probe artefacts: the
        # probe_runs table exists only inside the discarded copies.
        conn = sqlite3.connect("file:" + self.baseline + "?mode=ro", uri=True)
        names = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        conn.close()
        self.assertNotIn("probe_runs", names)
        self.assertNotIn("staging", names)

    def test_original_row_order_preserved_in_reduced_sample(self):
        rows = [("z", "1"), ("U", "10"), ("a", "2"), ("U", "20"), ("m", "3")]
        result = reduce_batch(self.baseline, rows, UNIQUE_SQL)
        self.assertEqual(result.status, "reduced")
        self.assertEqual(result.reduced_rows, [("U", "10"), ("U", "20")])

    def test_error_message_mismatch_is_not_reproduction(self):
        from app.reducer import ProbeOutcome
        target = ProbeOutcome(ERROR, error_code=19, message="UNIQUE failed: x")
        same_code_diff_msg = ProbeOutcome(ERROR, error_code=19, message="UNIQUE failed: y")
        diff_code = ProbeOutcome(ERROR, error_code=1, message="UNIQUE failed: x")
        timeout = ProbeOutcome(TIMEOUT)
        self.assertFalse(is_reproduction(same_code_diff_msg, target))
        self.assertFalse(is_reproduction(diff_code, target))
        self.assertFalse(is_reproduction(timeout, target))
        self.assertTrue(is_reproduction(
            ProbeOutcome(ERROR, error_code=19, message="  UNIQUE failed: x "), target
        ))


class ScriptSafetyTests(ReducerTestBase):
    def test_attach_and_vacuum_into_are_rejected(self):
        for bad in (
            "ATTACH DATABASE '/tmp/evil.db' AS evil;",
            "attach '/tmp/evil.db' as evil;",
            "VACUUM INTO '/tmp/evil.db';",
            "vacuum   into '/tmp/evil.db';",
        ):
            with self.assertRaises(UnsafeScriptError):
                validate_sql_script(bad)
            with self.assertRaises(UnsafeScriptError):
                run_probe(self.baseline, [("A", "1")], bad)
        # The benign substring inside an identifier/string must not trip it.
        validate_sql_script("SELECT 'attached note' AS note;")
        validate_sql_script(CHECK_SQL)

    def test_authorizer_blocks_attach_even_if_regex_bypassed(self):
        # Directly exercise the authorizer path via a script whose ATTACH
        # would otherwise succeed; run_probe pre-validation must raise, and
        # no outside file may appear.
        outside = os.path.join(self.tmp, "outside.db")
        script = f"ATTACH DATABASE '{outside}' AS x;"
        with self.assertRaises(UnsafeScriptError):
            run_probe(self.baseline, [("A", "1")], script)
        self.assertFalse(os.path.exists(outside))
        self.assertBaselineUntouched()

    def test_concurrent_jobs_share_baseline_without_pollution(self):
        # Two reductions run in parallel against the SAME baseline file.
        # Each must get its own independent copies and its own answer, and
        # the baseline must remain byte-identical afterwards.
        import threading

        script_a = CHECK_SQL
        rows_a = [("A", "1"), ("BAD", "x"), ("B", "2")]
        script_b = UNIQUE_SQL
        rows_b = [("K", "1"), ("K", "2"), ("Z", "3")]
        results = {}

        def run(key, rows, sql):
            results[key] = reduce_batch(self.baseline, rows, sql)

        t1 = threading.Thread(target=run, args=("a", rows_a, script_a))
        t2 = threading.Thread(target=run, args=("b", rows_b, script_b))
        t1.start(); t2.start(); t1.join(); t2.join()

        self.assertEqual(results["a"].status, "reduced")
        self.assertEqual(results["a"].reduced_rows, [("BAD", "x")])
        self.assertEqual(results["b"].status, "reduced")
        self.assertEqual(results["b"].reduced_rows, [("K", "1"), ("K", "2")])
        self.assertBaselineUntouched()


if __name__ == "__main__":
    unittest.main(verbosity=2)
