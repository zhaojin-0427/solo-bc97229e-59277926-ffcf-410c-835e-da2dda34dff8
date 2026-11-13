"""End-to-end tests for the Flask web layer."""

import io
import json
import os
import shutil
import sqlite3
import tempfile
import time
import unittest

from app.web import create_app

CHECK_SQL = """\
DROP TABLE IF EXISTS instrument_readings;
CREATE TABLE instrument_readings (
    sample_id TEXT PRIMARY KEY,
    reading   TEXT NOT NULL CHECK (reading GLOB '[0-9]*' AND reading NOT GLOB '*[^0-9]*')
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


class WebJobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="web-test-")
        os.environ["JOBS_DIR"] = os.path.join(self.tmp, "jobs")
        os.environ["PROBE_TIMEOUT_SECONDS"] = "2.0"
        self.app = create_app()
        self.client = self.app.test_client()

        self.baseline = os.path.join(self.tmp, "baseline.db")
        conn = sqlite3.connect(self.baseline)
        conn.execute("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT)")
        conn.commit()
        conn.close()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _csv_bytes(self, rows):
        buf = io.BytesIO()
        buf.write("sample_id,reading\n".encode())
        for sid, reading in rows:
            buf.write(f"{sid},{reading}\n".encode())
        buf.seek(0)
        return buf

    def _submit(self, rows, sql=CHECK_SQL, baseline_path=None):
        with open(baseline_path or self.baseline, "rb") as fh:
            baseline_bytes = fh.read()
        return self.client.post(
            "/api/jobs",
            data={
                "baseline": (io.BytesIO(baseline_bytes), "baseline.db"),
                "csvfile": (self._csv_bytes(rows), "input.csv"),
                "script": (io.BytesIO(sql.encode()), "script.sql"),
            },
            content_type="multipart/form-data",
        )

    def _wait(self, job_id, max_seconds=10):
        deadline = time.monotonic() + max_seconds
        while time.monotonic() < deadline:
            resp = self.client.get(f"/api/jobs/{job_id}")
            data = resp.get_json()
            if data["state"] == "done":
                return data
            time.sleep(0.1)
        self.fail("job did not finish in time")

    def test_happy_path_reduction_and_download(self):
        rows = [(f"S-{i:03d}", str(1000 + i)) for i in range(20)]
        rows.insert(8, ("S-BAD", "N/A"))
        resp = self._submit(rows)
        self.assertEqual(resp.status_code, 201)
        job_id = resp.get_json()["job_id"]

        data = self._wait(job_id)
        result = data["result"]
        self.assertEqual(result["status"], "reduced")
        self.assertEqual(result["reduced_rows"], [["S-BAD", "N/A"]])
        self.assertEqual(result["target"]["error_code"], 275)
        self.assertEqual(result["probe_timeout_count"], 0)

        dl = self.client.get(f"/api/jobs/{job_id}/download")
        self.assertEqual(dl.status_code, 200)
        self.assertEqual(
            dl.data.decode(), "sample_id,reading\r\nS-BAD,N/A\r\n"
        )
        self.assertEqual(
            dl.headers["Content-Disposition"],
            "attachment; filename=reduced_sample.csv",
        )

        # Baseline stored on disk must remain the pristine upload.
        conn = sqlite3.connect(
            "file:" + os.path.join(os.environ["JOBS_DIR"], job_id,
                                   "baseline.db") + "?mode=ro",
            uri=True,
        )
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        self.assertEqual(tables, {"meta"})  # no staging / instrument_readings

    def test_success_batch_is_explained(self):
        resp = self._submit([("A", "1"), ("B", "2")])
        data = self._wait(resp.get_json()["job_id"])
        self.assertEqual(data["result"]["status"], "no_error")
        self.assertEqual(data["result"]["target"]["kind"], "success")
        # No downloadable sample when nothing was reduced.
        self.assertEqual(
            self.client.get(
                f"/api/jobs/{resp.get_json()['job_id']}/download"
            ).status_code,
            404,
        )

    def test_timeout_job_is_incomplete(self):
        resp = self._submit([("T", "1"), ("T", "2")], sql=TIMEOUT_SQL)
        data = self._wait(resp.get_json()["job_id"], max_seconds=8)
        result = data["result"]
        self.assertEqual(result["status"], "incomplete_timeout")
        self.assertGreaterEqual(result["probe_timeout_count"], 1)
        self.assertEqual(result["target"]["kind"], "timeout")

    def test_validation_errors(self):
        # wrong header
        bad = io.BytesIO(b"id,val\nA,1\n")
        with open(self.baseline, "rb") as fh:
            baseline_bytes = fh.read()
        resp = self.client.post(
            "/api/jobs",
            data={
                "baseline": (io.BytesIO(baseline_bytes), "baseline.db"),
                "csvfile": (bad, "input.csv"),
                "script": (io.BytesIO(b"SELECT 1;"), "script.sql"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("表头", resp.get_json()["error"])

        # 41 data rows
        resp = self._submit([(f"R{i}", str(i)) for i in range(41)])
        self.assertEqual(resp.status_code, 400)
        self.assertIn("40", resp.get_json()["error"])

        # header only
        empty = io.BytesIO(b"sample_id,reading\n")
        resp = self.client.post(
            "/api/jobs",
            data={
                "baseline": (io.BytesIO(baseline_bytes), "baseline.db"),
                "csvfile": (empty, "input.csv"),
                "script": (io.BytesIO(b"SELECT 1;"), "script.sql"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 400)

        # invalid baseline file
        fake = os.path.join(self.tmp, "fake.db")
        with open(fake, "w") as fh:
            fh.write("not a database")
        resp = self._submit([("A", "1")], baseline_path=fake)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("SQLite", resp.get_json()["error"])

        # missing field
        resp = self.client.post(
            "/api/jobs",
            data={"csvfile": (self._csv_bytes([("A", "1")]), "input.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 400)

    def test_attach_script_rejected(self):
        with open(self.baseline, "rb") as fh:
            baseline_bytes = fh.read()
        resp = self.client.post(
            "/api/jobs",
            data={
                "baseline": (io.BytesIO(baseline_bytes), "baseline.db"),
                "csvfile": (self._csv_bytes([("A", "1")]), "input.csv"),
                "script": (io.BytesIO(
                    b"ATTACH DATABASE '/tmp/evil.db' AS evil;"), "script.sql"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("ATTACH", resp.get_json()["error"])

    def test_pages_and_404s(self):
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/healthz").status_code, 200)
        self.assertEqual(self.client.get("/jobs/deadbeef").status_code, 404)
        self.assertEqual(
            self.client.get("/api/jobs/deadbeef").status_code, 404
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
