"""Flask web application for SQLite CSV error reduction."""

from __future__ import annotations

import csv
import io
import os
import secrets
import sqlite3
import threading
import time
from dataclasses import asdict
from typing import Dict, List, Optional, Tuple

from flask import (
    Flask,
    abort,
    jsonify,
    render_template,
    request,
    send_file,
)

from .reducer import (
    ERROR,
    ProbeOutcome,
    ReductionResult,
    UnsafeScriptError,
    reduce_batch,
    sha256_file,
    validate_sql_script,
)

MAX_CSV_ROWS = 40
MAX_CONTENT_LENGTH = 60 * 1024 * 1024  # 60 MB total request
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT_SECONDS", "2.0"))


def _parse_csv(text: str) -> List[Tuple[str, str]]:
    """Parse an uploaded CSV. Exactly two text columns named
    ``sample_id`` and ``reading``; at most 40 data rows; original order is
    preserved."""
    if text.startswith("﻿"):
        text = text[1:]
    reader = csv.reader(io.StringIO(text))
    rows: List[Tuple[str, str]] = []
    header: Optional[List[str]] = None
    for raw in reader:
        if not raw or all(cell.strip() == "" for cell in raw):
            # Blank separator line: tolerated, not counted.
            continue
        if header is None:
            header = [c.strip() for c in raw]
            if header != ["sample_id", "reading"]:
                raise ValueError(
                    "CSV 表头必须恰好为 sample_id,reading（两列文本），"
                    f"实际为：{','.join(header)}"
                )
            continue
        if len(raw) != 2:
            raise ValueError(
                f"第 {len(rows) + 1} 行有 {len(raw)} 列，每行必须恰好为两列。"
            )
        rows.append((raw[0], raw[1]))
        if len(rows) > MAX_CSV_ROWS:
            raise ValueError(f"CSV 数据行最多 {MAX_CSV_ROWS} 行。")
    if header is None:
        raise ValueError("CSV 内容为空或缺少表头 sample_id,reading。")
    if not rows:
        raise ValueError(
            "CSV 只有表头、没有任何数据行；空样本无法缩减，请至少提供 1 行数据。"
        )
    return rows


def _validate_baseline(path: str) -> None:
    """Open the baseline strictly read-only. This never writes the file
    (mode=ro, immutable=1 prevents sidecar files too)."""
    uri = "file:" + path + "?mode=ro&immutable=1"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        # Querying sqlite_schema forces SQLite to parse the database
        # header, so an arbitrary file is rejected here rather than later.
        conn.execute("SELECT count(*) FROM sqlite_schema").fetchone()
    except sqlite3.Error as exc:
        raise ValueError(f"基线不是有效的 SQLite 数据库：{exc}") from exc
    finally:
        conn.close()


def _outcome_dict(outcome: Optional[ProbeOutcome]) -> Optional[dict]:
    if outcome is None:
        return None
    return {
        "kind": outcome.kind,
        "error_code": outcome.error_code,
        "error_name": outcome.error_name,
        "message": outcome.message,
        "duration": round(outcome.duration, 4),
        "n_rows": outcome.n_rows,
    }


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

    jobs_dir = os.environ.get(
        "JOBS_DIR", os.path.join(os.getcwd(), "instance", "jobs")
    )
    os.makedirs(jobs_dir, exist_ok=True)

    jobs: Dict[str, dict] = {}
    jobs_lock = threading.Lock()

    def _job_dir(job_id: str) -> str:
        d = os.path.join(jobs_dir, job_id)
        os.makedirs(d, exist_ok=True)
        return d

    def _worker(job_id: str, baseline_path: str, rows, script: str) -> None:
        events: List[dict] = []

        def report(event: dict) -> None:
            event = dict(event)
            event["ts"] = round(time.monotonic() - job["started_at"], 3)
            with jobs_lock:
                events.append(event)
                job["events"] = events[-300:]
                job["progress"] = {
                    "phase": event["phase"],
                    "message": event["message"],
                    "probe": event["probe"],
                    "current_size": event["current_size"],
                }

        with jobs_lock:
            job = jobs[job_id]
        try:
            result: ReductionResult = reduce_batch(
                baseline_path,
                rows,
                script,
                probe_timeout=PROBE_TIMEOUT,
                report=report,
            )
            with jobs_lock:
                job["result"] = result
                job["state"] = "done"
                if result.status == "reduced":
                    out = os.path.join(_job_dir(job_id), "reduced.csv")
                    with open(out, "w", newline="", encoding="utf-8") as fh:
                        writer = csv.writer(fh)
                        writer.writerow(["sample_id", "reading"])
                        writer.writerows(result.reduced_rows)
                    job["reduced_csv"] = out
        except Exception as exc:  # pragma: no cover - defensive
            with jobs_lock:
                job["state"] = "done"
                job["fatal"] = repr(exc)

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/jobs/<job_id>")
    def job_page(job_id: str):
        with jobs_lock:
            if job_id not in jobs:
                abort(404)
        return render_template("index.html", job_id=job_id)

    @app.post("/api/jobs")
    def create_job():
        baseline = request.files.get("baseline")
        csv_file = request.files.get("csvfile")
        script_file = request.files.get("script")
        if baseline is None or csv_file is None or script_file is None:
            return (
                jsonify(
                    {
                        "error": "需要同时上传三个文件：baseline(SQLite)、"
                        "csvfile(CSV)、script(SQL)。"
                    }
                ),
                400,
            )
        if not baseline.filename.lower().endswith(
            (".db", ".sqlite", ".sqlite3", ".db3")
        ):
            return jsonify({"error": "基线文件扩展名应为 .db/.sqlite/.sqlite3。"}), 400
        if not script_file.filename.lower().endswith(".sql"):
            return jsonify({"error": "SQL 脚本扩展名应为 .sql。"}), 400

        try:
            rows = _parse_csv(csv_file.read().decode("utf-8-sig"))
        except UnicodeDecodeError:
            return jsonify({"error": "CSV 必须为 UTF-8 文本编码。"}), 400
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        try:
            script = script_file.read().decode("utf-8-sig")
        except UnicodeDecodeError:
            return jsonify({"error": "SQL 脚本必须为 UTF-8 文本编码。"}), 400
        if not script.strip():
            return jsonify({"error": "SQL 脚本不能为空。"}), 400
        try:
            validate_sql_script(script)
        except UnsafeScriptError as exc:
            return jsonify({"error": str(exc)}), 400

        job_id = secrets.token_hex(12)
        d = _job_dir(job_id)
        baseline_path = os.path.join(d, "baseline.db")
        baseline.save(baseline_path)
        try:
            _validate_baseline(baseline_path)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        with open(os.path.join(d, "input.csv"), "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["sample_id", "reading"])
            writer.writerows(rows)
        with open(os.path.join(d, "script.sql"), "w", encoding="utf-8") as fh:
            fh.write(script)

        with jobs_lock:
            jobs[job_id] = {
                "state": "running",
                "started_at": time.monotonic(),
                "wall_started": time.time(),
                "input_rows": len(rows),
                "baseline_sha256": sha256_file(baseline_path),
                "progress": {"phase": "queued", "message": "任务已创建，等待执行 …"},
                "events": [],
                "result": None,
                "reduced_csv": None,
                "fatal": None,
            }

        thread = threading.Thread(
            target=_worker,
            args=(job_id, baseline_path, rows, script),
            name=f"reduce-{job_id}",
            daemon=True,
        )
        thread.start()
        return jsonify({"job_id": job_id}), 201

    @app.get("/api/jobs/<job_id>")
    def get_job(job_id: str):
        with jobs_lock:
            job = jobs.get(job_id)
            if job is None:
                return jsonify({"error": "任务不存在。"}), 404
            result: Optional[ReductionResult] = job["result"]
            payload = {
                "job_id": job_id,
                "state": job["state"],
                "input_rows": job["input_rows"],
                "baseline_sha256": job["baseline_sha256"],
                "progress": job["progress"],
                "events": list(job["events"]),
                "fatal": job.get("fatal"),
                "probe_timeout_seconds": PROBE_TIMEOUT,
            }
            if result is not None:
                payload["result"] = {
                    "status": result.status,
                    "note": result.note,
                    "probes": result.probes,
                    "probe_timeout_count": result.probe_timeout_count,
                    "elapsed": result.elapsed,
                    "target": _outcome_dict(result.target),
                    "empty_outcome": _outcome_dict(result.empty_outcome),
                    "reduced_rows": result.reduced_rows,
                    "reduced_count": len(result.reduced_rows),
                }
        return jsonify(payload)

    @app.get("/api/jobs/<job_id>/download")
    def download(job_id: str):
        with jobs_lock:
            job = jobs.get(job_id)
            if job is None:
                abort(404)
            path = job.get("reduced_csv")
        if not path or not os.path.exists(path):
            abort(404)
        return send_file(
            path,
            mimetype="text/csv",
            as_attachment=True,
            download_name="reduced_sample.csv",
        )

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok"})

    return app
