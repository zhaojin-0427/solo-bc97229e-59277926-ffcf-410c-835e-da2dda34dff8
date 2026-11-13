"""SQLite CSV delta-reduction engine.

Given:
  * a read-only SQLite baseline database,
  * a CSV batch (``sample_id,reading`` text columns) whose ingestion fails,
  * a deterministic SQL script that builds its own tables from a staging
    table ``staging(sample_id TEXT, reading TEXT)``,

this module finds a *minimal* subset of rows that still raises the exact
same SQLite error (same primary error code and same message) as the full
batch.

Design guarantees required by the specification:

* Every probe starts from an **independent copy** of the baseline inside a
  private temporary directory. Writes can therefore never reach the
  uploaded baseline or another probe's database.
* Each probe is limited to ``probe_timeout`` seconds (default 2). Timeouts
  are enforced by actually terminating the running SQLite statement via
  ``sqlite3.Connection.interrupt()`` (both a progress-handler and a
  watchdog thread call it). A timeout is never interpreted as the target
  error nor as minimality evidence; the reduction is marked incomplete.
* The result is verified: the reduced batch must reproduce the target
  error, and removing any single remaining row must stop reproducing it.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence, Tuple

# Statements that could make the script write outside its private database
# copy. They are rejected up-front and additionally blocked at authorizer
# level, so every SQL write stays inside the per-probe copy.
_FORBIDDEN_SQL_RE = re.compile(
    r"\battach\s+(?:database\s+)?|vacuum\s+into\b", re.IGNORECASE
)


class UnsafeScriptError(ValueError):
    """Raised when a script could write outside its private probe copy."""


def validate_sql_script(sql_script: str) -> None:
    """Reject constructs that escape the single connected database file
    (ATTACH DATABASE / VACUUM INTO)."""
    if _FORBIDDEN_SQL_RE.search(sql_script or ""):
        raise UnsafeScriptError(
            "SQL 脚本不允许使用 ATTACH DATABASE 或 VACUUM INTO；"
            "所有写入必须只落在该次探测的基线副本内。"
        )

# --- Public data structures ------------------------------------------------

ERROR = "error"
TIMEOUT = "timeout"
SUCCESS = "success"

# Reduction statuses
STATUS_REDUCED = "reduced"
STATUS_NO_ERROR = "no_error"
STATUS_EMPTY_REPRODUCES = "empty_reproduces"
STATUS_INCOMPLETE_TIMEOUT = "incomplete_timeout"
STATUS_FAILED = "failed"


@dataclass
class ProbeOutcome:
    """Result of one probe (one baseline copy + one script execution)."""

    kind: str                      # ERROR | TIMEOUT | SUCCESS
    error_code: Optional[int] = None
    error_name: Optional[str] = None
    message: Optional[str] = None
    duration: float = 0.0
    n_rows: int = 0

    @property
    def is_timeout(self) -> bool:
        return self.kind == TIMEOUT


@dataclass
class ReductionResult:
    status: str
    target: Optional[ProbeOutcome] = None
    empty_outcome: Optional[ProbeOutcome] = None
    reduced_rows: List[Tuple[str, str]] = field(default_factory=list)
    probes: int = 0
    probe_timeout_count: int = 0
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    note: str = ""

    @property
    def elapsed(self) -> Optional[float]:
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at


class ProbeTimeout(Exception):
    """Raised internally when a probe exceeds its time budget."""


# --- Helpers ----------------------------------------------------------------

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def is_reproduction(outcome: ProbeOutcome, target: ProbeOutcome) -> bool:
    """An outcome reproduces the target only when it is a real SQL error
    (never a timeout/success) with the same error code and message."""
    if outcome.kind != ERROR or target.kind != ERROR:
        return False
    if outcome.error_code != target.error_code:
        return False
    return (outcome.message or "").strip() == (target.message or "").strip()


# --- Single probe -----------------------------------------------------------

def run_probe(
    baseline_path: str,
    rows: Sequence[Tuple[str, str]],
    sql_script: str,
    probe_timeout: float = 2.0,
) -> ProbeOutcome:
    """Execute one probe on a fresh, private copy of the baseline.

    Steps:
      1. copy the baseline file into a brand-new temporary directory,
      2. recreate ``staging(sample_id TEXT, reading TEXT)`` and insert the
         candidate rows preserving their order,
      3. execute the deterministic SQL script,
      4. discard the whole temporary directory.

    A statement exceeding ``probe_timeout`` seconds is interrupted; the
    returned outcome kind is ``timeout``.
    """
    started = time.monotonic()
    n_rows = len(rows)
    validate_sql_script(sql_script)
    workdir = tempfile.mkdtemp(prefix="probe_")
    conn: Optional[sqlite3.Connection] = None
    deadline = started + probe_timeout
    state = {"interrupted": False, "timed_out": False}
    timer: Optional[threading.Timer] = None

    def on_progress() -> int:
        # Returning non-zero aborts the running statement.
        if state["interrupted"] or time.monotonic() >= deadline:
            state["timed_out"] = True
            return 1
        return 0

    def on_authorize(action, arg1, arg2, dbname, source):
        # Deny attaching any extra database (incl. VACUUM INTO, which
        # attaches internally) so no write can escape this private copy.
        if action == sqlite3.SQLITE_ATTACH:
            return sqlite3.SQLITE_DENY
        # Extension loading is off by default; refuse it explicitly too.
        if action == sqlite3.SQLITE_FUNCTION and arg1 == "load_extension":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def watchdog() -> None:
        # Belt-and-braces: even if the progress handler is invoked rarely
        # (e.g. inside a long CTE computation), interrupt() terminates the
        # statement from another thread at the wall-clock deadline.
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        if not state["interrupted"]:
            state["timed_out"] = True
            if conn is not None:
                conn.interrupt()

    try:
        db_path = os.path.join(workdir, "probe.db")
        shutil.copy2(baseline_path, db_path)
        # Drop any sidecar journal/WAL left by an abnormal previous copy
        # (the source baseline is never written to, so this is normally
        # empty).
        for suffix in ("-journal", "-wal", "-shm"):
            side = db_path + suffix
            if os.path.exists(side):
                os.remove(side)

        conn = sqlite3.connect(db_path, timeout=5.0)
        conn.set_progress_handler(on_progress, 2000)
        try:
            conn.set_authorizer(on_authorize)
        except AttributeError:  # pragma: no cover - stdlib always has it
            pass
        timer = threading.Timer(max(probe_timeout, 0.0), watchdog)
        timer.daemon = True
        timer.start()

        # Rebuild staging deterministically for every probe.
        conn.execute("DROP TABLE IF EXISTS staging")
        conn.execute(
            "CREATE TABLE staging (sample_id TEXT, reading TEXT)"
        )
        conn.executemany(
            "INSERT INTO staging (sample_id, reading) VALUES (?, ?)",
            list(rows),
        )
        conn.commit()

        try:
            conn.executescript(sql_script)
            conn.commit()
        except sqlite3.Error as exc:
            if state["timed_out"] or time.monotonic() >= deadline:
                return ProbeOutcome(
                    TIMEOUT, duration=time.monotonic() - started, n_rows=n_rows
                )
            return ProbeOutcome(
                ERROR,
                error_code=exc.sqlite_errorcode,
                error_name=getattr(exc, "sqlite_errorname", None),
                message=str(exc),
                duration=time.monotonic() - started,
                n_rows=n_rows,
            )

        if state["timed_out"] or time.monotonic() >= deadline:
            return ProbeOutcome(
                TIMEOUT, duration=time.monotonic() - started, n_rows=n_rows
            )
        return ProbeOutcome(
            SUCCESS, duration=time.monotonic() - started, n_rows=n_rows
        )
    except sqlite3.Error as exc:
        # Errors while building staging (e.g. interrupted INSERT).
        if state["timed_out"] or time.monotonic() >= deadline:
            return ProbeOutcome(
                TIMEOUT, duration=time.monotonic() - started, n_rows=n_rows
            )
        return ProbeOutcome(
            ERROR,
            error_code=exc.sqlite_errorcode,
            error_name=getattr(exc, "sqlite_errorname", None),
            message=str(exc),
            duration=time.monotonic() - started,
            n_rows=n_rows,
        )
    finally:
        state["interrupted"] = True
        if timer is not None:
            timer.cancel()
        if conn is not None:
            try:
                conn.set_progress_handler(None, 0)
                conn.close()
            except sqlite3.Error:
                pass
        shutil.rmtree(workdir, ignore_errors=True)


# --- Delta debugging --------------------------------------------------------

def _balanced_chunks(seq: Sequence, n: int) -> List[list]:
    """Split ``seq`` into ``n`` contiguous, size-balanced chunks."""
    if n <= 0:
        return []
    length = len(seq)
    n = min(n, length)
    base, extra = divmod(length, n)
    chunks: List[list] = []
    start = 0
    for i in range(n):
        size = base + (1 if i < extra else 0)
        chunks.append(list(seq[start:start + size]))
        start += size
    return chunks


def _ddmin(
    rows: List[Tuple[str, str]],
    test: Callable[[List[Tuple[str, str]]], bool],
) -> List[Tuple[str, str]]:
    """Classic delta debugging (Zeller/Hildebrandt) minimisation.

    ``test(subset)`` returns True when the subset still reproduces the
    target error.
    """
    current = list(rows)
    n = 2
    while len(current) >= 2:
        chunks = _balanced_chunks(current, n)
        reduced = False

        # 1. try each chunk alone
        for chunk in chunks:
            if test(chunk):
                current = chunk
                n = max(n - 1, 2)
                reduced = True
                break
        if reduced:
            continue

        # 2. try the complement of each chunk
        for i in range(len(chunks)):
            complement = [r for j, c in enumerate(chunks) if j != i for r in c]
            if complement and test(complement):
                current = complement
                n = max(n - 1, 2)
                reduced = True
                break
        if reduced:
            continue

        # 3. granularity refinement
        if n < len(current):
            n = min(n * 2, len(current))
        else:
            break

    # Single-row cases
    for row in list(current):
        if len(current) == 1:
            break
        candidate = [r for r in current if r is not row]
        if test(candidate):
            current = candidate
    return current


def _ensure_minimal(
    rows: List[Tuple[str, str]],
    test: Callable[[List[Tuple[str, str]]], bool],
) -> Tuple[List[Tuple[str, str]], bool]:
    """Explicitly verify 1-minimality: deleting ANY single row must make
    the error disappear. If not, keep shrinking. Returns (rows, ok)."""
    current = list(rows)
    changed = True
    while changed and len(current) > 1:
        changed = False
        for row in list(current):
            candidate = [r for r in current if r is not row]
            if test(candidate):
                current = candidate
                changed = True
                break
    if len(current) <= 1:
        return current, True
    for row in current:
        candidate = [r for r in current if r is not row]
        if test(candidate):
            return current, False
    return current, True


# --- Orchestration ----------------------------------------------------------

def reduce_batch(
    baseline_path: str,
    rows: List[Tuple[str, str]],
    sql_script: str,
    probe_timeout: float = 2.0,
    report: Optional[Callable[[dict], None]] = None,
) -> ReductionResult:
    """Run the full reduction workflow.

    ``report`` (optional) receives progress events::

        {"phase": "baseline"|"empty"|"ddmin"|"minimize"|"final",
         "probe": <int>, "probes": <int>, "message": <str>,
         "current_size": <int>}
    """
    def emit(phase: str, message: str, current_size: Optional[int] = None) -> None:
        if report is not None:
            report(
                {
                    "phase": phase,
                    "probe": result.probes,
                    "probes": result.probes,
                    "message": message,
                    "current_size": current_size,
                }
            )

    result = ReductionResult(status=STATUS_FAILED, started_at=time.monotonic())
    validate_sql_script(sql_script)
    if not rows:
        result.status = STATUS_NO_ERROR
        result.note = "上传的 CSV 没有任何数据行，无法缩减。"
        result.finished_at = time.monotonic()
        return result

    def probe(subset: Sequence[Tuple[str, str]]) -> ProbeOutcome:
        outcome = run_probe(
            baseline_path, list(subset), sql_script, probe_timeout=probe_timeout
        )
        result.probes += 1
        if outcome.is_timeout:
            result.probe_timeout_count += 1
        return outcome

    # 1. Full batch: establish the target error.
    emit("baseline", "对完整批次执行探测，确定目标错误 …", len(rows))
    target = probe(rows)
    result.target = target

    if target.is_timeout:
        result.status = STATUS_INCOMPLETE_TIMEOUT
        result.note = (
            "完整批次在 2 秒内未执行完，探测已被强制终止；"
            "无法确定目标错误，缩减未完成。"
        )
        result.finished_at = time.monotonic()
        return result
    if target.kind == SUCCESS:
        result.status = STATUS_NO_ERROR
        result.note = (
            "完整批次执行成功，未产生 SQLite 错误；没有可缩减的目标错误，"
            "因此不进行缩减。"
        )
        result.finished_at = time.monotonic()
        return result

    # 2. Empty sample: if it already fails with the same error, the CSV
    #    rows carry no explanatory power -> explain, do not reduce.
    emit("empty", "以空样本探测，检查错误是否与数据行无关 …", 0)
    empty = probe([])
    result.empty_outcome = empty
    if empty.is_timeout:
        result.status = STATUS_INCOMPLETE_TIMEOUT
        result.note = (
            "空样本探测超时并已被终止；无法判断错误是否依赖数据行，"
            "缩减未完成。"
        )
        result.finished_at = time.monotonic()
        return result
    if is_reproduction(empty, target):
        result.status = STATUS_EMPTY_REPRODUCES
        result.note = (
            "空样本（0 行 staging）执行脚本即报出与完整批次相同的错误"
            "（错误码与消息一致），说明该错误由脚本或基线本身触发，"
            "与 CSV 数据行无关，因此不进行缩减。"
        )
        result.finished_at = time.monotonic()
        return result

    # 3. Delta debugging with timeout guard.
    def test(subset: List[Tuple[str, str]]) -> bool:
        outcome = probe(subset)
        if outcome.is_timeout:
            raise ProbeTimeout()
        return is_reproduction(outcome, target)

    try:
        emit("ddmin", "Delta 缩减进行中 …", len(rows))
        candidate = _ddmin(rows, test)
        emit(
            "minimize",
            "校验最小性：逐行删除后均不应再报同一错误 …",
            len(candidate),
        )
        candidate, minimal = _ensure_minimal(candidate, test)
        if not minimal:
            raise ProbeTimeout()
    except ProbeTimeout:
        result.status = STATUS_INCOMPLETE_TIMEOUT
        result.note = (
            "缩减过程中有探测超过 2 秒限制，实际执行已被强制终止；"
            "超时不能作为目标错误或最小性证据，本次缩减标记为未完成。"
        )
        result.finished_at = time.monotonic()
        return result

    # 4. Final independent confirmation of the returned sample.
    emit("final", "对缩减结果做最终独立复核 …", len(candidate))
    confirm = probe(candidate)
    if confirm.is_timeout or not is_reproduction(confirm, target):
        result.status = STATUS_INCOMPLETE_TIMEOUT
        result.note = (
            "最终复核未能复现目标错误（或复核超时被终止），"
            "缩减结果不可靠，标记为未完成。"
        )
        result.finished_at = time.monotonic()
        return result

    if not candidate:
        # Defensive: empty-reproduction was already handled above.
        result.status = STATUS_EMPTY_REPRODUCES
        result.note = "缩减后样本为空，按空样本复现处理，不返回缩减样本。"
        result.finished_at = time.monotonic()
        return result

    result.status = STATUS_REDUCED
    result.reduced_rows = candidate
    result.note = (
        "缩减完成：返回的非空样本仍报出与原批相同的 SQLite 错误；"
        "且删除其中任意一行后都不再报同一错误（1-最小性已校验）。"
    )
    result.finished_at = time.monotonic()
    return result
