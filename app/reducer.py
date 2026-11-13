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
* Each probe is limited to ``probe_timeout`` seconds (default 2) and runs
  in a **separate worker process**. A progress handler plus
  ``Connection.interrupt()`` terminate interruptible statements
  gracefully, but some SQLite work (notably a catastrophic ``GLOB``
  backtracking match) executes inside one C-level VM opcode and never
  consults either mechanism. The parent therefore enforces the deadline
  by killing the worker process (SIGTERM, then SIGKILL), which terminates
  the actual execution unconditionally, and only then removes the
  private copy. A timeout is never interpreted as the target error nor
  as minimality evidence; the reduction is marked incomplete.
* The result is verified: the reduced batch must reproduce the target
  error, and removing any single remaining row must stop reproducing it.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
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
CRASH = "crash"      # worker died/was killed without a result -> inconclusive

# Reduction statuses
STATUS_REDUCED = "reduced"
STATUS_NO_ERROR = "no_error"
STATUS_EMPTY_REPRODUCES = "empty_reproduces"
STATUS_INCOMPLETE_TIMEOUT = "incomplete_timeout"
STATUS_FAILED = "failed"


@dataclass
class ProbeOutcome:
    """Result of one probe (one baseline copy + one script execution)."""

    kind: str                      # ERROR | TIMEOUT | SUCCESS | CRASH
    error_code: Optional[int] = None
    error_name: Optional[str] = None
    message: Optional[str] = None
    duration: float = 0.0
    n_rows: int = 0
    detail: Optional[str] = None   # diagnostic only; never error evidence

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


class ProbeCrash(Exception):
    """Raised internally when a probe worker dies without a result."""


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

# The probe runs in its own interpreter process.  A catastrophic SQLite
# match (e.g. GLOB over a long string with a pathologically ambiguous
# pattern) backtracks inside a *single* C-level VM opcode: the progress
# handler is never invoked, and Connection.interrupt() only sets a flag
# that is inspected between opcodes.  Neither can stop that execution.
# SIGTERM/SIGKILL delivered to the worker process does, unconditionally.
#
# A subprocess (rather than multiprocessing ``fork``) is used deliberately:
# the web layer runs reductions in background threads of a threaded
# gunicorn worker, and forking from a multi-threaded process can inherit
# locks held by other threads, stalling the child until it looks like a
# timeout.  ``spawn``/``forkserver`` instead re-import the host's main
# module, which gunicorn does not guarantee to be import-safe.  A fresh
# ``python`` process sidesteps all of it.

_WORKER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "probe_worker.py")


def _proc_start_time(pid: int) -> Optional[str]:
    """Kernel start time (field 22 of /proc/<pid>/stat) or None."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as fh:
            stat = fh.read().decode("ascii", "replace")
        # Field 2 (comm) may contain spaces and parentheses.
        return stat[stat.rindex(")") + 2:].split()[19]
    except (OSError, ValueError, IndexError):
        return None


def _hostname() -> Optional[str]:
    try:
        return os.uname().nodename
    except AttributeError:  # pragma: no cover - non-POSIX fallback
        return None


def _pid_is_alive(pid: int, ident: Optional[dict]) -> bool:
    """True only when ``pid`` is alive AND still the recorded process.

    Checking the kernel start time guards against PID reuse: a recycled
    PID belonging to an unrelated process is never signalled.
    """
    if ident is None:
        ident = {}
    start_time = ident.get("start_time")
    hostname = ident.get("hostname")
    if start_time is not None and hostname is not None:
        if _hostname() != hostname:
            return False
        return _proc_start_time(pid) == start_time
    # No kernel identity available: fall back to liveness only.
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - same-user worker
        return True


def terminate_probe_process(proc: "subprocess.Popen", ident: dict) -> bool:
    """Terminate the worker's real execution: SIGTERM, then SIGKILL.

    Returns True when a live worker was signalled.  The PID identity is
    re-verified before each signal, and SIGKILL is used unconditionally
    after a short grace period, so a statement stuck inside a C-level
    opcode cannot outlive the deadline.
    """
    if proc is None or proc.poll() is not None:
        return False
    if not _pid_is_alive(proc.pid, ident):
        return False
    try:
        proc.terminate()
    except ProcessLookupError:
        return False
    try:
        proc.wait(timeout=0.25)
        return True
    except subprocess.TimeoutExpired:
        pass
    if _pid_is_alive(proc.pid, ident):
        try:
            proc.kill()
        except ProcessLookupError:  # pragma: no cover
            pass
    return True


def _run_probe_inner(
    baseline_path: str,
    rows: Sequence[Tuple[str, str]],
    sql_script: str,
    probe_timeout: float,
    workdir: Optional[str] = None,
) -> ProbeOutcome:
    """Execute one probe on a fresh, private copy of the baseline.

    Steps:
      1. copy the baseline file into a brand-new temporary directory,
      2. recreate ``staging(sample_id TEXT, reading TEXT)`` and insert the
         candidate rows preserving their order,
      3. execute the deterministic SQL script,
      4. discard the whole temporary directory.

    A statement exceeding ``probe_timeout`` is interrupted when possible;
    the returned outcome kind is ``timeout``.  (The parent additionally
    kills this process at the same deadline, which is what actually stops
    non-interruptible statements.)
    """
    started = time.monotonic()
    n_rows = len(rows)
    validate_sql_script(sql_script)
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="probe_")
    else:
        os.makedirs(workdir, exist_ok=True)
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
        # Best-effort graceful abort for interruptible statements.  The
        # parent's SIGTERM/SIGKILL is the real deadline for work that
        # cannot be interrupted.
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


def run_probe(
    baseline_path: str,
    rows: Sequence[Tuple[str, str]],
    sql_script: str,
    probe_timeout: float = 2.0,
) -> ProbeOutcome:
    """Run one probe in a dedicated worker process, with a hard deadline.

    ``probe_timeout`` bounds actual execution: at the deadline the worker
    is terminated (SIGTERM, then SIGKILL), which stops even SQL that runs
    inside one non-interruptible C-level opcode.  Its private copy of the
    baseline is then removed.  A timeout is never treated as the target
    error nor as minimality evidence.
    """
    started = time.monotonic()
    validate_sql_script(sql_script)
    # One private directory holds the request, the worker's baseline copy
    # and its response.  It is removed in `finally`, after the worker has
    # been waited for -- including the SIGKILL path -- so no probe copy is
    # ever leaked.
    private_dir = tempfile.mkdtemp(prefix="probe_")
    request_path = os.path.join(private_dir, "request.json")
    response_path = os.path.join(private_dir, "response.json")
    stderr_path = os.path.join(private_dir, "stderr.log")
    copy_dir = os.path.join(private_dir, "copy")
    proc: Optional["subprocess.Popen"] = None
    ident: Optional[dict] = None
    try:
        with open(request_path, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "baseline_path": baseline_path,
                    "rows": [list(r) for r in rows],
                    "sql_script": sql_script,
                    "probe_timeout": probe_timeout,
                    "workdir": copy_dir,
                },
                fh,
            )

        with open(stderr_path, "wb") as stderr_fh:
            proc = subprocess.Popen(
                [sys.executable, _WORKER_SCRIPT, request_path, response_path],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=stderr_fh,
                close_fds=True,
                start_new_session=True,
            )
            # Record the kernel identity immediately: it is used to verify
            # that a PID is still ours before it is signalled.
            ident = {
                "pid": proc.pid,
                "start_time": _proc_start_time(proc.pid),
                "hostname": _hostname(),
            }
            remaining = probe_timeout - (time.monotonic() - started)
            try:
                proc.wait(timeout=max(remaining, 0.0))
            except subprocess.TimeoutExpired:
                # The worker has NOT produced a result yet.  Terminate it
                # unconditionally -- this is what stops execution that the
                # progress handler and interrupt() cannot.  A response
                # published just before the kill raced with the deadline is
                # still a legitimate, in-time result, so it is preferred
                # over reporting a timeout.
                terminate_probe_process(proc, ident)
                try:
                    proc.wait(timeout=0.5)
                except subprocess.TimeoutExpired:  # pragma: no cover
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
                    proc.wait(timeout=0.5)
                payload = _read_response(response_path)
                if payload is None:
                    return ProbeOutcome(
                        TIMEOUT,
                        duration=time.monotonic() - started,
                        n_rows=len(rows),
                    )
                return _outcome_from_payload(payload, rows, started)

        payload = _read_response(response_path)
        if payload is not None:
            return _outcome_from_payload(payload, rows, started)
        # The worker exited without publishing a result (crash or external
        # kill).  Report it as an inconclusive ``crash`` outcome: it must
        # never be mistaken for the target error, for a timeout, or for
        # minimality evidence.  The tail of the worker's stderr goes in
        # ``detail`` (diagnostic only), never in ``message``, which is what
        # participates in error matching.
        tail = _tail(stderr_path)
        return ProbeOutcome(
            CRASH,
            duration=time.monotonic() - started,
            n_rows=len(rows),
            detail=("probe worker exited without a result"
                    + (f": {tail}" if tail else "")),
        )
    finally:
        if proc is not None and proc.poll() is None:
            terminate_probe_process(proc, ident)
            try:
                proc.wait(timeout=0.5)
            except subprocess.TimeoutExpired:  # pragma: no cover
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    proc.wait(timeout=0.5)
                except subprocess.TimeoutExpired:  # pragma: no cover
                    pass
        shutil.rmtree(private_dir, ignore_errors=True)


def _outcome_from_payload(
    payload: dict, rows: Sequence[Tuple[str, str]], started: float
) -> ProbeOutcome:
    return ProbeOutcome(
        kind=payload["kind"],
        error_code=payload.get("error_code"),
        error_name=payload.get("error_name"),
        message=payload.get("message"),
        duration=payload.get("duration") or (time.monotonic() - started),
        n_rows=payload.get("n_rows", len(rows)),
    )


def _read_response(path: str) -> Optional[dict]:
    try:
        with open(path, "rb") as fh:
            return json.loads(fh.read().decode("utf-8"))
    except (OSError, ValueError):
        return None


def _tail(path: str, limit: int = 2000) -> str:
    try:
        with open(path, "rb") as fh:
            data = fh.read()
    except OSError:
        return ""
    text = data.decode("utf-8", "replace").strip()
    return text[-limit:]


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
            f"完整批次在 {probe_timeout:g} 秒内未执行完，探测已被强制终止；"
            "无法确定目标错误，缩减未完成。"
        )
        result.finished_at = time.monotonic()
        return result
    if target.kind == CRASH:
        result.status = STATUS_FAILED
        result.note = (
            "完整批次的探测进程异常结束，未能得到结果；"
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
    if empty.kind == CRASH:
        result.status = STATUS_FAILED
        result.note = (
            "空样本的探测进程异常结束，未能判断错误是否依赖数据行；"
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
        if outcome.kind == CRASH:
            raise ProbeCrash()
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
            f"缩减过程中有探测超过 {probe_timeout:g} 秒限制，实际执行已被强制终止；"
            "超时不能作为目标错误或最小性证据，本次缩减标记为未完成。"
        )
        result.finished_at = time.monotonic()
        return result
    except ProbeCrash:
        result.status = STATUS_FAILED
        result.note = (
            "缩减过程中有探测进程异常结束，结果不可靠；"
            "本次缩减标记为失败，不返回样本。"
        )
        result.finished_at = time.monotonic()
        return result

    # 4. Final independent confirmation of the returned sample.
    emit("final", "对缩减结果做最终独立复核 …", len(candidate))
    confirm = probe(candidate)
    if confirm.kind == CRASH:
        result.status = STATUS_FAILED
        result.note = (
            "最终复核的探测进程异常结束，结果不可靠；不返回缩减样本。"
        )
        result.finished_at = time.monotonic()
        return result
    if confirm.is_timeout:
        result.status = STATUS_INCOMPLETE_TIMEOUT
        result.note = (
            "最终复核超时并已被终止，超时不能作为结果证据；"
            "缩减结果不可靠，标记为未完成。"
        )
        result.finished_at = time.monotonic()
        return result
    if not is_reproduction(confirm, target):
        # Not a timeout: the candidate genuinely stopped reproducing the
        # target error.  That is a failed verification, not an incomplete
        # run, and it is never reported as a reduced sample.
        result.status = STATUS_FAILED
        result.note = (
            "最终复核未能复现目标错误，缩减结果不可靠；"
            "不返回缩减样本。"
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
