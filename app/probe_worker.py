"""Worker process for a single probe.

Started by :func:`app.reducer.run_probe` as a *fresh* interpreter process.
Isolation is what enforces the probe deadline: some SQLite work (a
catastrophic ``GLOB`` match, for instance) runs inside one C-level VM
opcode and never consults the progress handler or
``Connection.interrupt()``.  Only terminating this process can stop it.

Protocol
--------
``argv[1]``: request JSON, containing ``baseline_path``, ``rows``,
``sql_script``, ``probe_timeout`` and ``workdir``.
``argv[2]``: path where the outcome JSON is written.

The worker publishes the outcome and then exits via ``os._exit`` so that
no interpreter shutdown, atexit handler or lingering thread can keep the
process -- and the private baseline copy -- alive beyond the deadline.
"""

import importlib.util
import json
import os
import sys


def _die_with_parent(parent_pid: int) -> None:
    """Ask the kernel to kill this process if its parent disappears.

    A probe can run SQL that holds the GIL inside one C-level opcode, so
    an in-process Python watchdog thread would never be scheduled.  The
    kernel's PR_SET_PDEATHSIG is not subject to that limitation: if the
    service (gunicorn worker) is recycled or killed while a probe is
    running, the probe is terminated too, instead of burning CPU orphaned.
    Linux-only; silently ignored elsewhere, where the parent's own
    deadline still governs.
    """
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        # PR_SET_PDEATHSIG = 1, SIGKILL = 9.
        if libc.prctl(1, 9, 0, 0, 0) != 0:
            return
    except Exception:  # pragma: no cover - platform without prctl
        return
    # Close the race: the parent may have died before prctl took effect.
    if os.getppid() != parent_pid:
        os._exit(0)


def main() -> int:
    if len(sys.argv) != 3:
        return 2
    request_path, response_path = sys.argv[1], sys.argv[2]
    _die_with_parent(os.getppid())

    # Load the reducer module directly by path.  Importing ``app.reducer``
    # through the package would first execute ``app/__init__.py`` (which
    # imports Flask), paying several hundred milliseconds per probe for a
    # web framework this worker never uses.  ``reducer.py`` has no
    # relative imports, so a direct spec-based load is exact.
    reducer_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "reducer.py"
    )
    spec = importlib.util.spec_from_file_location("_probe_reducer", reducer_path)
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolves ``cls.__module__`` through sys.modules while
    # decorating, so the module must be registered before it executes.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    with open(request_path, "r", encoding="utf-8") as fh:
        request = json.load(fh)

    try:
        outcome = module._run_probe_inner(
            request["baseline_path"],
            [tuple(r) for r in request["rows"]],
            request["sql_script"],
            request["probe_timeout"],
            workdir=request["workdir"],
        )
        payload = {
            "kind": outcome.kind,
            "error_code": outcome.error_code,
            "error_name": outcome.error_name,
            "message": outcome.message,
            "duration": outcome.duration,
            "n_rows": outcome.n_rows,
        }
        with open(response_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
            fh.flush()
            os.fsync(fh.fileno())
    finally:
        os._exit(0)


if __name__ == "__main__":
    sys.exit(main())
