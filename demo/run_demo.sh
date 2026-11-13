#!/usr/bin/env bash
# End-to-end demonstration against a running instance (default localhost:8000).
# Submits every demo scenario and prints the expected vs actual outcome.
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! curl -sf "${BASE_URL}/healthz" >/dev/null; then
  echo "服务未就绪：${BASE_URL}/healthz 不可达。请先启动（docker compose up 或 python -m app）。" >&2
  exit 1
fi

submit() {
  local dir="$1"
  curl -s -X POST "${BASE_URL}/api/jobs" \
    -F "baseline=@${dir}/baseline.sqlite3" \
    -F "csvfile=@${dir}/input.csv" \
    -F "script=@${dir}/script.sql" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['job_id'])"
}

wait_done() {
  local job="$1"
  for _ in $(seq 1 60); do
    local state
    state=$(curl -s "${BASE_URL}/api/jobs/${job}" \
      | python3 -c "import sys,json;print(json.load(sys.stdin)['state'])")
    [ "$state" = "done" ] && return 0
    sleep 0.5
  done
  echo "任务 ${job} 等待超时" >&2
  exit 1
}

show() {
  local name="$1" job="$2" expect="$3"
  wait_done "$job"
  local payload
  payload="$(curl -s "${BASE_URL}/api/jobs/${job}")"
  BASE_URL="$BASE_URL" PAYLOAD="$payload" NAME="$name" EXPECT="$expect" \
    python3 <<'PY'
import json, os
name = os.environ["NAME"]
expect = os.environ["EXPECT"]
base_url = os.environ["BASE_URL"]
d = json.loads(os.environ["PAYLOAD"])
r = d["result"]
t = r.get("target") or {}
print(f"[{name}]")
print(f"  期望: {expect}")
print(f"  实际: status={r['status']}  probes={r['probes']}  "
      f"timeouts={r['probe_timeout_count']}  耗时={r['elapsed']:.2f}s")
if t.get("error_name"):
    print(f"  目标错误: {t['error_name']} (code={t['error_code']}) :: {t['message']}")
if r["reduced_rows"]:
    print(f"  缩减样本({len(r['reduced_rows'])}行): {r['reduced_rows']}")
    print(f"  下载: {base_url}/api/jobs/{d['job_id']}/download")
print(f"  说明: {r['note']}")
print()
PY
}

J1=$(submit "${HERE}/check_constraint")
J2=$(submit "${HERE}/unique_duplicate")
J3=$(submit "${HERE}/success_batch")
J4=$(submit "${HERE}/empty_reproduces")
J5=$(submit "${HERE}/timeout_probe")

show "CHECK 约束（40 行 → 1 行）"      "$J1" "reduced / 1 行 [S-BAD, N/A]"
show "UNIQUE 主键冲突（最小 2 行）"   "$J2" "reduced / 2 行重复 sample_id"
show "原批成功（不缩减）"             "$J3" "no_error"
show "空样本即报同错（不缩减）"       "$J4" "empty_reproduces"
show "探测超时（未完成）"             "$J5" "incomplete_timeout"

echo "演示完成。浏览器打开 ${BASE_URL}/ 可查看图形界面与下载入口。"
