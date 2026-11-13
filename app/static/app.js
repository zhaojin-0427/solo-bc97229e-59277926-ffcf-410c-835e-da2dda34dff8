/* Front-end logic: upload job, poll progress, render result, download. */
(function () {
  "use strict";

  var PHASE_LABELS = {
    queued: "排队中",
    baseline: "完整批次探测",
    empty: "空样本探测",
    ddmin: "Delta 缩减",
    minimize: "最小性校验",
    final: "最终复核",
  };

  var STATUS_LABELS = {
    reduced: "已缩减（最小复现样本）",
    no_error: "原批成功，无目标错误，不缩减",
    empty_reproduces: "空样本即报同错，与数据行无关，不缩减",
    incomplete_timeout: "未完成：存在超时探测",
    failed: "失败",
  };

  function $(id) { return document.getElementById(id); }

  function el(tag, attrs, text) {
    var node = document.createElement(tag);
    if (attrs) Object.keys(attrs).forEach(function (k) {
      node.setAttribute(k, attrs[k]);
    });
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function fmtOutcome(box, outcome) {
    box.className = "outcome";
    box.textContent = "";
    if (!outcome) {
      box.classList.add("empty");
      box.textContent = "尚未执行";
      return;
    }
    if (outcome.kind === "success") {
      box.classList.add("success");
      box.appendChild(el("div", null, "✔ 执行成功，无 SQL 错误"));
      box.appendChild(el("div", null, "耗时 " + outcome.duration + "s · " +
        outcome.n_rows + " 行"));
      return;
    }
    if (outcome.kind === "timeout") {
      box.classList.add("timeout");
      box.appendChild(el("div", null, "⏱ 超时（已强制终止实际执行，" +
        "不作为目标错误或最小性证据）"));
      box.appendChild(el("div", null, "耗时 " + outcome.duration + "s · " +
        outcome.n_rows + " 行"));
      return;
    }
    box.classList.add("error");
    box.appendChild(el("div", null, "✖ SQLite 错误"));
    box.appendChild(el("div", null,
      "错误码 " + outcome.error_code +
      (outcome.error_name ? "（" + outcome.error_name + "）" : "")));
    box.appendChild(el("code", null, outcome.message || ""));
    box.appendChild(el("div", null, "耗时 " + outcome.duration + "s · " +
      outcome.n_rows + " 行"));
  }

  function badge(state, status) {
    var span = el("span", { "class": "badge" });
    if (state !== "done") {
      span.classList.add("running");
      span.textContent = "执行中";
      return span;
    }
    if (status === "reduced") {
      span.classList.add("done-ok");
    } else if (status === "incomplete_timeout" || status === "failed") {
      span.classList.add("done-err");
    } else {
      span.classList.add("done-warn");
    }
    span.textContent = STATUS_LABELS[status] || status;
    return span;
  }

  var pollTimer = null;
  var startWall = null;

  function renderJob(data) {
    $("m-job-id").textContent = data.job_id;
    $("m-input-rows").textContent = data.input_rows + " 行";
    $("m-baseline-hash").textContent = data.baseline_sha256;
    $("timeout-label").textContent = data.probe_timeout_seconds;
    $("timeout-count").textContent =
      data.result ? data.result.probe_timeout_count : 0;

    var stateBox = $("m-state");
    stateBox.textContent = "";
    stateBox.appendChild(badge(data.state, data.result && data.result.status));

    var p = data.progress || {};
    $("progress-msg").textContent =
      (PHASE_LABELS[p.phase] || p.phase || "") + "：" + (p.message || "");
    $("probe-count").textContent = p.probe || 0;
    $("current-size").textContent =
      p.current_size === null || p.current_size === undefined
        ? "-" : p.current_size;

    var fill = $("progress-fill");
    if (data.state === "done") {
      fill.style.width = "100%";
      fill.className = data.result.status === "reduced" ? "done"
        : (data.result.status === "incomplete_timeout" ? "err" : "warn");
    } else if (p.probe > 0) {
      // Indeterminate total: ramp logarithmically, never to 100%.
      fill.style.width = Math.min(85, 12 + Math.log2(p.probe + 1) * 14) + "%";
    }

    var log = $("event-log");
    log.textContent = (data.events || []).map(function (e) {
      return "[t+" + e.ts + "s] " + (PHASE_LABELS[e.phase] || e.phase) +
        " · 第 " + e.probe + " 次探测 · " + e.message +
        (e.current_size !== null && e.current_size !== undefined
          ? "（候选 " + e.current_size + " 行）" : "");
    }).join("\n");
    log.scrollTop = log.scrollHeight;

    if (data.result) {
      fmtOutcome($("target-error"), data.result.target);
      fmtOutcome($("empty-outcome"), data.result.empty_outcome);
      var rbox = $("result-box");
      rbox.className = "outcome";
      rbox.textContent = "";
      rbox.appendChild(el("div", null,
        STATUS_LABELS[data.result.status] || data.result.status));
      rbox.appendChild(el("div", null, data.result.note));
      rbox.appendChild(el("div", null,
        "共探测 " + data.result.probes + " 次，总耗时 " +
        (data.result.elapsed ? data.result.elapsed.toFixed(2) : "-") + "s"));

      if (data.result.status === "reduced") {
        $("download-box").hidden = false;
        $("download-link").href =
          "/api/jobs/" + data.job_id + "/download";
        var tbody = $("sample-table").querySelector("tbody");
        tbody.textContent = "";
        data.result.reduced_rows.forEach(function (row, i) {
          var tr = el("tr");
          tr.appendChild(el("td", null, String(i + 1)));
          tr.appendChild(el("td", null, row[0]));
          tr.appendChild(el("td", null, row[1]));
          tbody.appendChild(tr);
        });
      }
      stopPolling();
    }
  }

  function poll(jobId) {
    fetch("/api/jobs/" + jobId)
      .then(function (r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      })
      .then(function (data) {
        renderJob(data);
        if (data.fatal) {
          var rbox = $("result-box");
          rbox.className = "outcome error";
          rbox.textContent = "服务端异常：" + data.fatal;
          stopPolling();
        }
      })
      .catch(function (err) {
        $("progress-msg").textContent = "状态查询失败：" + err.message +
          "（页面继续重试，缩减仍在后台执行）";
      });
  }

  function stopPolling() {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  function tickElapsed() {
    if (!startWall) return;
    $("elapsed").textContent = ((Date.now() - startWall) / 1000).toFixed(1);
  }

  function openJob(jobId) {
    $("form-section").hidden = true;
    $("job-section").hidden = false;
    history.replaceState(null, "", "/jobs/" + jobId);
    startWall = Date.now();
    setInterval(tickElapsed, 200);
    poll(jobId);
    pollTimer = setInterval(function () { poll(jobId); }, 1000);
  }

  $("upload-form").addEventListener("submit", function (ev) {
    ev.preventDefault();
    var errBox = $("form-error");
    errBox.hidden = true;
    var btn = $("submit-btn");
    btn.disabled = true;
    btn.textContent = "上传并启动 …";

    var fd = new FormData();
    fd.append("baseline", $("baseline").files[0]);
    fd.append("csvfile", $("csvfile").files[0]);
    fd.append("script", $("script").files[0]);

    fetch("/api/jobs", { method: "POST", body: fd })
      .then(function (r) {
        return r.json().then(function (body) { return { ok: r.ok, body: body }; });
      })
      .then(function (res) {
        if (!res.ok) throw new Error(res.body.error || "提交失败");
        openJob(res.body.job_id);
      })
      .catch(function (err) {
        errBox.textContent = err.message;
        errBox.hidden = false;
        btn.disabled = false;
        btn.textContent = "开始缩减";
      });
  });

  // Deep-link support: /jobs/<id>
  var m = location.pathname.match(/^\/jobs\/([a-f0-9]+)$/);
  if (m) {
    $("form-section").hidden = true;
    $("job-section").hidden = false;
    startWall = Date.now();
    setInterval(tickElapsed, 200);
    poll(m[1]);
    pollTimer = setInterval(function () { poll(m[1]); }, 1000);
  }
})();
