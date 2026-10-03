(() => {
  const $ = (id) => document.getElementById(id);
  const escapeText = (value) => value == null ? "—" : String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
  const escapeAttr = (value) => escapeText(value);
  const normalize = (value) => String(value || "").trim().toUpperCase();
  const labels = {
    READY: "就绪",
    STALE: "数据陈旧",
    ACTIVE: "活跃",
    INACTIVE: "未激活",
    PENDING: "等待中",
    QUEUED: "排队中",
    RUNNING: "运行中",
    SUCCEEDED: "成功",
    FAILED: "失败",
    ERROR: "错误",
    WARNING: "警告",
    WARN: "警告",
    DEGRADED: "降级",
    UNAVAILABLE: "不可用",
    CONFLICT: "冲突",
    EXPIRED: "已过期",
    OK: "正常",
    NONE: "无协调者",
    UNKNOWN: "未知",
    CONNECTED: "已连接",
    FALLBACK: "已切换备用入口",
    PRIMARY_SEED: "主 Seed",
    SEED: "备用 Seed",
    DISCOVERED_MEMBER: "发现的 Runtime",
    COORDINATOR: "协调者",
    MEMBER: "成员",
    BOUND: "已绑定",
    BINDING: "绑定中",
    PARTIAL: "部分可用",
    NOT_REQUIRED: "不要求",
    NOT_EXPOSED_BY_EXISTING_API: "现有 API 未暴露",
    NOT_EXPOSED: "未暴露",
    YES: "是",
    NO: "否",
    LAB: "LAB 模式",
    OBSERVE: "只读观测",
    SUCCESS: "成功",
    STOPPED: "已停止",
    STARTING: "启动中",
    PROCESS: "进程状态",
    HARNESS: "Harness",
    DTM: "DTM 成员",
    RESET_DEMO: "重置演示",
    RUN_GREENHOUSE_TASK: "运行温室示例任务",
    START_RUNTIME: "启动 Runtime",
    STOP_RUNTIME: "停止 Runtime",
    RESTART_RUNTIME: "重启 Runtime",
    STOP_CURRENT_COORDINATOR: "停止当前协调者"
  };
  const sourceLabels = {
    RECORDED_TASK_STEP_NODE: "任务步骤记录节点",
    RECORDED_TASK_NODE: "任务记录节点",
    OBSERVED_RUNTIME: "Runtime 观测",
    SYNTHETIC_DIFF: "观测差异"
  };
  const eventLabels = {
    TASK_STATUS_CHANGED: "任务状态变化",
    COORDINATOR_CHANGED: "协调者变化",
    CORE_STATE_CHANGED: "Core 状态变化",
    AUTHORITY_STATE_CHANGED: "权威服务状态变化",
    INGRESS_STATE_CHANGED: "入口服务状态变化",
    MESH_STATUS_CHANGED: "网格状态变化",
    RUNTIME_STATE_CHANGED: "运行时状态变化",
    RESOURCE_STATE_CHANGED: "资源状态变化",
    OBSERVATION_CHANGED: "观测变化"
  };
  const labelFor = (value, fallback = "—") => {
    if (value == null || value === "") return fallback;
    const key = normalize(value);
    return labels[key] || sourceLabels[key] || String(value);
  };
  const titleFor = (value) => value == null || value === "" ? "" : `原始值：${String(value)}`;
  const valueText = (value, fallback = "—") => {
    const label = labelFor(value, fallback);
    const title = titleFor(value);
    return title && label !== String(value) ? `<span title="${escapeAttr(title)}">${escapeText(label)}</span>` : escapeText(label);
  };
  const booleanText = (value) => value ? "是" : "否";
  const runtimeStateText = (state) => {
    const parts = String(state || "").split(";");
    const result = [];
    if (parts[0]) result.push(`成员状态=${valueText(parts[0])}`);
    for (const part of parts.slice(1)) {
      const separator = part.indexOf("=");
      if (separator <= 0) continue;
      const key = part.slice(0, separator);
      const value = part.slice(separator + 1);
      if (key === "coordinator") result.push(`协调者=${booleanText(value === "true")}`);
      else if (key === "core") result.push(`Core=${booleanText(value === "true")}`);
      else if (key === "authority") result.push(`权威服务=${valueText(value)}`);
    }
    return result.join("，");
  };
  const messageText = (event) => {
    const raw = String(event.message || "");
    const type = normalize(event.type);
    if (type === "COORDINATOR_CHANGED") {
      if (raw.startsWith("coordinator state: ")) return `协调者状态：${valueText(raw.slice("coordinator state: ".length))}`;
      if (raw.startsWith("coordinator selected: ")) return `已选择协调者：${escapeText(raw.slice("coordinator selected: ".length))}`;
    }
    if (raw.startsWith("dynamic core state: ")) return `动态 Core 状态：${valueText(raw.slice("dynamic core state: ".length))}`;
    if (raw.startsWith("authority state: ")) return `权威服务状态：${valueText(raw.slice("authority state: ".length))}`;
    if (raw.startsWith("ingress state: ")) return `入口服务状态：${valueText(raw.slice("ingress state: ".length))}`;
    if (raw.startsWith("mesh status: ")) return `网格状态：${valueText(raw.slice("mesh status: ".length))}`;
    if (raw.includes(" observed: ")) {
      const [identity, state] = raw.split(" observed: ");
      return `${escapeText(identity)}：${runtimeStateText(state)}`;
    }
    if (raw.startsWith("resource ")) {
      const separator = raw.indexOf(": ");
      if (separator > 0) return `资源 ${escapeText(raw.slice("resource ".length, separator))}：${valueText(raw.slice(separator + 2))}`;
    }
    if (raw.startsWith("task ")) {
      const separator = raw.indexOf(": ");
      if (separator > 0) return `任务 ${escapeText(raw.slice("task ".length, separator))}：${valueText(raw.slice(separator + 2))}`;
    }
    return escapeText(raw);
  };
  const classFor = (value) => {
    const normalized = String(value || "").toLowerCase();
    if (["ready", "active", "succeeded", "ok", "bound", "connected"].includes(normalized)) return "good";
    if (["warning", "binding", "not_ready", "suspect", "degraded", "partial", "stale", "fallback"].includes(normalized)) return "warn";
    if (["failed", "error", "unavailable", "conflict", "expired"].includes(normalized)) return "bad";
    return "neutral";
  };
  const badge = (value) => `<span class="badge ${classFor(value)}" title="${escapeAttr(titleFor(value))}">${valueText(value)}</span>`;
  const formatTime = (value) => value ? new Date(value).toLocaleTimeString("zh-CN") : "—";
  const pretty = (value) => value == null ? "—" : JSON.stringify(value);

  function render(snapshot) {
    const mesh = snapshot.mesh || {};
    $("mesh-status").innerHTML = badge(mesh.status || snapshot.status);
    $("mesh-subtitle").innerHTML = `协调者 ${escapeText(mesh.coordinator)} · ${valueText(mesh.coordinator_state)}`;
    $("runtime-count").textContent = escapeText(mesh.runtime_count);
    $("coordinator").textContent = escapeText(mesh.coordinator);
    $("core-state").innerHTML = badge(mesh.core_state);
    $("authority-state").innerHTML = badge(mesh.authority_state);
    $("ingress-state").innerHTML = badge(mesh.ingress_state);
    $("coordinator-state").innerHTML = badge(mesh.coordinator_state);
    const observer = snapshot.observer || {};
    $("observer-node").textContent = observer.node_id || "—";
    $("observer-detail").innerHTML = `${badge(observer.state || "UNKNOWN")} · ${escapeText(observer.endpoint || "—")} · ${escapeText(labelFor(observer.source || "UNKNOWN"))} · seeds=${escapeText(observer.seed_count == null ? "—" : observer.seed_count)}`;
    $("resource-count").textContent = escapeText((snapshot.resources || []).length);
    $("task-count").textContent = escapeText((snapshot.tasks || []).length);

    const runtimes = snapshot.runtimes || [];
    $("runtime-body").innerHTML = runtimes.length ? runtimes.map((runtime) => `<tr>
      <td><strong>${escapeText(runtime.node_id)}</strong><small>${escapeText(runtime.runtime_instance_id)}</small></td>
      <td>${runtime.coordinator ? badge("COORDINATOR") : badge("MEMBER")}</td>
      <td>${badge(runtime.membership_state)}</td>
      <td>${valueText(runtime.coordinator ? "COORDINATOR" : "MEMBER")}</td>
      <td>${badge(runtime.core_ready ? "ACTIVE" : "INACTIVE")}</td>
      <td>${badge(runtime.local_authority_binding || "UNKNOWN")}</td>
      <td><code>${escapeText(runtime.control_endpoint)}</code></td>
    </tr>`).join("") : `<tr><td colspan="7" class="empty">暂时没有 Runtime 观测。</td></tr>`;

    const resources = snapshot.resources || [];
    $("resource-list").innerHTML = resources.length ? resources.map((resource) => `<div class="resource-card">
      <div class="row-between"><strong>${escapeText(resource.capability || resource.resource_id)}</strong>${badge(resource.status)}</div>
      <div class="muted">${escapeText(resource.resource_id)} · 类型=${escapeText(resource.kind)}</div>
      <dl><dt>所有者</dt><dd>${escapeText(resource.owner_node_id)}</dd><dt>已发现</dt><dd>${booleanText(resource.discovered)}</dd><dt>权威绑定</dt><dd>${booleanText(resource.authority_bound)}</dd><dt>已发布</dt><dd>${valueText(resource.published)}</dd><dt>可调度</dt><dd>${valueText(resource.eligible)}</dd></dl>
    </div>`).join("") : `<div class="empty">暂时没有 Resource 广告。</div>`;

    const tasks = snapshot.tasks || [];
    $("task-list").innerHTML = tasks.length ? tasks.map((task) => `<details class="task-card" open>
      <summary><span><strong>${escapeText(task.task_id)}</strong><small>${escapeText(task.task_type)}</small></span>${badge(task.status)}</summary>
      <div class="task-meta">创建 ${formatTime(task.created_at)} · 更新 ${formatTime(task.updated_at)}</div>
      ${(task.steps || []).map((step) => `<div class="step-card"><div class="row-between"><strong>${escapeText(step.step_id)}</strong>${badge(step.status)}</div><div class="muted">${escapeText(step.capability)} → ${escapeText(step.mapped_node || "未记录节点")}</div><div class="muted">映射来源=${valueText(step.mapping_source)} · ResourceRef 历史=${valueText(step.resource_ref_history)}</div>${step.result ? `<pre>${escapeText(pretty(step.result))}</pre>` : ""}</div>`).join("")}
    </details>`).join("") : `<div class="empty">暂时没有观测到任务。</div>`;

    const events = snapshot.events || [];
    $("timeline").innerHTML = events.length ? events.slice().reverse().map((event) => `<li><time>${formatTime(event.timestamp)}</time><span class="event-type" title="${escapeAttr(titleFor(event.type))}">${escapeText(eventLabels[normalize(event.type)] || "观测事件")}</span><span>${messageText(event)}</span></li>`).join("") : `<li class="empty">暂时没有观测事件。</li>`;
    $("last-refresh").textContent = `最新快照：${formatTime(snapshot.timestamp)}`;
    $("refresh-dot").className = `status-dot ${classFor(snapshot.status)}`;
    const alert = $("alert");
    const message = snapshot.error || snapshot.task_query_error;
    alert.textContent = message ? `观测异常：${message}` : "";
    alert.classList.toggle("hidden", !message);
    renderLab(snapshot.lab || {}, runtimes, mesh);
  }

  const operationLabels = {
    RUN_GREENHOUSE_TASK: "运行温室示例任务",
    START_RUNTIME: "启动 Runtime",
    STOP_RUNTIME: "停止 Runtime",
    RESTART_RUNTIME: "重启 Runtime",
    STOP_CURRENT_COORDINATOR: "停止当前协调者",
    RESET_DEMO: "重置演示"
  };
  let labRequestInFlight = false;

  function renderLab(lab, runtimes, mesh) {
    const enabled = Boolean(lab.enabled && lab.controls_enabled);
    $("lab-banner").classList.toggle("hidden", !enabled);
    $("lab-panel").classList.toggle("hidden", !enabled);
    if (!enabled) return;

    $("lab-session").textContent = lab.session_id || "未命名会话";
    const membershipByNode = new Map();
    for (const runtime of runtimes || []) {
      const nodeID = String(runtime.node_id || "");
      if (!nodeID) continue;
      const existing = membershipByNode.get(nodeID);
      membershipByNode.set(nodeID, preferredMembership(existing, runtime));
    }
    const managed = lab.runtimes || [];
    $("lab-runtime-list").innerHTML = managed.length ? managed.map((runtime) => {
      const nodeID = String(runtime.node_id || "");
      const processState = normalize(runtime.process_state);
      const processRunning = processState === "RUNNING";
      const membership = membershipByNode.get(nodeID) || {};
      const encodedNodeID = encodeURIComponent(nodeID);
      return `<article class="lab-runtime-card">
        <div class="row-between"><div><strong>${escapeText(nodeID)}</strong><small>${escapeText(runtime.control_endpoint || "")}</small></div>${badge(processState || "UNKNOWN")}</div>
        <div class="lab-state-grid">
          <div><span class="state-label">Harness / 进程状态</span><strong>${valueText(processState || "UNKNOWN")}</strong><small>PID=${escapeText(runtime.pid || "—")}</small></div>
          <div><span class="state-label">DTM / 成员状态</span><strong>${badge(membership.membership_state || "UNKNOWN")}</strong><small>${membership.coordinator ? "当前协调者" : "非协调者"}</small></div>
        </div>
        <div class="lab-runtime-actions">
          <button class="lab-button" data-lab-action="/api/lab/runtimes/${escapeAttr(encodedNodeID)}/start" ${processRunning ? "disabled" : ""}>启动</button>
          <button class="lab-button danger" data-lab-action="/api/lab/runtimes/${escapeAttr(encodedNodeID)}/stop" data-lab-confirm="确认停止 Runtime ${escapeAttr(nodeID)} 进程？" ${processRunning ? "" : "disabled"}>停止</button>
          <button class="lab-button" data-lab-action="/api/lab/runtimes/${escapeAttr(encodedNodeID)}/restart" data-lab-confirm="确认重启 Runtime ${escapeAttr(nodeID)}？" ${processRunning ? "" : "disabled"}>重启</button>
        </div>
        ${runtime.error ? `<div class="lab-error">${escapeText(runtime.error)}</div>` : ""}
      </article>`;
    }).join("") : `<div class="empty">暂无 Harness Runtime。</div>`;

    const coordinator = normalize(mesh.coordinator);
    const coordinatorButton = document.querySelector('[data-lab-action="/api/lab/coordinator/stop"]');
    if (coordinatorButton) coordinatorButton.disabled = !coordinator || ["NONE", "UNKNOWN", "CONFLICT"].includes(coordinator);
    const operation = lab.last_operation;
    $("lab-operation").innerHTML = operation
      ? `最近操作：<strong>${escapeText(operationLabels[operation.type] || operation.type)}</strong> · ${badge(operation.status)}${operation.target ? ` · 目标=${escapeText(operation.target)}` : ""}${operation.task_id ? ` · Task=${escapeText(operation.task_id)}` : ""}${operation.message ? ` · ${escapeText(operation.message)}` : ""}`
      : "尚未执行 LAB 操作。";
    wireLabActions();
  }

  function preferredMembership(existing, candidate) {
    if (!existing) return candidate;
    const rank = (runtime) => ({ ACTIVE: 3, SUSPECT: 2, EXPIRED: 1 }[normalize(runtime.membership_state)] || 0);
    const existingRank = rank(existing);
    const candidateRank = rank(candidate);
    if (candidateRank !== existingRank) return candidateRank > existingRank ? candidate : existing;
    if (Boolean(candidate.coordinator) !== Boolean(existing.coordinator)) {
      return candidate.coordinator ? candidate : existing;
    }
    return existing;
  }

  function wireLabActions() {
    document.querySelectorAll("[data-lab-action]").forEach((button) => {
      button.onclick = () => postLab(button.dataset.labAction, button.dataset.labConfirm || "", button);
    });
  }

  async function postLab(path, confirmation, sourceButton) {
    if (labRequestInFlight || sourceButton.disabled) return;
    if (confirmation && !window.confirm(confirmation)) return;
    labRequestInFlight = true;
    document.querySelectorAll("[data-lab-action]").forEach((button) => { button.disabled = true; });
    $("lab-operation").textContent = "正在执行 LAB 操作……";
    try {
      const response = await fetch(path, { method: "POST", headers: { Accept: "application/json" }, cache: "no-store" });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.error || `LAB 操作 HTTP ${response.status}`);
      const operation = body.operation || {};
      $("lab-operation").textContent = `${operationLabels[operation.type] || "LAB 操作"}：${labelFor(operation.status)}${operation.message ? `，${operation.message}` : ""}`;
    } catch (error) {
      $("lab-operation").textContent = `LAB 操作失败：${error.message}`;
    } finally {
      labRequestInFlight = false;
      await refresh();
    }
  }

  async function refresh() {
    try {
      const response = await fetch("/api/snapshot", { cache: "no-store" });
      if (!response.ok) throw new Error(`snapshot HTTP ${response.status}`);
      render(await response.json());
    } catch (error) {
      $("alert").textContent = `页面请求异常：${error.message}`;
      $("alert").classList.remove("hidden");
      $("refresh-dot").className = "status-dot bad";
    }
  }

  refresh();
  window.setInterval(refresh, 1000);
})();
