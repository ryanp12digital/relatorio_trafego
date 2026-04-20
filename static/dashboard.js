const state = {
  clients: [],
  eventsByClient: new Map(),
  eventStream: null,
};

const DASHBOARD_BASE =
  (document.querySelector('meta[name="dashboard-base"]')?.getAttribute("content") || "").replace(/\/+$/, "");
const apiUrl = (path) => `${DASHBOARD_BASE}${path}`;

const flowSteps = [
  "RECEBIDO",
  "PAYLOAD_OK",
  "ROTA_RESOLVIDA",
  "MENSAGEM_FORMATADA",
  "WHATSAPP_ENVIADO_OK",
  "CONCLUIDO_OK",
];

function fmtTime(iso) {
  if (!iso) return "--:--:--";
  const d = new Date(iso);
  return d.toLocaleTimeString("pt-BR");
}

function setConnection(status, label) {
  const dot = document.getElementById("connectionDot");
  const txt = document.getElementById("connectionLabel");
  dot.className = `dot ${status}`;
  txt.textContent = label;
}

function normalizeClientEventList(clientName, list) {
  const base = Array.isArray(list) ? list.slice() : [];
  const sorted = base.sort((a, b) => (a.timestamp || "").localeCompare(b.timestamp || ""));
  return sorted.slice(-18);
}

function buildStats(clients) {
  const active = clients.filter((c) => c.enabled).length;
  const full = clients.filter((c) => c.checks?.status_label === "Ativo completo").length;
  const partial = clients.filter((c) => c.checks?.status_label === "Ativo parcial").length;
  const bad = clients.filter((c) => c.checks?.status_label === "Inconsistente").length;
  const statsRow = document.getElementById("statsRow");
  statsRow.innerHTML = "";
  [
    ["Clientes totais", clients.length],
    ["Automações ligadas", active],
    ["Ativo completo", full + partial],
    ["Inconsistentes", bad],
  ].forEach(([label, value]) => {
    const div = document.createElement("div");
    div.className = "stat";
    div.innerHTML = `<strong>${value}</strong><span>${label}</span>`;
    statsRow.appendChild(div);
  });
}

function statusPillClass(label) {
  if (label === "Ativo completo") return "pill-ok";
  if (label === "Ativo parcial" || label === "Pausado") return "pill-warn";
  return "pill-err";
}

function checkPill(name, ok) {
  const span = document.createElement("span");
  span.className = `check-pill ${ok ? "ok" : "error"}`;
  span.textContent = `${ok ? "OK" : "ERRO"} · ${name}`;
  return span;
}

function stageClass(status) {
  const allowed = new Set(["info", "ok", "warning", "error"]);
  return allowed.has(status) ? `st-${status}` : "st-info";
}

function eventItem(ev) {
  const li = document.createElement("li");
  li.className = `event-item ${stageClass(ev.status)}`;
  li.innerHTML = `
    <div class="event-head">
      <span class="event-stage">${ev.stage || "EVENTO"}</span>
      <span class="event-time">${fmtTime(ev.timestamp)}</span>
    </div>
    <div class="event-detail">${ev.detail || ""}</div>
  `;
  return li;
}

function injectFlowPlaceholders(listElement, events) {
  const stagesDone = new Set(events.map((e) => e.stage));
  for (const step of flowSteps) {
    if (stagesDone.has(step)) continue;
    const li = document.createElement("li");
    li.className = "event-item st-info";
    li.innerHTML = `
      <div class="event-head">
        <span class="event-stage">${step}</span>
        <span class="event-time">pendente</span>
      </div>
      <div class="event-detail">Aguardando execução dessa etapa.</div>
    `;
    listElement.appendChild(li);
    if (listElement.children.length >= 8) break;
  }
}

function renderClients() {
  const grid = document.getElementById("clientsGrid");
  const tpl = document.getElementById("clientCardTemplate");
  grid.innerHTML = "";

  state.clients.forEach((client) => {
    const node = tpl.content.cloneNode(true);
    const card = node.querySelector(".client-card");
    card.dataset.clientId = String(client.id);
    card.querySelector(".client-name").textContent = client.client_name || "(sem nome)";

    const statusLabel = client.checks?.status_label || "Inconsistente";
    const pill = card.querySelector(".status-pill");
    pill.className = `status-pill ${statusPillClass(statusLabel)}`;
    pill.textContent = statusLabel;

    card.querySelector(".f-ad_account_id").textContent = client.ad_account_id || "-";
    card.querySelector(".f-group_id").textContent = client.group_id || "-";
    card.querySelector(".f-meta_page_id").textContent = client.meta_page_id || "(vazio)";
    card.querySelector(".f-lead_group_id").textContent = client.lead_group_id || "(fallback group_id)";
    card.querySelector(".f-lead_template").textContent = client.lead_template || "default";
    card.querySelector(".f-enabled").textContent = client.enabled ? "true" : "false";

    const checks = card.querySelector(".checks");
    checks.appendChild(checkPill("ad_account_id", !!client.checks?.ad_account_ok));
    checks.appendChild(checkPill("group_id", !!client.checks?.group_id_ok));
    checks.appendChild(checkPill("meta_page_id", !!client.checks?.meta_page_id_ok));
    checks.appendChild(checkPill("lead_group_id", !!client.checks?.lead_group_id_ok));

    const list = card.querySelector(".event-list");
    const events = normalizeClientEventList(client.client_name, state.eventsByClient.get(client.client_name));
    events.reverse().forEach((ev) => list.appendChild(eventItem(ev)));
    injectFlowPlaceholders(list, events);

    card.querySelectorAll(".actions button").forEach((btn) => {
      btn.addEventListener("click", () => simulateHarness(client.id, btn.dataset.scenario));
    });

    grid.appendChild(node);
  });
}

async function fetchClients() {
  const r = await fetch(apiUrl("/api/clients"));
  if (!r.ok) throw new Error("Falha ao carregar clientes.");
  const data = await r.json();
  state.clients = data.clients || [];
  state.eventsByClient.clear();
  for (const c of state.clients) {
    state.eventsByClient.set(c.client_name, c.events || []);
  }
  buildStats(state.clients);
  renderClients();
}

async function submitNewClient(ev) {
  ev.preventDefault();
  const form = ev.currentTarget;
  const feedback = document.getElementById("formFeedback");
  feedback.textContent = "Enviando...";

  const fd = new FormData(form);
  const payload = Object.fromEntries(fd.entries());
  payload.enabled = !!fd.get("enabled");

  const r = await fetch(apiUrl("/api/clients"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const body = await r.json();
  if (!r.ok || !body.ok) {
    feedback.textContent = `Erro: ${body.error || "nao foi possível salvar"}`;
    return;
  }
  feedback.textContent = "Cliente adicionado com sucesso.";
  form.reset();
  form.querySelector('input[name="enabled"]').checked = true;
  await fetchClients();
}

async function simulateHarness(clientId, scenario) {
  const r = await fetch(apiUrl("/api/harness/simulate-webhook"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ client_id: clientId, scenario }),
  });
  const body = await r.json();
  if (!r.ok || !body.ok) {
    alert(`Falha no harness: ${body.error || "desconhecido"}`);
    return;
  }
}

function applyIncomingEvent(ev) {
  const clientName = (ev.client_name || "").trim();
  if (!clientName) return;
  const list = state.eventsByClient.get(clientName) || [];
  list.push(ev);
  state.eventsByClient.set(clientName, list.slice(-22));
  renderClients();
}

function connectStream() {
  if (state.eventStream) {
    state.eventStream.close();
  }
  const es = new EventSource(apiUrl("/api/events/stream"));
  state.eventStream = es;

  es.addEventListener("open", () => setConnection("live", "Stream ao vivo"));
  es.addEventListener("error", () => setConnection("offline", "Stream desconectado"));
  es.addEventListener("bootstrap", (msg) => {
    try {
      const data = JSON.parse(msg.data);
      for (const ev of data.events || []) {
        applyIncomingEvent(ev);
      }
    } catch (err) {
      console.error(err);
    }
  });
  es.addEventListener("event", (msg) => {
    try {
      const ev = JSON.parse(msg.data);
      applyIncomingEvent(ev);
    } catch (err) {
      console.error(err);
    }
  });
}

function bindUI() {
  document.getElementById("newClientForm").addEventListener("submit", submitNewClient);
  document.getElementById("refreshBtn").addEventListener("click", fetchClients);
}

async function boot() {
  bindUI();
  await fetchClients();
  connectStream();
}

boot().catch((err) => {
  console.error(err);
  setConnection("offline", "Falha ao iniciar");
});
