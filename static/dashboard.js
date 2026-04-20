const state = {
  metaClients: [],
  googleClients: [],
  catalogGroups: [],
  templates: { channels: {}, variables: {}, filters: {} },
  eventsByClient: new Map(),
  eventStream: null,
};

const DASHBOARD_BASE =
  (document.querySelector('meta[name="dashboard-base"]')?.getAttribute("content") || "").replace(/\/+$/, "");
const apiUrl = (path) => `${DASHBOARD_BASE}${path}`;

function dashFetch(input, init) {
  const next = { credentials: "same-origin", ...(init || {}) };
  return fetch(input, next).then((r) => {
    if (r.status === 401) {
      const base = DASHBOARD_BASE || "";
      window.location.href = base ? `${base.replace(/\/+$/, "")}/login` : "/login";
    }
    return r;
  });
}

const flowSteps = ["RECEBIDO", "PAYLOAD_OK", "ROTA_RESOLVIDA", "MENSAGEM_FORMATADA", "WHATSAPP_ENVIADO_OK", "CONCLUIDO_OK"];

function parseCsvValue(value) {
  if (!value) return [];
  return String(value)
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function uniqueKeepOrder(values) {
  const seen = new Set();
  const out = [];
  values.forEach((v) => {
    const key = v.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push(v);
  });
  return out;
}

function stringifyCsv(values) {
  return uniqueKeepOrder(values).join(", ");
}

/** IDs de template Meta Lead conhecidos no backend (integrados). */
const META_LEAD_BUILTIN_IDS = ["default", "lorena", "pratical_life"];

function metaLeadTemplateBucket() {
  const ch = state.templates?.channels?.meta_lead;
  return ch && typeof ch === "object" ? ch : {};
}

/** Preenche o select `lead_template` com integrados e templates do arquivo/API. */
function populateLeadTemplateSelect(selectEl, currentValue) {
  if (!selectEl) return;
  const cur = String(currentValue || "default").trim() || "default";
  const bucket = metaLeadTemplateBucket();
  selectEl.innerHTML = "";

  const mkOptgroup = (label) => {
    const og = document.createElement("optgroup");
    og.label = label;
    return og;
  };

  const builtinOg = mkOptgroup("Integrados");
  META_LEAD_BUILTIN_IDS.forEach((id) => {
    const entry = bucket[id];
    const text = entry?.name || id;
    const opt = document.createElement("option");
    opt.value = id;
    opt.textContent = text;
    builtinOg.appendChild(opt);
  });
  selectEl.appendChild(builtinOg);

  const customIds = Object.keys(bucket)
    .filter((id) => !META_LEAD_BUILTIN_IDS.includes(id))
    .sort((a, b) => {
      const na = (bucket[a]?.name || a).toLowerCase();
      const nb = (bucket[b]?.name || b).toLowerCase();
      return na.localeCompare(nb, "pt-BR");
    });

  if (customIds.length) {
    const customOg = mkOptgroup("Personalizados (aba Templates)");
    customIds.forEach((id) => {
      const entry = bucket[id];
      const opt = document.createElement("option");
      opt.value = id;
      opt.textContent = entry?.name ? `${entry.name} · ${id}` : id;
      customOg.appendChild(opt);
    });
    selectEl.appendChild(customOg);
  }

  const known = new Set([...META_LEAD_BUILTIN_IDS, ...Object.keys(bucket)]);
  if (!known.has(cur)) {
    const orphan = document.createElement("option");
    orphan.value = cur;
    orphan.textContent = `ID salvo no cliente (não listado): ${cur}`;
    selectEl.insertBefore(orphan, selectEl.firstChild);
  }
  selectEl.value = cur;
}

function refreshLeadTemplateSelects() {
  const newSel = document.getElementById("newClientLeadTemplate");
  if (newSel) populateLeadTemplateSelect(newSel, newSel.value || "default");

  document.querySelectorAll('.edit-form select[name="lead_template"]').forEach((sel) => {
    const card = sel.closest(".client-card");
    const cid = card?.dataset?.clientId;
    const client = state.metaClients.find((c) => String(c.id) === String(cid));
    populateLeadTemplateSelect(sel, client?.lead_template || sel.value || "default");
  });
}

function ensureChipControl(form, fieldName) {
  const hidden = form.querySelector(`input[name="${fieldName}"]`);
  const control = form.querySelector(`.chips-control[data-chip-for="${fieldName}"]`);
  if (!hidden || !control) return;

  const listEl = control.querySelector(".chips-list");
  const entry = control.querySelector(".chips-entry");
  if (!listEl || !entry) return;

  const render = () => {
    listEl.innerHTML = "";
    const values = parseCsvValue(hidden.value);
    values.forEach((value, index) => {
      const chip = document.createElement("span");
      chip.className = "chip-tag";
      chip.innerHTML = `<span>${value}</span>`;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "chip-remove";
      btn.setAttribute("aria-label", `Remover ${value}`);
      btn.textContent = "×";
      btn.addEventListener("click", () => {
        const next = parseCsvValue(hidden.value).filter((_, idx) => idx !== index);
        hidden.value = stringifyCsv(next);
        render();
      });
      chip.appendChild(btn);
      listEl.appendChild(chip);
    });
  };

  const addEntryValue = () => {
    const raw = (entry.value || "").trim();
    if (!raw) return;
    const parts = raw
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);
    const current = parseCsvValue(hidden.value);
    hidden.value = stringifyCsv([...current, ...parts]);
    entry.value = "";
    render();
  };

  if (control.dataset.ready !== "1") {
    entry.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter" || ev.key === ",") {
        ev.preventDefault();
        addEntryValue();
        return;
      }
      if (ev.key === "Backspace" && !entry.value) {
        const current = parseCsvValue(hidden.value);
        if (!current.length) return;
        hidden.value = stringifyCsv(current.slice(0, -1));
        render();
      }
    });
    entry.addEventListener("blur", addEntryValue);
    control.addEventListener("click", () => entry.focus());
    control.dataset.ready = "1";
  }

  control._chipRender = render;
  render();
}

function setupChipFields(form, names) {
  names.forEach((name) => ensureChipControl(form, name));
}

function fmtTime(iso) {
  if (!iso) return "--:--:--";
  return new Date(iso).toLocaleTimeString("pt-BR");
}

function setConnection(status, label) {
  document.getElementById("connectionDot").className = `dot ${status}`;
  document.getElementById("connectionLabel").textContent = label;
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
  li.innerHTML = `<div class="event-head"><span class="event-stage">${ev.stage || "EVENTO"}</span><span class="event-time">${fmtTime(ev.timestamp)}</span></div><div class="event-detail">${ev.detail || ""}</div>`;
  return li;
}

function normalizeClientEvents(clientName) {
  const list = state.eventsByClient.get(clientName) || [];
  return [...list].sort((a, b) => (a.timestamp || "").localeCompare(b.timestamp || "")).slice(-18);
}

function injectFlowPlaceholders(listElement, events) {
  const done = new Set(events.map((e) => e.stage));
  for (const step of flowSteps) {
    if (done.has(step)) continue;
    const li = document.createElement("li");
    li.className = "event-item st-info";
    li.innerHTML = `<div class="event-head"><span class="event-stage">${step}</span><span class="event-time">pendente</span></div><div class="event-detail">Aguardando execução dessa etapa.</div>`;
    listElement.appendChild(li);
    if (listElement.children.length >= 8) break;
  }
}

function buildStats(containerId, clients, statusKey = "status_label") {
  const active = clients.filter((c) => c.enabled).length;
  const full = clients.filter((c) => c.checks?.[statusKey] === "Ativo completo").length;
  const partial = clients.filter((c) => c.checks?.[statusKey] === "Ativo parcial").length;
  const bad = clients.filter((c) => c.checks?.[statusKey] === "Inconsistente").length;
  const row = document.getElementById(containerId);
  row.innerHTML = "";
  [
    ["Clientes totais", clients.length],
    ["Automações ligadas", active],
    ["Ativo completo", full + partial],
    ["Inconsistentes", bad],
  ].forEach(([label, value]) => {
    const div = document.createElement("div");
    div.className = "stat";
    div.innerHTML = `<strong>${value}</strong><span>${label}</span>`;
    row.appendChild(div);
  });
}

function renderMetaClients() {
  const grid = document.getElementById("clientsGrid");
  const tpl = document.getElementById("clientCardTemplate");
  grid.innerHTML = "";

  state.metaClients.forEach((client) => {
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
    const events = normalizeClientEvents(client.client_name);
    events.reverse().forEach((ev) => list.appendChild(eventItem(ev)));
    injectFlowPlaceholders(list, events);

    const editForm = card.querySelector(".edit-form");
    const editFeedback = card.querySelector(".edit-feedback");
    editForm.elements.client_name.value = client.client_name || "";
    editForm.elements.ad_account_id.value = client.ad_account_id || "";
    editForm.elements.group_id.value = client.group_id || "";
    editForm.elements.meta_page_id.value = client.meta_page_id || "";
    editForm.elements.lead_group_id.value = client.lead_group_id || "";
    editForm.elements.lead_phone_number.value = client.lead_phone_number || "";
    populateLeadTemplateSelect(editForm.querySelector('select[name="lead_template"]'), client.lead_template);
    editForm.elements.lead_exclude_fields.value = (client.lead_exclude_fields || []).join(", ");
    editForm.elements.lead_exclude_contains.value = (client.lead_exclude_contains || []).join(", ");
    editForm.elements.lead_exclude_regex.value = (client.lead_exclude_regex || []).join(", ");
    editForm.elements.enabled.checked = !!client.enabled;
    setupChipFields(editForm, ["lead_exclude_fields", "lead_exclude_contains", "lead_exclude_regex"]);

    card.querySelector('[data-action="toggle-edit"]').addEventListener("click", () => {
      editForm.classList.toggle("hidden");
      editFeedback.textContent = "";
    });
    card.querySelector('[data-action="cancel-edit"]').addEventListener("click", () => {
      editForm.classList.add("hidden");
      editFeedback.textContent = "";
    });
    editForm.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      editFeedback.textContent = "Salvando alterações...";
      const fd = new FormData(editForm);
      const payload = Object.fromEntries(fd.entries());
      payload.enabled = !!fd.get("enabled");
      const resp = await dashFetch(apiUrl(`/api/clients/${client.id}`), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = await resp.json();
      if (!resp.ok || !body.ok) {
        editFeedback.textContent = `Erro ao salvar: ${body.error || "desconhecido"}`;
        return;
      }
      editFeedback.textContent = "Cliente atualizado com sucesso.";
      editForm.classList.add("hidden");
      await fetchMetaClients();
    });

    card.querySelectorAll(".actions button[data-scenario]").forEach((btn) => {
      btn.addEventListener("click", () => simulateHarness(client.id, btn.dataset.scenario));
    });

    grid.appendChild(node);
  });
}

function renderGoogleClients() {
  const grid = document.getElementById("googleClientsGrid");
  const tpl = document.getElementById("googleClientCardTemplate");
  grid.innerHTML = "";
  state.googleClients.forEach((client) => {
    const node = tpl.content.cloneNode(true);
    const card = node.querySelector(".client-card");
    card.querySelector(".client-name").textContent = client.client_name || "(sem nome)";
    const statusLabel = client.checks?.status_label || "Inconsistente";
    const pill = card.querySelector(".status-pill");
    pill.className = `status-pill ${statusPillClass(statusLabel)}`;
    pill.textContent = statusLabel;
    card.querySelector(".g-google_customer_id").textContent = client.google_customer_id || "-";
    card.querySelector(".g-group_id").textContent = client.group_id || "-";
    card.querySelector(".g-google_template").textContent = client.google_template || "default";
    card.querySelector(".g-enabled").textContent = client.enabled ? "true" : "false";
    card.querySelector(".g-primary_conversions").textContent = (client.primary_conversions || []).join(", ") || "(vazio)";
    card.querySelector(".g-notes").textContent = client.notes || "(sem notas)";
    const checks = card.querySelector(".checks");
    checks.appendChild(checkPill("customer_id", !!client.checks?.customer_id_ok));
    checks.appendChild(checkPill("group_id", !!client.checks?.group_id_ok));

    const editForm = card.querySelector(".edit-form");
    const feedback = card.querySelector(".edit-feedback");
    editForm.elements.client_name.value = client.client_name || "";
    editForm.elements.google_customer_id.value = client.google_customer_id || "";
    editForm.elements.group_id.value = client.group_id || "";
    editForm.elements.google_template.value = client.google_template || "default";
    editForm.elements.primary_conversions.value = (client.primary_conversions || []).join(", ");
    editForm.elements.notes.value = client.notes || "";
    editForm.elements.enabled.checked = !!client.enabled;

    card.querySelector('[data-action="toggle-edit-google"]').addEventListener("click", () => {
      editForm.classList.toggle("hidden");
      feedback.textContent = "";
    });
    card.querySelector('[data-action="cancel-edit-google"]').addEventListener("click", () => {
      editForm.classList.add("hidden");
      feedback.textContent = "";
    });
    editForm.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      feedback.textContent = "Salvando alterações...";
      const fd = new FormData(editForm);
      const payload = Object.fromEntries(fd.entries());
      payload.enabled = !!fd.get("enabled");
      const resp = await dashFetch(apiUrl(`/api/google-clients/${client.id}`), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = await resp.json();
      if (!resp.ok || !body.ok) {
        feedback.textContent = `Erro ao salvar: ${body.error || "desconhecido"}`;
        return;
      }
      feedback.textContent = "Cliente Google atualizado com sucesso.";
      editForm.classList.add("hidden");
      await fetchGoogleClients();
    });

    grid.appendChild(node);
  });
}

function renderTemplateVariables(channel) {
  const vars = state.templates.variables?.[channel] || {};
  const box = document.getElementById("tplVars");
  box.innerHTML = "";
  Object.entries(vars).forEach(([key, label]) => {
    const pill = document.createElement("button");
    pill.type = "button";
    pill.className = "var-pill";
    pill.textContent = `{{${key}}}`;
    pill.title = label;
    pill.addEventListener("click", () => {
      const textarea = document.querySelector('#templateForm textarea[name="content"]');
      const insertion = `{{${key}}}`;
      const start = textarea.selectionStart || textarea.value.length;
      const end = textarea.selectionEnd || textarea.value.length;
      textarea.value = `${textarea.value.slice(0, start)}${insertion}${textarea.value.slice(end)}`;
      textarea.focus();
      textarea.selectionStart = textarea.selectionEnd = start + insertion.length;
    });
    box.appendChild(pill);
  });
}

function renderTemplatesCatalog() {
  const root = document.getElementById("templatesCatalog");
  root.innerHTML = "";
  const channels = state.templates.channels || {};
  Object.entries(channels).forEach(([channel, bucket]) => {
    const section = document.createElement("section");
    section.className = "tpl-channel-box";
    section.innerHTML = `<h3>${channel}</h3>`;
    const list = document.createElement("div");
    list.className = "tpl-items";
    Object.entries(bucket || {}).forEach(([templateId, data]) => {
      const card = document.createElement("article");
      card.className = "tpl-item";
      card.innerHTML = `<h4>${templateId}</h4><p>${data.name || ""}</p><pre>${data.content || ""}</pre>`;
      card.addEventListener("click", () => {
        const form = document.getElementById("templateForm");
        form.elements.channel.value = channel;
        form.elements.template_id.value = templateId;
        form.elements.name.value = data.name || templateId;
        form.elements.description.value = data.description || "";
        form.elements.content.value = data.content || "";
        renderTemplateVariables(channel);
      });
      list.appendChild(card);
    });
    section.appendChild(list);
    root.appendChild(section);
  });
}

function renderFiltersForm() {
  const form = document.getElementById("filtersForm");
  if (!form) return;
  const rules = state.templates.filters?.meta_lead || {};
  form.elements.exclude_exact.value = (rules.exclude_exact || []).join(", ");
  form.elements.exclude_contains.value = (rules.exclude_contains || []).join(", ");
  form.elements.exclude_regex.value = (rules.exclude_regex || []).join(", ");
  setupChipFields(form, ["exclude_exact", "exclude_contains", "exclude_regex"]);
}

async function fetchMetaClients() {
  const r = await dashFetch(apiUrl("/api/clients"));
  if (!r.ok) throw new Error("Falha ao carregar clientes Meta");
  const data = await r.json();
  state.metaClients = data.clients || [];
  state.eventsByClient.clear();
  for (const c of state.metaClients) state.eventsByClient.set(c.client_name, c.events || []);
  buildStats("statsRow", state.metaClients);
  renderMetaClients();
}

async function fetchGoogleClients() {
  const r = await dashFetch(apiUrl("/api/google-clients"));
  if (!r.ok) throw new Error("Falha ao carregar clientes Google");
  const data = await r.json();
  state.googleClients = data.clients || [];
  buildStats("googleStatsRow", state.googleClients);
  renderGoogleClients();
}

async function fetchTemplates() {
  const r = await dashFetch(apiUrl("/api/message-templates"));
  if (!r.ok) throw new Error("Falha ao carregar templates");
  const data = await r.json();
  state.templates = data;
  renderTemplateVariables(document.getElementById("tplChannel").value);
  renderTemplatesCatalog();
  renderFiltersForm();
  refreshLeadTemplateSelects();
}

async function submitNewMetaClient(ev) {
  ev.preventDefault();
  const form = ev.currentTarget;
  const feedback = document.getElementById("formFeedback");
  feedback.textContent = "Enviando...";
  const fd = new FormData(form);
  const payload = Object.fromEntries(fd.entries());
  payload.enabled = !!fd.get("enabled");
  const r = await dashFetch(apiUrl("/api/clients"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await r.json();
  if (!r.ok || !body.ok) {
    feedback.textContent = `Erro: ${body.error || "nao foi possível salvar"}`;
    return;
  }
  feedback.textContent = "Cliente Meta adicionado com sucesso.";
  form.reset();
  form.querySelector('input[name="enabled"]').checked = true;
  populateLeadTemplateSelect(document.getElementById("newClientLeadTemplate"), "default");
  setupChipFields(form, ["lead_exclude_fields", "lead_exclude_contains"]);
  await fetchMetaClients();
}

async function submitNewGoogleClient(ev) {
  ev.preventDefault();
  const form = ev.currentTarget;
  const feedback = document.getElementById("googleFormFeedback");
  feedback.textContent = "Enviando...";
  const fd = new FormData(form);
  const payload = Object.fromEntries(fd.entries());
  payload.enabled = !!fd.get("enabled");
  const r = await dashFetch(apiUrl("/api/google-clients"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await r.json();
  if (!r.ok || !body.ok) {
    feedback.textContent = `Erro: ${body.error || "nao foi possível salvar"}`;
    return;
  }
  feedback.textContent = "Cliente Google adicionado com sucesso.";
  form.reset();
  form.querySelector('input[name="enabled"]').checked = true;
  await fetchGoogleClients();
}

async function saveTemplate(ev) {
  ev.preventDefault();
  const form = ev.currentTarget;
  const feedback = document.getElementById("templateFeedback");
  feedback.textContent = "Salvando template...";
  const fd = new FormData(form);
  const payload = Object.fromEntries(fd.entries());
  const channel = payload.channel;
  const templateId = payload.template_id;
  const r = await dashFetch(apiUrl(`/api/message-templates/${encodeURIComponent(channel)}/${encodeURIComponent(templateId)}`), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await r.json();
  if (!r.ok || !body.ok) {
    feedback.textContent = `Erro: ${body.error || "falha ao salvar template"}`;
    return;
  }
  feedback.textContent = "Template salvo com sucesso.";
  await fetchTemplates();
}

async function saveFilters(ev) {
  ev.preventDefault();
  const form = ev.currentTarget;
  const feedback = document.getElementById("filtersFeedback");
  feedback.textContent = "Salvando filtros...";
  const payload = Object.fromEntries(new FormData(form).entries());
  const r = await dashFetch(apiUrl("/api/message-filters/meta_lead"), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await r.json();
  if (!r.ok || !body.ok) {
    feedback.textContent = `Erro: ${body.error || "falha ao salvar filtros"}`;
    return;
  }
  feedback.textContent = "Filtros globais salvos com sucesso.";
  await fetchTemplates();
}

async function generateTemplatePreview() {
  const form = document.getElementById("templateForm");
  const payload = Object.fromEntries(new FormData(form).entries());
  const sampleContext = {
    client_name: "Cliente Exemplo",
    page_id: "102086421781424",
    template_id: "default",
    nome: "Maria da Silva",
    email: "maria@email.com",
    whatsapp: "https://wa.me/5511999999999",
    telefone_digitos: "5511999999999",
    form_name: "Formulário Principal",
    respostas: "*interesse:* Plano Premium\n*cidade:* São Paulo",
    respostas_filtradas: "*interesse:* Plano Premium\n*cidade:* São Paulo",
    respostas_raw: "*utm_source:* {{site_source_name}}\n*referencia:* AP29\n*interesse:* Plano Premium\n*cidade:* São Paulo",
    respostas_omitidas: "utm_source, referencia",
    respostas_count: "2",
    respostas_raw_count: "4",
    respostas_omitidas_count: "2",
    received_at: "20/04/2026 11:42:00",
    customer_id: "253-906-3374",
    period_start_br: "01/04/2026",
    period_end_br: "07/04/2026",
    conversions_block: "- Formulário: 12\n- WhatsApp: 8",
    campaigns_block: "1) *Campanha Busca*\n👁️ Impressoes: 12.300\n🖱️ Cliques: 550",
  };
  const r = await dashFetch(apiUrl("/api/message-templates/preview"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content: payload.content || "", context: sampleContext }),
  });
  const body = await r.json();
  document.getElementById("tplPreview").textContent = body.preview || "";
}

async function simulateHarness(clientId, scenario) {
  const r = await dashFetch(apiUrl("/api/harness/simulate-webhook"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ client_id: clientId, scenario }),
  });
  const body = await r.json();
  if (!r.ok || !body.ok) alert(`Falha no harness: ${body.error || "desconhecido"}`);
}

function applyIncomingEvent(ev) {
  const clientName = (ev.client_name || "").trim();
  if (!clientName) return;
  const list = state.eventsByClient.get(clientName) || [];
  list.push(ev);
  state.eventsByClient.set(clientName, list.slice(-22));
  renderMetaClients();
}

function connectStream() {
  if (state.eventStream) state.eventStream.close();
  const es = new EventSource(apiUrl("/api/events/stream"));
  state.eventStream = es;
  es.addEventListener("open", () => setConnection("live", "Stream ao vivo"));
  es.addEventListener("error", () => setConnection("offline", "Stream desconectado"));
  es.addEventListener("bootstrap", (msg) => {
    try {
      const data = JSON.parse(msg.data);
      for (const ev of data.events || []) applyIncomingEvent(ev);
    } catch (err) {
      console.error(err);
    }
  });
  es.addEventListener("event", (msg) => {
    try {
      applyIncomingEvent(JSON.parse(msg.data));
    } catch (err) {
      console.error(err);
    }
  });
}

function bindTabs() {
  const buttons = Array.from(document.querySelectorAll(".tab-btn"));
  const panels = {
    meta: document.getElementById("tab-meta"),
    google: document.getElementById("tab-google"),
    templates: document.getElementById("tab-templates"),
    groups: document.getElementById("tab-groups"),
  };
  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      buttons.forEach((b) => b.classList.remove("is-active"));
      btn.classList.add("is-active");
      Object.values(panels).forEach((p) => p?.classList.remove("is-active"));
      panels[btn.dataset.tab]?.classList.add("is-active");
      if (btn.dataset.tab === "groups") {
        fetchCatalogGroups().catch((e) => console.error(e));
      }
    });
  });
}

async function fetchCatalogGroups() {
  const fb = document.getElementById("catalogGroupsFeedback");
  if (fb) fb.textContent = "";
  const res = await dashFetch(apiUrl("/api/catalog-groups"));
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    if (fb) fb.textContent = data.error || "Falha ao carregar grupos.";
    return;
  }
  state.catalogGroups = Array.isArray(data.groups) ? data.groups : [];
  renderCatalogGroups();
}

function renderCatalogGroups() {
  const wrap = document.getElementById("catalogGroupsWrap");
  if (!wrap) return;
  const rows = state.catalogGroups;
  if (!rows.length) {
    wrap.innerHTML = `<p class="catalog-empty">Nenhum grupo catalogado ainda. Envie mensagens no grupo com o webhook ativo.</p>`;
    return;
  }
  const esc = (s) =>
    String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  const head = `<table class="catalog-table"><thead><tr>
    <th>Nome</th><th>JID</th><th>Última actividade</th><th>Evento</th><th>Monitorar</th><th></th>
  </tr></thead><tbody>`;
  const body = rows
    .map((g) => {
      const jid = esc(g.group_jid);
      const rawSub = String(g.subject || "").trim();
      const subVal = esc(rawSub);
      const la = esc(g.last_activity_at || "");
      const ev = esc(g.last_event_type || "");
      const mon = !!g.monitoring_enabled;
      return `<tr data-group-jid="${jid}">
        <td><input type="text" class="catalog-subject-input" value="${subVal}" data-jid="${jid}" placeholder="Nome do grupo" /></td>
        <td><code class="catalog-jid">${jid}</code></td>
        <td class="catalog-muted">${la ? new Date(la).toLocaleString("pt-BR") : "—"}</td>
        <td class="catalog-muted">${ev || "—"}</td>
        <td><label class="catalog-toggle"><input type="checkbox" class="catalog-mon" data-jid="${jid}" ${mon ? "checked" : ""} /><span>Activo</span></label></td>
        <td class="catalog-actions">
          <button type="button" class="small ghost catalog-copy" data-jid="${jid}">Copiar JID</button>
          <button type="button" class="small ghost catalog-refresh" data-jid="${jid}">Nome API</button>
          <button type="button" class="small primary catalog-save-sub" data-jid="${jid}">Guardar nome</button>
        </td>
      </tr>`;
    })
    .join("");
  wrap.innerHTML = head + body + `</tbody></table>`;

  wrap.querySelectorAll(".catalog-copy").forEach((btn) => {
    btn.addEventListener("click", () => {
      const j = btn.getAttribute("data-jid");
      if (j) navigator.clipboard?.writeText(j).catch(() => {});
    });
  });
  wrap.querySelectorAll(".catalog-refresh").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const j = btn.getAttribute("data-jid");
      if (!j) return;
      btn.disabled = true;
      try {
        const res = await dashFetch(apiUrl("/api/catalog-groups/refresh"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ group_jid: j }),
        });
        const data = await res.json().catch(() => ({}));
        const fb = document.getElementById("catalogGroupsFeedback");
        if (!res.ok) {
          if (fb) fb.textContent = data.error || "Falha ao atualizar nome.";
        } else if (fb) {
          fb.textContent = data.fetched ? "Nome actualizado pela Evolution." : "API não devolveu subject.";
        }
        await fetchCatalogGroups();
      } finally {
        btn.disabled = false;
      }
    });
  });
  wrap.querySelectorAll(".catalog-mon").forEach((cb) => {
    cb.addEventListener("change", async () => {
      const j = cb.getAttribute("data-jid");
      if (!j) return;
      const res = await dashFetch(apiUrl("/api/catalog-groups"), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ group_jid: j, monitoring_enabled: cb.checked }),
      });
      if (!res.ok) {
        cb.checked = !cb.checked;
        const fb = document.getElementById("catalogGroupsFeedback");
        if (fb) fb.textContent = "Falha ao guardar monitoramento.";
      }
    });
  });
  wrap.querySelectorAll(".catalog-save-sub").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const j = btn.getAttribute("data-jid");
      if (!j) return;
      const row = btn.closest("tr");
      const inp = row?.querySelector(".catalog-subject-input");
      const subject = (inp?.value || "").trim();
      const res = await dashFetch(apiUrl("/api/catalog-groups"), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ group_jid: j, subject }),
      });
      const fb = document.getElementById("catalogGroupsFeedback");
      if (fb) fb.textContent = res.ok ? "Nome guardado." : "Falha ao guardar nome.";
      if (res.ok) await fetchCatalogGroups();
    });
  });
}

function bindUI() {
  bindTabs();
  document.getElementById("newClientForm").addEventListener("submit", submitNewMetaClient);
  document.getElementById("newGoogleClientForm").addEventListener("submit", submitNewGoogleClient);
  document.getElementById("templateForm").addEventListener("submit", saveTemplate);
  document.getElementById("filtersForm").addEventListener("submit", saveFilters);
  document.getElementById("refreshBtn").addEventListener("click", fetchMetaClients);
  document.getElementById("refreshGoogleBtn").addEventListener("click", fetchGoogleClients);
  document.getElementById("refreshTemplatesBtn").addEventListener("click", fetchTemplates);
  const rCat = document.getElementById("refreshCatalogGroupsBtn");
  if (rCat) rCat.addEventListener("click", () => fetchCatalogGroups().catch((e) => console.error(e)));
  document.getElementById("previewBtn").addEventListener("click", generateTemplatePreview);
  document.getElementById("tplChannel").addEventListener("change", (ev) => renderTemplateVariables(ev.target.value));
  setupChipFields(document.getElementById("newClientForm"), ["lead_exclude_fields", "lead_exclude_contains"]);
  setupChipFields(document.getElementById("filtersForm"), ["exclude_exact", "exclude_contains", "exclude_regex"]);
}

async function boot() {
  bindUI();
  await Promise.all([fetchMetaClients(), fetchGoogleClients(), fetchTemplates()]);
  connectStream();
}

boot().catch((err) => {
  console.error(err);
  setConnection("offline", "Falha ao iniciar");
});
