const state = {
  metaClients: [],
  googleClients: [],
  catalogGroups: [],
  catalogFlowEvents: [],
  templates: { channels: {}, variables: {}, filters: {} },
  eventsByClient: new Map(),
  eventStream: null,
  metaCatalog: {
    accounts: [],
    pages: [],
    sourceAccounts: "",
    sourcePages: "",
    warningsAccounts: [],
    warningsPages: [],
  },
};

const DASHBOARD_BASE = (() => {
  const raw = (document.querySelector('meta[name="dashboard-base"]')?.getAttribute("content") || "").trim();
  if (raw) return raw.replace(/\/+$/, "");
  const path = window.location.pathname || "/";
  const m = path.match(/^(\/dash)(?=\/|$)/i);
  if (m) return m[1];
  return "";
})();
const apiUrl = (path) => {
  const base = DASHBOARD_BASE;
  const p = path.startsWith("/") ? path : `/${path}`;
  if (!base) return p;
  return `${base.replace(/\/+$/, "")}${p}`;
};

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

/** Regex inválidas nos chips (mesmo critério case-insensitive do backend). */
function invalidRegexPatterns(hiddenInput) {
  const vals = parseCsvValue(hiddenInput?.value || "");
  const bad = [];
  for (const pat of vals) {
    try {
      new RegExp(pat, "i");
    } catch {
      bad.push(pat);
    }
  }
  return bad;
}

function fillMetaSelect(selectEl, items, preferredValue, emptyLabel) {
  if (!selectEl) return;
  const prev =
    preferredValue !== undefined && preferredValue !== null && String(preferredValue).trim() !== ""
      ? String(preferredValue).trim()
      : String(selectEl.value || "").trim();
  const known = new Set([""]);
  selectEl.replaceChildren();
  const ph = document.createElement("option");
  ph.value = "";
  ph.textContent = emptyLabel;
  selectEl.appendChild(ph);
  (Array.isArray(items) ? items : []).forEach((it) => {
    const id = String(it.id || "").trim();
    if (!id || known.has(id)) return;
    known.add(id);
    const o = document.createElement("option");
    o.value = id;
    o.textContent = it.label || id;
    o.title = id;
    selectEl.appendChild(o);
  });
  if (prev && !known.has(prev)) {
    const o = document.createElement("option");
    o.value = prev;
    const short = prev.length > 44 ? `${prev.slice(0, 22)}…${prev.slice(-18)}` : prev;
    o.textContent = `${short} (fora do catálogo)`;
    o.title = prev;
    selectEl.appendChild(o);
  }
  if (prev && [...selectEl.options].some((o) => o.value === prev)) selectEl.value = prev;
}

function updateMetaCatalogHint() {
  const el = document.getElementById("metaCatalogHint");
  if (!el) return;
  const mc = state.metaCatalog || {};
  const sa = mc.sourceAccounts || "—";
  const sp = mc.sourcePages || "—";
  const bits = [`Catálogo: contas (${sa}) · páginas (${sp})`];
  const wa = (mc.warningsAccounts || []).filter(Boolean);
  const wp = (mc.warningsPages || []).filter(Boolean);
  if (wa.length) bits.push(wa.join(" "));
  if (wp.length) bits.push(wp.join(" "));
  el.textContent = bits.join(" · ");
}

function syncMetaCatalogSelects() {
  const accItems = state.metaCatalog?.accounts || [];
  const pageItems = state.metaCatalog?.pages || [];

  document.querySelectorAll("select.meta-catalog-account-select").forEach((sel) => {
    let preferred = String(sel.value || "").trim();
    const card = sel.closest(".client-card");
    if (card?.dataset?.clientId) {
      const cl = state.metaClients.find((x) => String(x.id) === String(card.dataset.clientId));
      if (cl?.ad_account_id) preferred = String(cl.ad_account_id).trim();
    }
    fillMetaSelect(sel, accItems, preferred, "— Escolher conta —");
  });

  document.querySelectorAll("select.meta-catalog-page-select").forEach((sel) => {
    let preferred = String(sel.value || "").trim();
    const card = sel.closest(".client-card");
    if (card?.dataset?.clientId) {
      const cl = state.metaClients.find((x) => String(x.id) === String(card.dataset.clientId));
      if (cl?.meta_page_id) preferred = String(cl.meta_page_id).trim();
    }
    fillMetaSelect(sel, pageItems, preferred, "(Opcional) — escolher página —");
  });
}

async function fetchMetaCatalogs() {
  try {
    const [ra, rp] = await Promise.all([
      dashFetch(apiUrl("/api/meta-catalog/ad-accounts")),
      dashFetch(apiUrl("/api/meta-catalog/pages")),
    ]);
    const a = await ra.json().catch(() => ({}));
    const p = await rp.json().catch(() => ({}));
    state.metaCatalog = {
      accounts: Array.isArray(a.items) ? a.items : [],
      pages: Array.isArray(p.items) ? p.items : [],
      sourceAccounts: a.source || "fallback",
      sourcePages: p.source || "fallback",
      warningsAccounts: Array.isArray(a.warnings) ? a.warnings : [],
      warningsPages: Array.isArray(p.warnings) ? p.warnings : [],
    };
    updateMetaCatalogHint();
  } catch (e) {
    console.error(e);
    state.metaCatalog = {
      accounts: [],
      pages: [],
      sourceAccounts: "fallback",
      sourcePages: "fallback",
      warningsAccounts: [String(e)],
      warningsPages: [],
    };
    updateMetaCatalogHint();
  }
  syncMetaCatalogSelects();
}

function bindFiltersHelpModal() {
  const dlg = document.getElementById("filtersHelpDialog");
  if (!dlg) return;
  if (dlg.dataset.filtersHelpBound === "1") return;
  dlg.dataset.filtersHelpBound = "1";
  let lastFocus = null;

  const trapFocus = (ev) => {
    if (ev.key !== "Tab" || dlg.hidden) return;
    const focusables = dlg.querySelectorAll(
      'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
    );
    const list = [...focusables].filter((n) => !n.hasAttribute("disabled") && n.offsetParent !== null);
    if (!list.length) return;
    const first = list[0];
    const last = list[list.length - 1];
    if (ev.shiftKey && document.activeElement === first) {
      ev.preventDefault();
      last.focus();
    } else if (!ev.shiftKey && document.activeElement === last) {
      ev.preventDefault();
      first.focus();
    }
  };

  const onKey = (ev) => {
    if (dlg.hidden) return;
    if (ev.key === "Escape") {
      close();
      return;
    }
    if (ev.key === "Tab" && dlg.contains(document.activeElement)) trapFocus(ev);
  };

  function open() {
    if (!dlg.hidden) return;
    lastFocus = document.activeElement;
    dlg.hidden = false;
    dlg.setAttribute("aria-hidden", "false");
    const btn = dlg.querySelector(".filters-help-close-btn");
    (btn || dlg).focus();
    document.addEventListener("keydown", onKey);
  }

  function close() {
    dlg.hidden = true;
    dlg.setAttribute("aria-hidden", "true");
    document.removeEventListener("keydown", onKey);
    if (lastFocus && typeof lastFocus.focus === "function") lastFocus.focus();
  }

  /* Delegação: botões "?" vivem também em cards clonados do template (não existem no DOM no primeiro bindUI). */
  document.addEventListener("click", (ev) => {
    const opener = ev.target.closest?.('[data-open="filters-help"]');
    if (!opener) return;
    ev.preventDefault();
    open();
  });

  dlg.querySelectorAll("[data-filters-help-dismiss]").forEach((el) => {
    el.addEventListener("click", (ev) => {
      if (ev.target === el) close();
    });
  });
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
    editForm.elements.group_id.value = client.group_id || "";
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
      const badRx = invalidRegexPatterns(editForm.querySelector('input[name="lead_exclude_regex"]'));
      if (badRx.length) {
        editFeedback.textContent = `Corrija a(s) regex inválida(s) antes de salvar: ${badRx.join(", ")}`;
        return;
      }
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
  syncMetaCatalogSelects();
}

function renderGoogleClients() {
  const grid = document.getElementById("googleClientsGrid");
  const tpl = document.getElementById("googleClientCardTemplate");
  grid.innerHTML = "";
  state.googleClients.forEach((client) => {
    const node = tpl.content.cloneNode(true);
    const card = node.querySelector(".client-card");
    card.dataset.clientId = String(client.id);
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
  syncCatalogGroupSelects();
}

async function fetchGoogleClients() {
  const r = await dashFetch(apiUrl("/api/google-clients"));
  if (!r.ok) throw new Error("Falha ao carregar clientes Google");
  const data = await r.json();
  state.googleClients = data.clients || [];
  buildStats("googleStatsRow", state.googleClients);
  renderGoogleClients();
  syncCatalogGroupSelects();
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
  const badRx = invalidRegexPatterns(form.querySelector('input[name="lead_exclude_regex"]'));
  if (badRx.length) {
    feedback.textContent = `Corrija a(s) regex inválida(s): ${badRx.join(", ")}`;
    return;
  }
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
  const en = form.querySelector('[name="enabled"]');
  if (en && "checked" in en) en.checked = true;
  populateLeadTemplateSelect(document.getElementById("newClientLeadTemplate"), "default");
  setupChipFields(form, ["lead_exclude_fields", "lead_exclude_contains", "lead_exclude_regex"]);
  syncCatalogGroupSelects();
  syncMetaCatalogSelects();
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
  const enG = form.querySelector('[name="enabled"]');
  if (enG && "checked" in enG) enG.checked = true;
  syncCatalogGroupSelects();
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
  const badRx = invalidRegexPatterns(form.querySelector('input[name="exclude_regex"]'));
  if (badRx.length) {
    feedback.textContent = `Corrija a(s) regex inválida(s): ${badRx.join(", ")}`;
    return;
  }
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

function isCatalogFlowEvent(ev) {
  return String(ev.source || "").toLowerCase().startsWith("catalog_");
}

function pushCatalogFlowEvent(ev) {
  if (!isCatalogFlowEvent(ev)) return;
  const list = state.catalogFlowEvents || [];
  list.push(ev);
  state.catalogFlowEvents = list.slice(-120);
  if (document.getElementById("tab-groups")?.classList.contains("is-active")) {
    renderCatalogFlow();
  }
}

function catalogFlowItem(ev) {
  const li = document.createElement("li");
  li.className = `catalog-flow-item ${stageClass(ev.status)}`;
  const agent = String(ev.source || "")
    .replace(/^catalog_/i, "")
    .trim() || "agente";
  const head = document.createElement("div");
  head.className = "catalog-flow-head";
  const spAgent = document.createElement("span");
  spAgent.className = "catalog-flow-agent";
  spAgent.textContent = agent;
  const spStage = document.createElement("span");
  spStage.className = "catalog-flow-stage";
  spStage.textContent = ev.stage || "";
  const spTime = document.createElement("span");
  spTime.className = "catalog-flow-time";
  spTime.textContent = fmtTime(ev.timestamp);
  head.append(spAgent, spStage, spTime);
  const det = document.createElement("div");
  det.className = "catalog-flow-detail";
  let t = ev.detail || "";
  const gid = (ev.group_id || "").trim();
  if (gid) t = t ? `${t} · ${gid}` : gid;
  det.textContent = t;
  li.append(head, det);
  return li;
}

function renderCatalogFlow() {
  const ul = document.getElementById("catalogFlowList");
  if (!ul) return;
  ul.innerHTML = "";
  const items = [...(state.catalogFlowEvents || [])].reverse();
  if (!items.length) {
    const li = document.createElement("li");
    li.className = "catalog-flow-empty";
    li.textContent =
      "Sem eventos do catálogo ainda. Abra esta aba durante um POST da Evolution ou aguarde o stream.";
    ul.appendChild(li);
    return;
  }
  for (const ev of items) {
    ul.appendChild(catalogFlowItem(ev));
  }
}

async function fetchCatalogFlowHistory() {
  const r = await dashFetch(apiUrl("/api/events/recent?limit=400"));
  const body = await r.json().catch(() => ({}));
  if (!r.ok) return;
  const all = Array.isArray(body.events) ? body.events : [];
  const cat = all.filter(isCatalogFlowEvent);
  state.catalogFlowEvents = cat.slice(-120);
  renderCatalogFlow();
}

async function ensureCatalogFlowHydrated() {
  if ((state.catalogFlowEvents || []).length) {
    renderCatalogFlow();
    return;
  }
  await fetchCatalogFlowHistory();
}

function applyIncomingEvent(ev) {
  pushCatalogFlowEvent(ev);
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

function setCatalogFeedback(message, kind) {
  const el = document.getElementById("catalogGroupsFeedback");
  if (!el) return;
  const text = (message || "").trim();
  el.textContent = text;
  el.hidden = !text;
  el.dataset.state = text ? kind || "info" : "";
}

function setGroupsStatus(message) {
  const el = document.getElementById("catalogGroupsStatus");
  if (!el) return;
  el.textContent = message || "";
}

function updateGroupsCount(n) {
  const el = document.getElementById("catalogGroupsCount");
  if (!el) return;
  el.textContent = Number.isFinite(n) ? String(n) : "—";
}

function applyCatalogListenerUI(listening) {
  const badge = document.getElementById("catalogListenerBadge");
  const onBtn = document.getElementById("catalogListenerOnBtn");
  const offBtn = document.getElementById("catalogListenerOffBtn");
  if (badge) {
    badge.dataset.state = listening ? "on" : "off";
    badge.textContent = listening ? "Escuta ativa" : "Escuta pausada";
  }
  if (onBtn) onBtn.disabled = !!listening;
  if (offBtn) offBtn.disabled = !listening;
}

async function fetchCatalogListenerState() {
  const res = await dashFetch(apiUrl("/api/catalog-groups/webhook-listener"));
  const data = await res.json().catch(() => ({}));
  if (!res.ok) return;
  applyCatalogListenerUI(!!data.listening);
}

async function setCatalogListenerState(listening) {
  const res = await dashFetch(apiUrl("/api/catalog-groups/webhook-listener"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ listening }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    setCatalogFeedback(data.error || "Falha ao mudar escuta do webhook.", "error");
    return;
  }
  applyCatalogListenerUI(!!data.listening);
  setCatalogFeedback(
    data.listening
      ? "Escuta ligada: eventos do catálogo voltam a ser processados."
      : "Escuta pausada: o servidor responde 200 OK à Evolution, mas não grava nem pede nomes.",
    "success",
  );
  setTimeout(() => setCatalogFeedback("", ""), 4000);
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
        ensureCatalogFlowHydrated().catch((e) => console.error(e));
        fetchCatalogGroups().catch((e) => console.error(e));
        fetchCatalogListenerState().catch((e) => console.error(e));
      }
      if (btn.dataset.tab === "meta" || btn.dataset.tab === "google") {
        fetchCatalogGroups().catch((e) => console.error(e));
      }
    });
  });
}

function formatCatalogGroupLabel(g) {
  const sub = (g.subject || "").trim() || "Sem nome";
  const jid = (g.group_jid || "").trim();
  if (!jid) return sub;
  const short = jid.length > 42 ? `${jid.slice(0, 20)}…${jid.slice(-18)}` : jid;
  return `${sub} — ${short}`;
}

function syncCatalogGroupSelects() {
  const groups = Array.isArray(state.catalogGroups) ? state.catalogGroups : [];
  const activeGroups = groups.filter((g) => !!g?.monitoring_enabled);
  const optionsSource = activeGroups.length ? activeGroups : groups;
  document.querySelectorAll("select.catalog-group-select").forEach((sel) => {
    const optional = sel.dataset.catalogOptional === "1";
    let prev = (sel.value || "").trim();
    const card = sel.closest(".client-card");
    const fieldName = String(sel.name || "").trim();
    if (card?.dataset?.clientId) {
      const cid = String(card.dataset.clientId);
      const metaClient = state.metaClients.find((x) => String(x.id) === cid);
      const googleClient = state.googleClients.find((x) => String(x.id) === cid);
      if (metaClient && fieldName === "group_id") prev = String(metaClient.group_id || "").trim();
      if (metaClient && fieldName === "lead_group_id") prev = String(metaClient.lead_group_id || "").trim();
      if (googleClient && fieldName === "group_id") prev = String(googleClient.group_id || "").trim();
    }
    const known = new Set([""]);
    sel.replaceChildren();
    const ph = document.createElement("option");
    ph.value = "";
    ph.textContent = optional ? "(Opcional) sem grupo lead" : "— Escolher do catálogo —";
    sel.appendChild(ph);
    optionsSource.forEach((g) => {
      const jid = (g.group_jid || "").trim();
      if (!jid || known.has(jid)) return;
      known.add(jid);
      const o = document.createElement("option");
      o.value = jid;
      o.textContent = formatCatalogGroupLabel(g);
      o.title = jid;
      sel.appendChild(o);
    });
    if (prev && !known.has(prev)) {
      const o = document.createElement("option");
      o.value = prev;
      o.textContent = `${prev.length > 48 ? `${prev.slice(0, 24)}…` : prev} (fora do catálogo)`;
      o.title = prev;
      sel.appendChild(o);
    }
    const hasPrev = prev && [...sel.options].some((o) => o.value === prev);
    if (hasPrev) sel.value = prev;
  });
}

async function fetchCatalogGroups() {
  setCatalogFeedback("", "");
  setGroupsStatus("A sincronizar…");
  const btn = document.getElementById("refreshCatalogGroupsBtn");
  if (btn) btn.disabled = true;
  try {
    const res = await dashFetch(apiUrl("/api/catalog-groups"));
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const parts = [data.error, data.hint].filter(Boolean);
      if (!parts.length && res.status === 404) {
        parts.push(
          "Rota da API nao encontrada. Se a Pulseboard abre em /dash/, defina DASHBOARD_URL_PREFIX=/dash no Easypanel ou actualize o deploy.",
        );
      }
      setCatalogFeedback(parts.join(" — ") || "Falha ao carregar grupos.", "error");
      setGroupsStatus("");
      updateGroupsCount(state.catalogGroups.length);
      return;
    }
    state.catalogGroups = Array.isArray(data.groups) ? data.groups : [];
    renderCatalogGroups();
    syncCatalogGroupSelects();
    const n = state.catalogGroups.length;
    setGroupsStatus(n ? `Lista actualizada · ${n} ${n === 1 ? "grupo" : "grupos"}` : "Lista vazia — aguardando eventos do webhook.");
    setCatalogFeedback("", "");
  } finally {
    if (btn) btn.disabled = false;
  }
}

function renderCatalogGroups() {
  const wrap = document.getElementById("catalogGroupsWrap");
  if (!wrap) return;
  const rows = state.catalogGroups;
  updateGroupsCount(rows.length);
  if (!rows.length) {
    wrap.innerHTML = `<div class="catalog-empty-state" role="status">
      <div class="catalog-empty-orb" aria-hidden="true">◎</div>
      <h3 class="catalog-empty-title">Sem grupos ainda</h3>
      <p class="catalog-empty-text">
        Quando a Evolution enviar eventos de conversas em grupos (<code>@g.us</code>), os JIDs aparecem aqui.
        Confirme o webhook e o secret na documentação acima.
      </p>
    </div>`;
    return;
  }
  const esc = (s) =>
    String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  const cards = rows
    .map((g, idx) => {
      const jid = esc(g.group_jid);
      const rawSub = String(g.subject || "").trim();
      const subVal = esc(rawSub);
      const laRaw = g.last_activity_at || "";
      const laDisp = laRaw
        ? esc(new Date(laRaw).toLocaleString("pt-BR"))
        : `<span class="catalog-meta-dash">—</span>`;
      const evRaw = String(g.last_event_type || "").trim();
      const ev = evRaw ? esc(evRaw) : `<span class="catalog-meta-dash">—</span>`;
      const mon = !!g.monitoring_enabled;
      const monLabel = mon ? "Ao vivo" : "Pausado";
      const titleId = `catalog-card-h-${idx}`;
      return `<article class="catalog-group-card ${mon ? "is-live" : "is-paused"}" data-group-jid="${jid}" style="--i:${idx}" aria-labelledby="${titleId}">
        <div class="catalog-card-gutter" aria-hidden="true"></div>
        <div class="catalog-card-inner">
          <header class="catalog-card-head">
            <div class="catalog-card-head-top">
              <span id="${titleId}" class="catalog-card-kicker">Grupo WhatsApp</span>
              <div class="catalog-card-status" data-live="${mon ? "1" : "0"}">
                <span class="catalog-status-dot" aria-hidden="true"></span>
                <span class="catalog-status-text">${monLabel}</span>
              </div>
            </div>
            <input type="text" class="catalog-subject-input catalog-card-subject" value="${subVal}" data-jid="${jid}" placeholder="Nome ou etiqueta interna…" autocomplete="off" />
            <p class="catalog-card-jid-line"><code class="catalog-jid catalog-jid-pill" translate="no">${jid}</code></p>
          </header>
          <dl class="catalog-card-meta">
            <div class="catalog-meta-cell"><dt>Última actividade</dt><dd>${laDisp}</dd></div>
            <div class="catalog-meta-cell"><dt>Último evento</dt><dd class="catalog-meta-event">${ev}</dd></div>
          </dl>
          <footer class="catalog-card-foot">
            <label class="catalog-switch catalog-card-switch">
              <input type="checkbox" class="catalog-mon" data-jid="${jid}" ${mon ? "checked" : ""} aria-label="Monitorar eventos deste grupo no catálogo" />
              <span class="catalog-switch-track" aria-hidden="true"><span class="catalog-switch-thumb"></span></span>
              <span class="catalog-switch-label">Monitorar</span>
            </label>
            <div class="catalog-card-actions">
              <button type="button" class="catalog-chip catalog-copy" data-jid="${jid}">Copiar JID</button>
              <button type="button" class="catalog-chip catalog-refresh" data-jid="${jid}">Nome API</button>
              <button type="button" class="catalog-chip catalog-chip-primary catalog-save-sub" data-jid="${jid}">Guardar nome</button>
              <button type="button" class="catalog-chip catalog-chip-danger catalog-delete" data-jid="${jid}">Remover</button>
            </div>
          </footer>
        </div>
      </article>`;
    })
    .join("");
  wrap.innerHTML = `<div class="catalog-card-list" role="list">${cards}</div>`;

  wrap.querySelectorAll(".catalog-copy").forEach((btn) => {
    btn.addEventListener("click", () => {
      const j = btn.getAttribute("data-jid");
      if (!j) return;
      navigator.clipboard?.writeText(j).then(
        () => {
          setCatalogFeedback("JID copiado para a área de transferência.", "success");
          setTimeout(() => setCatalogFeedback("", ""), 2000);
        },
        () => {},
      );
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
        if (!res.ok) {
          setCatalogFeedback(data.error || "Falha ao atualizar nome.", "error");
        } else {
          setCatalogFeedback(
            data.fetched ? "Nome actualizado pela Evolution." : "API não devolveu subject.",
            data.fetched ? "success" : "info",
          );
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
      const card = cb.closest(".catalog-group-card");
      const applyMonitorVisual = (on) => {
        if (!card) return;
        card.classList.toggle("is-live", on);
        card.classList.toggle("is-paused", !on);
        const pill = card.querySelector(".catalog-card-status");
        const txt = card.querySelector(".catalog-status-text");
        if (pill) pill.setAttribute("data-live", on ? "1" : "0");
        if (txt) txt.textContent = on ? "Ao vivo" : "Pausado";
      };
      const res = await dashFetch(apiUrl("/api/catalog-groups"), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ group_jid: j, monitoring_enabled: cb.checked }),
      });
      if (!res.ok) {
        cb.checked = !cb.checked;
        applyMonitorVisual(cb.checked);
        setCatalogFeedback("Falha ao guardar monitoramento.", "error");
      } else {
        const row = (state.catalogGroups || []).find((x) => String(x.group_jid || "").trim() === String(j || "").trim());
        if (row) row.monitoring_enabled = !!cb.checked;
        applyMonitorVisual(cb.checked);
        syncCatalogGroupSelects();
        setCatalogFeedback("Monitoramento actualizado.", "success");
        setTimeout(() => setCatalogFeedback("", ""), 2200);
      }
    });
  });
  wrap.querySelectorAll(".catalog-save-sub").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const j = btn.getAttribute("data-jid");
      if (!j) return;
      const card = btn.closest(".catalog-group-card");
      const inp = card?.querySelector(".catalog-subject-input");
      const subject = (inp?.value || "").trim();
      const res = await dashFetch(apiUrl("/api/catalog-groups"), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ group_jid: j, subject }),
      });
      if (res.ok) {
        setCatalogFeedback("Nome guardado.", "success");
        await fetchCatalogGroups();
      } else {
        setCatalogFeedback("Falha ao guardar nome.", "error");
      }
    });
  });
  wrap.querySelectorAll(".catalog-delete").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const j = btn.getAttribute("data-jid");
      if (!j) return;
      const card = btn.closest(".catalog-group-card");
      const nameHint = (card?.querySelector(".catalog-card-subject")?.value || "").trim() || j;
      const ok = window.confirm(
        `Remover este grupo do catálogo na Pulseboard?\n\n${nameHint}\n\nO registo deixa de aparecer aqui; novos eventos da Evolution podem voltar a criá-lo.`,
      );
      if (!ok) return;
      btn.disabled = true;
      try {
        const res = await dashFetch(apiUrl("/api/catalog-groups"), {
          method: "DELETE",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ group_jid: j }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          setCatalogFeedback(data.error || "Falha ao remover grupo.", "error");
          return;
        }
        setCatalogFeedback("Grupo removido do catálogo.", "success");
        await fetchCatalogGroups();
      } finally {
        btn.disabled = false;
      }
    });
  });
}

function bindUI() {
  bindTabs();
  bindFiltersHelpModal();
  document.getElementById("newClientForm").addEventListener("submit", submitNewMetaClient);
  document.getElementById("newGoogleClientForm").addEventListener("submit", submitNewGoogleClient);
  document.getElementById("templateForm").addEventListener("submit", saveTemplate);
  document.getElementById("filtersForm").addEventListener("submit", saveFilters);
  document.getElementById("refreshBtn").addEventListener("click", fetchMetaClients);
  document.getElementById("refreshGoogleBtn").addEventListener("click", fetchGoogleClients);
  document.getElementById("refreshTemplatesBtn").addEventListener("click", fetchTemplates);
  const rCat = document.getElementById("refreshCatalogGroupsBtn");
  if (rCat) rCat.addEventListener("click", () => fetchCatalogGroups().catch((e) => console.error(e)));
  document.getElementById("catalogListenerOnBtn")?.addEventListener("click", () =>
    setCatalogListenerState(true).catch((e) => console.error(e)),
  );
  document.getElementById("catalogListenerOffBtn")?.addEventListener("click", () =>
    setCatalogListenerState(false).catch((e) => console.error(e)),
  );
  document.getElementById("previewBtn").addEventListener("click", generateTemplatePreview);
  document.getElementById("tplChannel").addEventListener("change", (ev) => renderTemplateVariables(ev.target.value));
  setupChipFields(document.getElementById("newClientForm"), [
    "lead_exclude_fields",
    "lead_exclude_contains",
    "lead_exclude_regex",
  ]);
  setupChipFields(document.getElementById("filtersForm"), ["exclude_exact", "exclude_contains", "exclude_regex"]);
}

async function boot() {
  bindUI();
  await Promise.all([
    fetchMetaClients(),
    fetchGoogleClients(),
    fetchTemplates(),
    fetchCatalogGroups(),
    fetchMetaCatalogs(),
  ]);
  connectStream();
}

boot().catch((err) => {
  console.error(err);
  setConnection("offline", "Falha ao iniciar");
});
