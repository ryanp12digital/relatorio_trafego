const state = {
  metaClients: [],
  googleClients: [],
  siteLeadRoutes: [],
  catalogGroups: [],
  catalogFlowEvents: [],
  templates: {
    channels: {},
    variables: {},
    filters: {},
    variable_resolution: {},
    custom_variables: {},
    lead_source_key_defaults: {},
    /** Rascunho local de origem (CSV por slot) antes de «Salvar chaves de origem». */
    vrDraftMeta: {},
    vrDraftSite: {},
  },
  activeFilterChannels: [],
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

function escHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

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
  const meth = String((init && init.method) || "GET").toUpperCase();
  const next = {
    credentials: "same-origin",
    ...(meth === "GET" ? { cache: "no-store" } : {}),
    ...(init || {}),
  };
  return fetch(input, next).then((r) => {
    if (r.status === 401) {
      const base = DASHBOARD_BASE || "";
      window.location.href = base ? `${base.replace(/\/+$/, "")}/login` : "/login";
    }
    return r;
  });
}

/** Mensagem legível para falhas HTTP (JSON com error/hint ou HTML/proxy). */
async function dashFetchErrorMessage(res, pathHint = "") {
  const status = res.status;
  const path = pathHint || (() => {
    try {
      const u = new URL(res.url, window.location.origin);
      return (u.pathname || "") + (u.search || "");
    } catch {
      return "API";
    }
  })();
  const prefix = `${path} → HTTP ${status}`;
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  try {
    if (ct.includes("application/json")) {
      const j = await res.json();
      const parts = [j.error, j.hint, j.message].filter(Boolean);
      if (parts.length) return `${prefix}: ${parts.join(" — ")}`;
      return prefix;
    }
    const text = await res.text();
    const snippet = text.replace(/\s+/g, " ").trim().slice(0, 220);
    if (snippet.startsWith("<") || snippet.toLowerCase().includes("<!doctype")) {
      return `${prefix}: resposta não é JSON (HTML/proxy); verifique DASHBOARD_URL_PREFIX e o path.`;
    }
    if (snippet) return `${prefix}: ${snippet}`;
  } catch (e) {
    return `${prefix}: leitura do corpo falhou (${e?.message || e})`;
  }
  return prefix;
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

function compactListText(values, fallback = "—") {
  const arr = Array.isArray(values)
    ? values.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  return arr.length ? arr.join(", ") : fallback;
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
    fillMetaSelect(sel, accItems, preferred, "Nenhum");
  });

  document.querySelectorAll("select.meta-catalog-page-select").forEach((sel) => {
    let preferred = String(sel.value || "").trim();
    const card = sel.closest(".client-card");
    if (card?.dataset?.clientId) {
      const cl = state.metaClients.find((x) => String(x.id) === String(card.dataset.clientId));
      if (cl?.meta_page_id) preferred = String(cl.meta_page_id).trim();
    }
    fillMetaSelect(sel, pageItems, preferred, "Nenhum");
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

const FIELD_COPY_COMMON = {
  client_name: {
    label: "Nome do cliente",
    hint: "Nome exibido no painel e nas mensagens.",
  },
  group_id: {
    label: "Grupo do cliente",
    hint: "WhatsApp que recebe o novo lead do cliente.",
  },
  lead_phone_number: {
    label: "Telefone do cliente",
    hint: "Uso em fluxos extras, quando configurado.",
  },
  lead_template: {
    label: "Template do lead",
    hint: "Texto enviado ao grupo do cliente quando chega um lead.",
  },
  p12_report_group_id: {
    label: "Grupo P12",
    hint: "WhatsApp da equipe para o relatório semanal.",
  },
  p12_report_template: {
    label: "Template P12 (resumo)",
    hint: "Primeira mensagem do relatório semanal.",
  },
  p12_data_report_template: {
    label: "Template P12 (dados)",
    hint: "Segunda mensagem com dados do relatório.",
  },
  internal_notify_group_id: {
    label: "Grupo interno",
    hint: "Cópia para a equipe quando chega um lead.",
  },
  internal_lead_template: {
    label: "Template interno (lead)",
    hint: "Texto da cópia interna quando chega um lead.",
  },
  internal_weekly_template: {
    label: "Template interno (relatório)",
    hint: "Texto interno após o relatório semanal, quando aplicável.",
  },
  lead_exclude_fields: {
    label: "Excluir perguntas (exato)",
    hint: "Igual ao nome ou chave da pergunta; ignora maiúsculas.",
  },
  lead_exclude_contains: {
    label: "Excluir se o nome contiver",
    hint: "Remove a linha se o nome da pergunta contiver este trecho.",
  },
  lead_exclude_regex: {
    label: "Excluir por regex",
    hint: "Regex sobre o nome da pergunta; inválida é ignorada no envio.",
  },
  enabled: {
    label: "Automação ativa",
    hint: "Com pausa, o envio automático deixa de rodar para este cadastro.",
  },
  notes: {
    label: "Observações",
    hint: "Notas internas opcionais.",
  },
};

const FIELD_COPY_CONTEXT = {
  meta: {
    ad_account_id: {
      label: "Conta Meta",
      hint: "Conta de anúncios ligada a este cliente.",
    },
    meta_page_id: {
      label: "Página Meta",
      hint: "Página que identifica o lead no webhook.",
    },
    internal_notify_group_id: {
      hint: "Cópia para a equipe quando chega um lead; também usada em avisos internos configurados.",
    },
  },
  google: {
    google_customer_id: {
      label: "ID Google Ads",
      hint: "Customer ID da conta Google Ads.",
    },
    google_template: {
      label: "Template do lead",
      hint: "Template enviado ao grupo do cliente nos envios Google.",
    },
    primary_conversions: {
      label: "Conversões primárias",
      hint: "Lista CSV usada nos relatórios (ex.: formulário, WhatsApp).",
    },
    internal_notify_group_id: {
      hint: "Grupo que recebe o aviso interno após o relatório semanal.",
    },
  },
  site: {
    codi_id: {
      label: "CODI ID",
      hint: "Identificador do formulário no site (28 a 36 dígitos).",
    },
    cliente_origem: {
      label: "Rótulo interno",
      hint: "Só organização no painel; não roteia envio.",
    },
    origem_anuncio: {
      label: "Origem do anúncio",
      hint: "Referência opcional de campanha ou canal.",
    },
    cors_allowed_origins: {
      label: "Origens CORS",
      hint: "URLs do site autorizadas a chamar o webhook.",
    },
    internal_notify_group_id: {
      hint: "Cópia para a equipe quando chega um lead do site.",
    },
  },
};

function fieldCopyFor(context, fieldName) {
  const common = FIELD_COPY_COMMON[fieldName];
  const specific = (FIELD_COPY_CONTEXT[context] || {})[fieldName];
  if (!common && !specific) return null;
  return {
    label: specific?.label || common?.label || fieldName,
    hint: specific?.hint || common?.hint || "",
  };
}

function fieldLabelText(context, fieldName) {
  return fieldCopyFor(context, fieldName)?.label || fieldName;
}

function fieldCopyDtLabel(context, fieldName) {
  return escHtml(fieldLabelText(context, fieldName));
}

function applyFieldCopy(root, context) {
  if (!root || !context) return;
  root.querySelectorAll("label").forEach((label) => {
    if (label.closest(".chips-control")) return;
    const control = label.querySelector(
      ":scope > input[name], :scope > select[name], :scope > textarea[name]",
    );
    if (!control) return;
    const fieldName = label.dataset.fieldKey || control.getAttribute("name");
    const copy = fieldCopyFor(context, fieldName);
    if (!copy) return;

    let titleEl = label.querySelector(":scope > .field-label-text");
    if (!titleEl) {
      titleEl = document.createElement("span");
      titleEl.className = "field-label-text";
      label.insertBefore(titleEl, control);
      let node = label.firstChild;
      while (node && node !== titleEl) {
        const next = node.nextSibling;
        if (node.nodeType === Node.TEXT_NODE) {
          if (!titleEl.textContent.trim() && node.textContent.trim()) {
            titleEl.textContent = node.textContent.trim();
          }
          label.removeChild(node);
        } else if (
          node.nodeType === Node.ELEMENT_NODE &&
          !node.classList.contains("field-hint") &&
          !node.classList.contains("field-micro")
        ) {
          if (
            !titleEl.textContent.trim() &&
            node.textContent.trim() &&
            !node.matches("input, select, textarea, .chips-control")
          ) {
            titleEl.textContent = node.textContent.trim();
          }
          if (!node.matches("input, select, textarea, .chips-control")) {
            label.removeChild(node);
          }
        }
        node = next;
      }
    }
    titleEl.textContent = copy.label;

    let hintEl = label.querySelector(":scope > .field-hint");
    const microEl = label.querySelector(":scope > .field-micro");
    if (copy.hint) {
      if (microEl && !hintEl) {
        microEl.textContent = copy.hint;
        microEl.classList.add("field-hint");
      } else {
        if (!hintEl) {
          hintEl = document.createElement("span");
          hintEl.className = "field-hint";
          label.insertBefore(hintEl, control);
        }
        hintEl.textContent = copy.hint;
      }
    } else if (hintEl) {
      hintEl.remove();
    }
  });

  root.querySelectorAll(".meta-grid dt[data-field-key]").forEach((dt) => {
    const copy = fieldCopyFor(context, dt.dataset.fieldKey);
    if (copy) dt.textContent = copy.label;
  });
}

function applyAllFieldCopy() {
  document.querySelectorAll("[data-field-context]").forEach((root) => {
    applyFieldCopy(root, root.dataset.fieldContext);
  });
}

function buildCadastroHelpBody(introHtml, sections) {
  const parts = [`<div class="cadastro-help-body"><p>${introHtml}</p>`];
  sections.forEach((section) => {
    parts.push(`<h4>${section.title}</h4><dl class="cadastro-help-defs">`);
    section.fields.forEach((field) => {
      const copy = fieldCopyFor(section.context, field.key);
      const label = escHtml(copy?.label || field.key);
      const desc = field.desc ? escHtml(field.desc) : escHtml(copy?.hint || "");
      parts.push(`<dt>${label}</dt><dd>${desc}</dd>`);
    });
    parts.push("</dl>");
  });
  parts.push("</div>");
  return parts.join("");
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

const MESSAGES_SECTION_HELP = {
  templates_form: {
    title: "Templates de Mensagem",
    body:
      "Crie e edite textos base por canal (meta, site, google, interno). O conteúdo pode usar variáveis no formato {{variavel}} e o preview ajuda a validar antes de salvar.",
  },
  variable_resolution: {
    title: "Origem dos campos (webhook)",
    body:
      "Clique numa variável para abrir o editor: chaves JSON em ordem de tentativa, separadas por vírgula, para Meta/interno (partilhado) e para Site. Campos vazios usam o padrão do sistema. Variáveis só de contexto (ex.: nome do cliente no cadastro) aparecem só para consulta.",
  },
  custom_variables: {
    title: "Variáveis personalizadas",
    body:
      "Permite criar novas variáveis para os templates, mapeando chaves do payload para um valor final amigável. Ideal para traduzir códigos internos em textos legíveis para a mensagem.",
  },
  global_filters: {
    title: "Filtros globais",
    body:
      "Aplica regras por canal para remover perguntas do bloco {{respostas}} em todos os envios. Esses filtros se somam aos filtros configurados individualmente por cliente.",
  },
  templates_catalog: {
    title: "Templates cadastrados",
    body:
      "Mostra todos os templates salvos agrupados por canal. Clique em um item para carregar no formulário de edição e atualizar rapidamente conteúdo, nome ou descrição.",
  },
  cadastro_meta_client: {
    title: "Ajuda: Novo cliente Meta",
    bodyHtml: buildCadastroHelpBody(
      "O cadastro vai para <code>data/clients.json</code>. O catálogo de contas e páginas usa a Business API onde configurado.",
      [
        {
          title: "Cliente",
          context: "meta",
          fields: [
            { key: "client_name" },
            {
              key: "ad_account_id",
              desc: "Contas do Business (API) mais contas já guardadas neste projeto.",
            },
            {
              key: "group_id",
              desc: "Grupo WhatsApp do cliente (lista na aba Grupos WhatsApp · webhook Evolution).",
            },
            {
              key: "meta_page_id",
              desc: "Página ligada aos leads · páginas da API mais páginas já cadastradas.",
            },
            { key: "lead_phone_number" },
          ],
        },
        {
          title: "Template — mensagem ao cliente",
          context: "meta",
          fields: [
            {
              key: "lead_template",
              desc: "Mensagem para o grupo do cliente · integrados ou templates da aba Templates (<code>meta_lead</code>).",
            },
          ],
        },
        {
          title: "Notificações internas",
          context: "meta",
          fields: [
            {
              key: "p12_report_group_id",
              desc: "Onde vai o relatório semanal Meta (resumo + dados) para a equipe P12.",
            },
            {
              key: "p12_report_template",
              desc: "Canal <code>meta_report</code> · em vazio usa <code>default</code>.",
            },
            {
              key: "p12_data_report_template",
              desc: "Segunda mensagem de dados ao grupo P12, canal <code>meta_report</code>.",
            },
            {
              key: "internal_notify_group_id",
              desc: "Cópia opcional para a equipe · texto definido nos templates <code>internal_lead</code> e <code>internal_report</code>.",
            },
            {
              key: "internal_lead_template",
              desc: "Aba Templates, canal <code>internal_lead</code> — mesmas variáveis do lead.",
            },
            {
              key: "internal_weekly_template",
              desc: "Canal <code>internal_report</code>, após o envio P12 Meta.",
            },
          ],
        },
      ],
    ),
  },
  cadastro_google_client: {
    title: "Ajuda: Novo cliente Google Ads",
    bodyHtml: buildCadastroHelpBody("Cadastro em <code>data/google_clients.json</code>.", [
      {
        title: "Cliente",
        context: "google",
        fields: [
          { key: "client_name" },
          { key: "google_customer_id" },
          { key: "group_id", desc: "Lista da aba Grupos WhatsApp." },
          { key: "lead_phone_number" },
          { key: "primary_conversions" },
          { key: "notes" },
        ],
      },
      {
        title: "Template — mensagem ao cliente",
        context: "google",
        fields: [
          {
            key: "google_template",
            desc: "Identificador do template ligado aos envios Google para o cliente (ex.: <code>default</code>).",
          },
        ],
      },
      {
        title: "Notificações internas",
        context: "google",
        fields: [
          { key: "p12_report_group_id", desc: "Destino dos relatórios semanais da equipe P12." },
          {
            key: "p12_report_template",
            desc: "Canal <code>google_report</code> · ex.: <code>p12_resumo</code> · vazio usa <code>default</code>.",
          },
          {
            key: "p12_data_report_template",
            desc: "Segunda mensagem aos P12 (ex.: <code>p12_dados</code>).",
          },
          {
            key: "internal_notify_group_id",
            desc: "Aviso após envio do relatório · conteúdo no canal <code>internal_report</code>.",
          },
          {
            key: "internal_weekly_template",
            desc: "Templates da aba <code>internal_report</code> · variáveis Google.",
          },
        ],
      },
    ]),
  },
  cadastro_site_lead: {
    title: "Ajuda: Leads Site",
    bodyHtml: buildCadastroHelpBody(
      "Roteamento por <code>codi_id</code> · origem Meta/Google pode aparecer em <code>{{traffic_source}}</code> na mensagem.",
      [
        {
          title: "Cliente e roteamento",
          context: "site",
          fields: [
            { key: "codi_id" },
            {
              key: "cliente_origem",
              desc: "Não roteia envio · painel e variáveis <code>{{cliente_origem}}</code> / relacionadas.",
            },
            {
              key: "origem_anuncio",
              desc: "Opcional · qual anúncio ou canal associa a este <code>codi_id</code>.",
            },
            { key: "group_id" },
            { key: "lead_phone_number" },
            { key: "notes" },
            {
              key: "cors_allowed_origins",
              desc: "URLs do site do cliente (uma por linha ou vírgula) · mesma forma que o browser envia no header <code>Origin</code> · funde com <code>META_LEAD_WEBHOOK_CORS_ORIGINS</code> no worker · cadastro pausado não conta.",
            },
          ],
        },
        {
          title: "Template — mensagem ao cliente",
          context: "site",
          fields: [
            {
              key: "lead_template",
              desc: "Canal <code>site_lead</code> na aba Templates.",
            },
          ],
        },
        {
          title: "Notificações internas",
          context: "site",
          fields: [
            { key: "internal_notify_group_id", desc: "Cópia ao time quando chega um lead." },
            {
              key: "internal_lead_template",
              desc: "Canal <code>internal_lead</code> na aba Templates.",
            },
          ],
        },
      ],
    ),
  },
};

function bindMessagesSectionHelpModal() {
  const dlg = document.getElementById("messagesSectionHelpDialog");
  if (!dlg) return;
  if (dlg.dataset.messagesHelpBound === "1") return;
  dlg.dataset.messagesHelpBound = "1";
  let lastFocus = null;

  const titleEl = document.getElementById("messagesSectionHelpTitle");
  const bodyEl = document.getElementById("messagesSectionHelpBody");

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

  function open(helpKey) {
    const content = MESSAGES_SECTION_HELP[helpKey] || MESSAGES_SECTION_HELP.templates_form;
    if (titleEl) titleEl.textContent = content.title;
    if (bodyEl) {
      if (content.bodyHtml) bodyEl.innerHTML = content.bodyHtml;
      else bodyEl.textContent = content.body || "";
    }
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

  document.addEventListener("click", (ev) => {
    const opener = ev.target.closest?.('[data-open="messages-section-help"]');
    if (!opener) return;
    ev.preventDefault();
    open(opener.getAttribute("data-help-key") || "");
  });

  dlg.querySelectorAll("[data-messages-help-dismiss]").forEach((el) => {
    el.addEventListener("click", (ev) => {
      if (ev.target === el) close();
    });
  });
}

function renderFlowList(listEl, clientName) {
  if (!listEl) return;
  listEl.innerHTML = "";
  const events = normalizeClientEvents(clientName);
  events.reverse().forEach((ev) => listEl.appendChild(eventItem(ev)));
  injectFlowPlaceholders(listEl, events);
}

function openFlowModal(clientName) {
  const dlg = document.getElementById("flowDialog");
  const listEl = document.getElementById("flowEventList");
  const line = document.getElementById("flowModalClientLine");
  if (!dlg || !listEl || !line) return;
  const clean = String(clientName || "").trim() || "Cliente";
  dlg.dataset.clientName = clean;
  line.textContent = `Cliente: ${clean}`;
  renderFlowList(listEl, clean);
  dlg.hidden = false;
  dlg.setAttribute("aria-hidden", "false");
  const btn = dlg.querySelector(".flow-modal-close-btn");
  (btn || dlg).focus();
}

function closeFlowModal() {
  const dlg = document.getElementById("flowDialog");
  if (!dlg) return;
  dlg.hidden = true;
  dlg.setAttribute("aria-hidden", "true");
  delete dlg.dataset.clientName;
}

function refreshOpenFlowModal() {
  const dlg = document.getElementById("flowDialog");
  if (!dlg || dlg.hidden) return;
  const clientName = String(dlg.dataset.clientName || "").trim();
  if (!clientName) return;
  renderFlowList(document.getElementById("flowEventList"), clientName);
}

function bindFlowModal() {
  const dlg = document.getElementById("flowDialog");
  if (!dlg) return;
  if (dlg.dataset.bound === "1") return;
  dlg.dataset.bound = "1";

  const onKey = (ev) => {
    if (dlg.hidden) return;
    if (ev.key === "Escape") closeFlowModal();
  };
  document.addEventListener("keydown", onKey);

  dlg.querySelectorAll("[data-flow-dismiss]").forEach((el) => {
    el.addEventListener("click", (ev) => {
      if (ev.target === el || ev.currentTarget === el) closeFlowModal();
    });
  });
}

/** IDs de template Meta Lead conhecidos no backend (integrados). */
const META_LEAD_BUILTIN_IDS = ["default", "pratical_life"];
const SITE_LEAD_BUILTIN_IDS = ["default"];
const META_REPORT_BUILTIN_IDS = ["default", "p12_resumo", "p12_dados"];
const GOOGLE_REPORT_BUILTIN_IDS = ["default", "p12_resumo", "p12_dados"];
const INTERNAL_LEAD_BUILTIN_IDS = ["default"];
const INTERNAL_REPORT_BUILTIN_IDS = ["default"];

function metaLeadTemplateBucket() {
  const ch = state.templates?.channels?.meta_lead;
  return ch && typeof ch === "object" ? ch : {};
}

/** Preenche o select `lead_template` com integrados e templates do arquivo/API. */
function populateLeadTemplateSelect(selectEl, currentValue) {
  if (!selectEl) return;
  const cur =
    currentValue === undefined || currentValue === null
      ? ""
      : String(currentValue).trim();
  const bucket = metaLeadTemplateBucket();
  selectEl.innerHTML = "";

  const none = document.createElement("option");
  none.value = "";
  none.textContent = "Nenhum";
  selectEl.appendChild(none);

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

  const known = new Set([...META_LEAD_BUILTIN_IDS, ...Object.keys(bucket), ""]);
  if (cur && !known.has(cur)) {
    const orphan = document.createElement("option");
    orphan.value = cur;
    orphan.textContent = `ID salvo no cliente (não listado): ${cur}`;
    selectEl.appendChild(orphan);
  }
  if (cur === "") {
    selectEl.value = "";
  } else {
    selectEl.value = cur || "default";
  }
}

function populateSiteLeadTemplateSelect(selectEl, currentValue) {
  const raw = currentValue === undefined || currentValue === null ? "" : String(currentValue).trim();
  populateChannelTemplateSelect(
    selectEl,
    "site_lead",
    SITE_LEAD_BUILTIN_IDS,
    raw === "" ? "" : raw || "default",
    true
  );
}

function channelTemplateBucket(channel) {
  const ch = state.templates?.channels?.[channel];
  return ch && typeof ch === "object" ? ch : {};
}

function listTemplateChannels() {
  return Object.keys(state.templates?.channels || {}).sort((a, b) => a.localeCompare(b, "pt-BR"));
}

/** Alinha com `custom_variables_storage_key` no backend: internal_lead → meta_lead. */
function customVariablesStorageKey(ch) {
  return ch === "internal_lead" ? "meta_lead" : ch;
}

const CV_OPT_LEADS = new Set(["meta_lead", "site_lead", "internal_lead"]);
const CV_OPT_REPORTS = new Set(["google_report", "meta_report", "internal_report"]);

function ensureCustomVarsChannelOptions(selectEl, channels, preferredValue) {
  if (!selectEl) return;
  const prev = String(preferredValue || selectEl.value || "").trim();
  selectEl.innerHTML = "";
  const leads = channels.filter((c) => CV_OPT_LEADS.has(c));
  const reports = channels.filter((c) => CV_OPT_REPORTS.has(c));
  const rest = channels.filter((c) => !CV_OPT_LEADS.has(c) && !CV_OPT_REPORTS.has(c));
  const addGroup = (label, items) => {
    if (!items.length) return;
    const og = document.createElement("optgroup");
    og.label = label;
    items.forEach((c) => {
      const o = document.createElement("option");
      o.value = c;
      o.textContent = c;
      og.appendChild(o);
    });
    selectEl.appendChild(og);
  };
  addGroup("Leads", leads);
  addGroup("Relatórios e interno", reports);
  if (rest.length) addGroup("Outros", rest);
  if (!channels.length) return;
  if (prev && channels.includes(prev)) selectEl.value = prev;
  else selectEl.value = channels[0];
}

function initCvChipsControl(wrapEl) {
  const hidden = wrapEl.querySelector('input[type="hidden"].cv-source-keys');
  const listEl = wrapEl.querySelector(".cv-chips-list");
  const entry = wrapEl.querySelector(".cv-chips-entry");
  if (!hidden || !listEl || !entry) return;
  const notify = () => {
    hidden.dispatchEvent(new Event("input", { bubbles: true }));
  };
  const render = () => {
    listEl.innerHTML = "";
    const values = parseCsvValue(hidden.value);
    values.forEach((value, index) => {
      const chip = document.createElement("span");
      chip.className = "chip-tag";
      chip.innerHTML = `<span>${escHtml(value)}</span>`;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "chip-remove";
      btn.setAttribute("aria-label", `Remover ${value}`);
      btn.textContent = "×";
      btn.addEventListener("click", () => {
        const next = parseCsvValue(hidden.value).filter((_, idx) => idx !== index);
        hidden.value = next.join(", ");
        render();
        notify();
      });
      chip.appendChild(btn);
      listEl.appendChild(chip);
    });
  };
  const addEntryValue = () => {
    const raw = (entry.value || "").trim();
    if (!raw) return;
    const parts = raw
      .split(/[,;]+/)
      .map((x) => x.trim())
      .filter(Boolean);
    const current = parseCsvValue(hidden.value);
    hidden.value = [...current, ...parts].join(", ");
    entry.value = "";
    render();
    notify();
  };
  if (wrapEl.dataset.cvChipsReady !== "1") {
    entry.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter" || ev.key === ",") {
        ev.preventDefault();
        addEntryValue();
        return;
      }
      if (ev.key === "Backspace" && !entry.value) {
        const current = parseCsvValue(hidden.value);
        if (!current.length) return;
        hidden.value = current.slice(0, -1).join(", ");
        render();
        notify();
      }
    });
    entry.addEventListener("blur", addEntryValue);
    wrapEl.addEventListener("click", () => entry.focus());
    wrapEl.dataset.cvChipsReady = "1";
  }
  render();
}

function updateCvCardHead(article) {
  const kn = article.querySelector(".cv-key")?.value?.trim() || "";
  const pre = article.querySelector(".cv-head-preview");
  if (pre) pre.textContent = kn ? `{{${kn}}}` : "(sem nome)";
  const st = article.querySelector(".cv-head-status");
  if (!st) return;
  const sk = article.querySelector('.cv-source-keys')?.value?.trim();
  st.textContent = kn && sk ? "completo" : "incompleto";
  st.className = `cv-head-status${kn && sk ? " is-ok" : " is-warn"}`;
}

function ensureSelectOptions(selectEl, channels, preferredValue) {
  if (!selectEl) return;
  const prev = String(preferredValue || selectEl.value || "").trim();
  selectEl.innerHTML = "";
  channels.forEach((channel) => {
    const opt = document.createElement("option");
    opt.value = channel;
    opt.textContent = channel;
    selectEl.appendChild(opt);
  });
  if (!channels.length) return;
  if (prev && channels.includes(prev)) {
    selectEl.value = prev;
    return;
  }
  selectEl.value = channels[0];
}

/**
 * @param {HTMLSelectElement|null} selectEl
 * @param {string} channel
 * @param {string[]} builtinIds
 * @param {string} currentValue
 * @param {boolean} allowEmpty - primeira opção value="" (template dados opcional)
 */
function populateChannelTemplateSelect(selectEl, channel, builtinIds, currentValue, allowEmpty) {
  if (!selectEl) return;
  const cur = String(currentValue ?? "").trim();
  const bucket = channelTemplateBucket(channel);
  selectEl.innerHTML = "";
  if (allowEmpty) {
    const o = document.createElement("option");
    o.value = "";
    o.textContent = "Nenhum";
    selectEl.appendChild(o);
  }
  const mkOptgroup = (label) => {
    const og = document.createElement("optgroup");
    og.label = label;
    return og;
  };
  const builtinOg = mkOptgroup("Integrados");
  builtinIds.forEach((id) => {
    const entry = bucket[id];
    const opt = document.createElement("option");
    opt.value = id;
    opt.textContent = entry?.name || id;
    builtinOg.appendChild(opt);
  });
  selectEl.appendChild(builtinOg);
  const customIds = Object.keys(bucket)
    .filter((id) => !builtinIds.includes(id))
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
  const known = new Set([...builtinIds, ...Object.keys(bucket), ""]);
  if (cur && !known.has(cur)) {
    const orphan = document.createElement("option");
    orphan.value = cur;
    orphan.textContent = `ID salvo: ${cur}`;
    selectEl.insertBefore(orphan, selectEl.firstChild);
  }
  if (allowEmpty && cur === "") {
    selectEl.value = "";
  } else {
    selectEl.value = cur || builtinIds[0] || "default";
  }
}

function refreshMetaReportTemplateSelects() {
  const newForm = document.getElementById("newClientForm");
  if (newForm) {
    const t1 = newForm.querySelector('select[name="p12_report_template"]');
    const t2 = newForm.querySelector('select[name="p12_data_report_template"]');
    populateChannelTemplateSelect(
      t1,
      "meta_report",
      META_REPORT_BUILTIN_IDS,
      String((t1?.value ?? "").trim()),
      true
    );
    populateChannelTemplateSelect(
      t2,
      "meta_report",
      META_REPORT_BUILTIN_IDS,
      t2?.value || "",
      true
    );
  }
  document.querySelectorAll("#clientsGrid .client-card").forEach((card) => {
    const cid = card.dataset.clientId;
    const client = state.metaClients.find((c) => String(c.id) === String(cid));
    const t1 = card.querySelector('select[name="p12_report_template"]');
    const t2 = card.querySelector('select[name="p12_data_report_template"]');
    populateChannelTemplateSelect(
      t1,
      "meta_report",
      META_REPORT_BUILTIN_IDS,
      String((client?.p12_report_template ?? t1?.value ?? "").trim()),
      true
    );
    populateChannelTemplateSelect(
      t2,
      "meta_report",
      META_REPORT_BUILTIN_IDS,
      client?.p12_data_report_template || t2?.value || "",
      true
    );
  });
}

function refreshInternalTemplateSelects() {
  const newForm = document.getElementById("newClientForm");
  if (newForm) {
    const sLead = newForm.querySelector('select[name="internal_lead_template"]');
    const sWeek = newForm.querySelector('select[name="internal_weekly_template"]');
    populateChannelTemplateSelect(
      sLead,
      "internal_lead",
      INTERNAL_LEAD_BUILTIN_IDS,
      sLead?.value ?? "",
      true
    );
    populateChannelTemplateSelect(
      sWeek,
      "internal_report",
      INTERNAL_REPORT_BUILTIN_IDS,
      sWeek?.value ?? "",
      true
    );
  }
  document.querySelectorAll("#clientsGrid .client-card").forEach((card) => {
    const cid = card.dataset.clientId;
    const client = state.metaClients.find((c) => String(c.id) === String(cid));
    const sLead = card.querySelector('select[name="internal_lead_template"]');
    const sWeek = card.querySelector('select[name="internal_weekly_template"]');
    populateChannelTemplateSelect(
      sLead,
      "internal_lead",
      INTERNAL_LEAD_BUILTIN_IDS,
      client?.internal_lead_template ?? sLead?.value ?? "",
      true
    );
    populateChannelTemplateSelect(
      sWeek,
      "internal_report",
      INTERNAL_REPORT_BUILTIN_IDS,
      client?.internal_weekly_template ?? sWeek?.value ?? "",
      true
    );
  });

  const newG = document.getElementById("newGoogleClientForm");
  if (newG) {
    const gWeek = newG.querySelector('select[name="internal_weekly_template"]');
    populateChannelTemplateSelect(
      gWeek,
      "internal_report",
      INTERNAL_REPORT_BUILTIN_IDS,
      gWeek?.value ?? "",
      true
    );
  }
  document.querySelectorAll("#googleClientsGrid .client-card").forEach((card) => {
    const cid = card.dataset.clientId;
    const client = state.googleClients.find((c) => String(c.id) === String(cid));
    const gWeek = card.querySelector('select[name="internal_weekly_template"]');
    populateChannelTemplateSelect(
      gWeek,
      "internal_report",
      INTERNAL_REPORT_BUILTIN_IDS,
      client?.internal_weekly_template ?? gWeek?.value ?? "",
      true
    );
  });
  const siteForm = document.getElementById("siteLeadRouteForm");
  if (siteForm) {
    const sLead = siteForm.querySelector('select[name="internal_lead_template"]');
    populateChannelTemplateSelect(
      sLead,
      "internal_lead",
      INTERNAL_LEAD_BUILTIN_IDS,
      sLead?.value ?? "",
      true
    );
  }
}

function refreshGoogleP12TemplateSelects() {
  const newG = document.getElementById("newGoogleClientForm");
  if (newG) {
    const t1 = newG.querySelector('select[name="p12_report_template"]');
    const t2 = newG.querySelector('select[name="p12_data_report_template"]');
    populateChannelTemplateSelect(
      t1,
      "google_report",
      GOOGLE_REPORT_BUILTIN_IDS,
      String((t1?.value ?? "").trim()),
      true
    );
    populateChannelTemplateSelect(
      t2,
      "google_report",
      GOOGLE_REPORT_BUILTIN_IDS,
      t2?.value || "",
      true
    );
  }
  document.querySelectorAll("#googleClientsGrid .client-card").forEach((card) => {
    const cid = card.dataset.clientId;
    const client = state.googleClients.find((c) => String(c.id) === String(cid));
    const t1 = card.querySelector('select[name="p12_report_template"]');
    const t2 = card.querySelector('select[name="p12_data_report_template"]');
    populateChannelTemplateSelect(
      t1,
      "google_report",
      GOOGLE_REPORT_BUILTIN_IDS,
      String((client?.p12_report_template ?? t1?.value ?? "").trim()),
      true
    );
    populateChannelTemplateSelect(
      t2,
      "google_report",
      GOOGLE_REPORT_BUILTIN_IDS,
      client?.p12_data_report_template || t2?.value || "",
      true
    );
  });
}

function refreshLeadTemplateSelects() {
  const newSel = document.getElementById("newClientLeadTemplate");
  if (newSel) populateLeadTemplateSelect(newSel, newSel.value || "");
  const siteSel = document.querySelector('#siteLeadRouteForm select[name="lead_template"]');
  if (siteSel) populateSiteLeadTemplateSelect(siteSel, siteSel.value || "");

  document.querySelectorAll('.edit-form select[name="lead_template"]').forEach((sel) => {
    const card = sel.closest(".client-card");
    const cid = card?.dataset?.clientId;
    const client = state.metaClients.find((c) => String(c.id) === String(cid));
    populateLeadTemplateSelect(sel, client?.lead_template ?? sel.value ?? "");
  });
  refreshMetaReportTemplateSelects();
  refreshGoogleP12TemplateSelects();
  refreshInternalTemplateSelects();
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

/** Indicador exclusivo do stream SSE (não confundir com a carga inicial /bootChecklist). */
function setStreamStatus(status, label) {
  const dot = document.getElementById("connectionDot");
  const lab = document.getElementById("connectionLabel");
  if (dot) dot.className = `dot ${status}`;
  if (lab) lab.textContent = label;
}

function setBootChecklistButtonState(steps = []) {
  const btn = document.getElementById("bootChecklistToggle");
  if (!btn) return;
  const items = Array.isArray(steps) ? steps : [];
  const errCount = items.filter((s) => s?.state === "err").length;
  const warnCount = items.filter((s) => s?.state === "warn").length;
  btn.classList.toggle("has-error", errCount > 0);
  if (!items.length) {
    btn.textContent = "Diagnóstico";
    return;
  }
  if (errCount > 0) {
    btn.textContent = `Erros: ${errCount}`;
    return;
  }
  if (warnCount > 0) {
    btn.textContent = `Avisos: ${warnCount}`;
    return;
  }
  btn.textContent = "Tudo OK";
}

function bindBootChecklistTooltip() {
  const btn = document.getElementById("bootChecklistToggle");
  const pop = document.getElementById("bootChecklistPopover");
  if (!btn || !pop) return;
  if (btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";

  const close = () => {
    pop.hidden = true;
    btn.setAttribute("aria-expanded", "false");
  };
  const open = () => {
    pop.hidden = false;
    btn.setAttribute("aria-expanded", "true");
  };

  btn.addEventListener("click", (ev) => {
    ev.stopPropagation();
    if (pop.hidden) open();
    else close();
  });

  document.addEventListener("click", (ev) => {
    const t = ev.target;
    if (!(t instanceof Element)) return;
    if (!pop.hidden && !pop.contains(t) && t !== btn) close();
  });

  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape" && !pop.hidden) close();
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
    const fPhone = card.querySelector(".f-lead_phone_number");
    if (fPhone) fPhone.textContent = client.lead_phone_number || "—";
    const fP12 = card.querySelector(".f-p12_report_group_id");
    if (fP12) fP12.textContent = client.p12_report_group_id || "—";
    const fP12Tpl = card.querySelector(".f-p12_report_template");
    if (fP12Tpl) fP12Tpl.textContent = client.p12_report_template || "—";
    const fP12DataTpl = card.querySelector(".f-p12_data_report_template");
    if (fP12DataTpl) fP12DataTpl.textContent = client.p12_data_report_template || "—";
    const fIntGroup = card.querySelector(".f-internal_notify_group_id");
    if (fIntGroup) fIntGroup.textContent = client.internal_notify_group_id || "—";
    card.querySelector(".f-lead_template").textContent = client.lead_template || "default";
    const fIntLeadTpl = card.querySelector(".f-internal_lead_template");
    if (fIntLeadTpl) fIntLeadTpl.textContent = client.internal_lead_template || "—";
    const fIntWeeklyTpl = card.querySelector(".f-internal_weekly_template");
    if (fIntWeeklyTpl) fIntWeeklyTpl.textContent = client.internal_weekly_template || "—";
    const fExFields = card.querySelector(".f-lead_exclude_fields");
    if (fExFields) fExFields.textContent = compactListText(client.lead_exclude_fields);
    const fExContains = card.querySelector(".f-lead_exclude_contains");
    if (fExContains) fExContains.textContent = compactListText(client.lead_exclude_contains);
    const fExRegex = card.querySelector(".f-lead_exclude_regex");
    if (fExRegex) fExRegex.textContent = compactListText(client.lead_exclude_regex);
    card.querySelector(".f-enabled").textContent = client.enabled ? "true" : "false";

    const checks = card.querySelector(".checks");
    checks.appendChild(checkPill("ad_account_id", !!client.checks?.ad_account_ok));
    checks.appendChild(checkPill("group_id", !!client.checks?.group_id_ok));
    checks.appendChild(checkPill("meta_page_id", !!client.checks?.meta_page_id_ok));
    checks.appendChild(checkPill("telefone_cliente (opcional)", true));
    checks.appendChild(checkPill("p12_report_group_id", !!client.checks?.p12_report_group_id_ok));
    checks.appendChild(checkPill("interno", !!client.checks?.internal_notify_group_id_ok));

    const flowBtn = card.querySelector('[data-action="open-flow"]');
    if (flowBtn) {
      flowBtn.addEventListener("click", () => openFlowModal(client.client_name || "Cliente"));
    }

    const editForm = card.querySelector(".edit-form");
    const editFeedback = card.querySelector(".edit-feedback");
    editForm.elements.client_name.value = client.client_name || "";
    editForm.elements.group_id.value = client.group_id || "";
    editForm.elements.lead_phone_number.value = client.lead_phone_number || "";
    populateLeadTemplateSelect(editForm.querySelector('select[name="lead_template"]'), client.lead_template ?? "");
    editForm.elements.lead_exclude_fields.value = (client.lead_exclude_fields || []).join(", ");
    editForm.elements.lead_exclude_contains.value = (client.lead_exclude_contains || []).join(", ");
    editForm.elements.lead_exclude_regex.value = (client.lead_exclude_regex || []).join(", ");
    editForm.elements.enabled.checked = !!client.enabled;
    if (editForm.elements.p12_report_group_id)
      editForm.elements.p12_report_group_id.value = client.p12_report_group_id || "";
    if (editForm.elements.internal_notify_group_id)
      editForm.elements.internal_notify_group_id.value = client.internal_notify_group_id || "";
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
      let body = {};
      try {
        body = await resp.json();
      } catch {
        editFeedback.textContent = `Erro ao salvar: resposta inválida (HTTP ${resp.status}). Verifique rede / proxy.`;
        return;
      }
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
    applyFieldCopy(card, "meta");
  });
  refreshMetaReportTemplateSelects();
  state.metaClients.forEach((client) => {
    const card = document.querySelector(`#clientsGrid .client-card[data-client-id="${client.id}"]`);
    if (!card) return;
    const ef = card.querySelector(".edit-form");
    if (!ef) return;
    if (ef.elements.p12_report_group_id) ef.elements.p12_report_group_id.value = client.p12_report_group_id || "";
    const t1 = ef.querySelector('[name="p12_report_template"]');
    const t2 = ef.querySelector('[name="p12_data_report_template"]');
    if (t1) t1.value = client.p12_report_template || "default";
    if (t2) t2.value = client.p12_data_report_template ?? "";
    if (ef.elements.internal_notify_group_id) ef.elements.internal_notify_group_id.value = client.internal_notify_group_id || "";
  });
  syncMetaCatalogSelects();
  refreshInternalTemplateSelects();
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
    const gPhone = card.querySelector(".g-lead_phone_number");
    if (gPhone) gPhone.textContent = client.lead_phone_number || "—";
    const gp12 = card.querySelector(".g-p12_report_group_id");
    if (gp12) gp12.textContent = client.p12_report_group_id || "—";
    const gp12Tpl = card.querySelector(".g-p12_report_template");
    if (gp12Tpl) gp12Tpl.textContent = client.p12_report_template || "—";
    const gp12DataTpl = card.querySelector(".g-p12_data_report_template");
    if (gp12DataTpl) gp12DataTpl.textContent = client.p12_data_report_template || "—";
    const gIntGroup = card.querySelector(".g-internal_notify_group_id");
    if (gIntGroup) gIntGroup.textContent = client.internal_notify_group_id || "—";
    const gIntWeeklyTpl = card.querySelector(".g-internal_weekly_template");
    if (gIntWeeklyTpl) gIntWeeklyTpl.textContent = client.internal_weekly_template || "—";
    card.querySelector(".g-google_template").textContent = client.google_template || "default";
    card.querySelector(".g-enabled").textContent = client.enabled ? "true" : "false";
    card.querySelector(".g-primary_conversions").textContent = (client.primary_conversions || []).join(", ") || "(vazio)";
    card.querySelector(".g-notes").textContent = client.notes || "(sem notas)";
    const checks = card.querySelector(".checks");
    checks.appendChild(checkPill("customer_id", !!client.checks?.customer_id_ok));
    checks.appendChild(checkPill("group_id", !!client.checks?.group_id_ok));
    checks.appendChild(checkPill("p12_grupo", !!client.checks?.p12_report_group_id_ok));
    checks.appendChild(checkPill("interno", !!client.checks?.internal_notify_group_id_ok));
    const flowBtn = card.querySelector('[data-action="open-flow"]');
    if (flowBtn) {
      flowBtn.addEventListener("click", () => openFlowModal(client.client_name || "Cliente"));
    }

    const editForm = card.querySelector(".edit-form");
    const feedback = card.querySelector(".edit-feedback");
    editForm.elements.client_name.value = client.client_name || "";
    editForm.elements.google_customer_id.value = client.google_customer_id || "";
    editForm.elements.group_id.value = client.group_id || "";
    editForm.elements.google_template.value = client.google_template || "default";
    editForm.elements.primary_conversions.value = (client.primary_conversions || []).join(", ");
    editForm.elements.notes.value = client.notes || "";
    editForm.elements.enabled.checked = !!client.enabled;
    if (editForm.elements.lead_phone_number) editForm.elements.lead_phone_number.value = client.lead_phone_number || "";
    if (editForm.elements.p12_report_group_id) editForm.elements.p12_report_group_id.value = client.p12_report_group_id || "";
    if (editForm.elements.internal_notify_group_id)
      editForm.elements.internal_notify_group_id.value = client.internal_notify_group_id || "";

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
      let body = {};
      try {
        body = await resp.json();
      } catch {
        feedback.textContent = `Erro ao salvar: resposta inválida (HTTP ${resp.status}). Verifique rede / proxy.`;
        return;
      }
      if (!resp.ok || !body.ok) {
        feedback.textContent = `Erro ao salvar: ${body.error || "desconhecido"}`;
        return;
      }
      feedback.textContent = "Cliente Google atualizado com sucesso.";
      editForm.classList.add("hidden");
      await fetchGoogleClients();
    });

    grid.appendChild(node);
    applyFieldCopy(card, "google");
  });
  refreshGoogleP12TemplateSelects();
  state.googleClients.forEach((client) => {
    const card = document.querySelector(`#googleClientsGrid .client-card[data-client-id="${client.id}"]`);
    if (!card) return;
    const ef = card.querySelector(".edit-form");
    if (!ef) return;
    const t1 = ef.querySelector('[name="p12_report_template"]');
    const t2 = ef.querySelector('[name="p12_data_report_template"]');
    if (t1) t1.value = client.p12_report_template || "default";
    if (t2) t2.value = client.p12_data_report_template ?? "";
    if (ef.elements.p12_report_group_id) ef.elements.p12_report_group_id.value = client.p12_report_group_id || "";
    if (ef.elements.internal_notify_group_id) ef.elements.internal_notify_group_id.value = client.internal_notify_group_id || "";
  });
  refreshInternalTemplateSelects();
}

const LEAD_RESOLVABLE_SLOTS = [
  "nome",
  "email",
  "form_name",
  "whatsapp",
  "telefone_digitos",
  "page_path",
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_term",
  "utm_content",
];

const _VR_NAME_RE = /^[a-zA-Z][a-zA-Z0-9_]*$/;
/** Alineado ao backend: reservados de sistema, não reutilizáveis como origem adicional. */
const VR_EXT_FORBIDDEN = new Set(
  "client_name page_id template_id respostas respostas_filtradas respostas_raw respostas_omitidas respostas_count respostas_raw_count respostas_omitidas_count received_at chegada_em traffic_source traffic_origin_url origem_anuncio cliente_origem"
    .split(/\s+/),
);

function renderTemplateVariables(channel) {
  const vars = state.templates.variables?.[channel] || {};
  const box = document.getElementById("tplVars");
  box.innerHTML = "";
  const addPill = (key, label) => {
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
  };
  Object.entries(vars).forEach(([key, label]) => {
    addPill(key, label);
  });
  const sk = customVariablesStorageKey(channel);
  const customs = state.templates.custom_variables?.[sk] || [];
  customs.forEach((ent) => {
    const k = String(ent?.key || "").trim();
    if (!k) return;
    addPill(k, `Variável personalizada → ${k}`);
  });
}

const LEAD_ORIGIN_CHANNELS = new Set(["meta_lead", "site_lead", "internal_lead"]);

function listLeadOriginChannels() {
  return listTemplateChannels().filter((c) => LEAD_ORIGIN_CHANNELS.has(c));
}

function copyTextToClipboard(text) {
  if (navigator.clipboard?.writeText) {
    return navigator.clipboard.writeText(text).then(
      () => true,
      () => false,
    );
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return Promise.resolve(ok);
  } catch {
    return Promise.resolve(false);
  }
}

function hydrateVrDraftFromTemplates() {
  state.vrDraftMeta = {};
  state.vrDraftSite = {};
  const mergeBucket = (b) => {
    const out = {};
    if (!b || typeof b !== "object") return out;
    for (const [k, meta] of Object.entries(b)) {
      const sk = meta?.source_keys;
      if (Array.isArray(sk) && sk.length) {
        out[k] = sk.join(", ");
      }
    }
    return out;
  };
  Object.assign(state.vrDraftMeta, mergeBucket(state.templates.variable_resolution?.meta_lead));
  Object.assign(state.vrDraftSite, mergeBucket(state.templates.variable_resolution?.site_lead));
}

function allLeadOriginVariableKeys() {
  const keys = new Set();
  listLeadOriginChannels().forEach((ch) => {
    const v = state.templates.variables?.[ch];
    if (v && typeof v === "object") Object.keys(v).forEach((k) => keys.add(k));
  });
  Object.keys(state.vrDraftMeta || {}).forEach((k) => keys.add(k));
  Object.keys(state.vrDraftSite || {}).forEach((k) => keys.add(k));
  return Array.from(keys).sort((a, b) => a.localeCompare(b, "pt-BR"));
}

function isVrConfigurableSlot(key) {
  if (LEAD_RESOLVABLE_SLOTS.includes(key)) return true;
  if (VR_EXT_FORBIDDEN.has(key)) return false;
  return _VR_NAME_RE.test(key);
}

let vrModalSlot = "";
let vrModalIsNew = false;

function closeVrEditDialog() {
  const dlg = document.getElementById("varResolutionEditDialog");
  if (!dlg) return;
  dlg.hidden = true;
  dlg.setAttribute("aria-hidden", "true");
}

function openVrEditDialog(slotKey, isNew = false) {
  vrModalSlot = String(slotKey || "").trim();
  vrModalIsNew = !!isNew;
  const dlg = document.getElementById("varResolutionEditDialog");
  if (!dlg) return;
  const titleEl = document.getElementById("varResEditTitle");
  const subEl = document.getElementById("varResEditSubtitle");
  const readOnly = document.getElementById("varResEditReadonly");
  const editForm = document.getElementById("varResEditForm");
  const newRow = document.getElementById("varResEditNewRow");
  const metaInp = document.getElementById("varResEditMetaKeys");
  const siteInp = document.getElementById("varResEditSiteKeys");
  const newKeysInp = document.getElementById("varResEditNewKeys");
  const nameInp = document.getElementById("varResEditNewName");
  const defMeta = document.getElementById("varResEditDefaultsMeta");
  const defSite = document.getElementById("varResEditDefaultsSite");
  const applyBtn = document.getElementById("varResModalApplyBtn");
  const rmBtn = document.getElementById("varResModalRemoveBtn");

  const dualKeys = document.getElementById("varResEditDualKeys");
  if (isNew) {
    if (titleEl) titleEl.textContent = "Nova variável de origem";
    if (subEl) subEl.textContent = "Crie um nome e as chaves JSON; escolha em que armazenamentos entra (Meta/interno e/ou Site).";
    if (readOnly) readOnly.hidden = true;
    if (editForm) editForm.hidden = false;
    if (newRow) newRow.hidden = false;
    if (dualKeys) dualKeys.hidden = true;
    if (nameInp) nameInp.value = "";
    if (metaInp) metaInp.value = "";
    if (siteInp) siteInp.value = "";
    if (newKeysInp) newKeysInp.value = "";
    const nm = document.getElementById("varResEditNewMeta");
    const ns = document.getElementById("varResEditNewSite");
    if (nm) nm.checked = true;
    if (ns) ns.checked = true;
    if (defMeta) defMeta.textContent = "";
    if (defSite) defSite.textContent = "";
    if (rmBtn) rmBtn.hidden = true;
    if (applyBtn) applyBtn.hidden = false;
  } else {
    const key = vrModalSlot;
    if (titleEl) titleEl.textContent = `{{${key}}}`;
    const desc =
      state.templates.variables?.meta_lead?.[key] ||
      state.templates.variables?.site_lead?.[key] ||
      state.templates.variables?.internal_lead?.[key] ||
      "";
    if (subEl) subEl.textContent = desc || "Variável de lead";
    const conf = isVrConfigurableSlot(key);
    if (readOnly) {
      readOnly.hidden = conf;
      if (!conf) {
        readOnly.innerHTML =
          "<p class=\"field-micro\">Esta variável não é mapeada aqui por lista de chaves no JSON (ex.: dados do cadastro do cliente, blocos montados ou API). Use «Copiar» se precisar do placeholder noutro sítio.</p>";
      }
    }
    if (editForm) editForm.hidden = !conf;
    if (newRow) newRow.hidden = true;
    if (dualKeys) dualKeys.hidden = !conf;
    if (conf) {
      if (metaInp) metaInp.value = state.vrDraftMeta[key] || "";
      if (siteInp) siteInp.value = state.vrDraftSite[key] || "";
      const dm = state.templates.lead_source_key_defaults?.[key];
      const defLine =
        Array.isArray(dm) && dm.length ? `Padrão do sistema (se o campo acima estiver vazio): ${dm.join(", ")}` : "";
      if (defMeta) defMeta.textContent = defLine;
      if (defSite) defSite.textContent = defLine;
    }
    const isExtra = !LEAD_RESOLVABLE_SLOTS.includes(key);
    if (rmBtn) rmBtn.hidden = !conf || !isExtra;
    if (applyBtn) applyBtn.hidden = !conf;
  }
  dlg.hidden = false;
  dlg.setAttribute("aria-hidden", "false");
  if (isNew && nameInp) nameInp.focus();
  else if (metaInp && !editForm?.hidden) metaInp.focus();
}

function applyVrEditDialog() {
  const fb = document.getElementById("varResFeedback");
  const metaInp = document.getElementById("varResEditMetaKeys");
  const siteInp = document.getElementById("varResEditSiteKeys");

  if (vrModalIsNew) {
    const name = (document.getElementById("varResEditNewName")?.value || "").trim();
    const raw = (document.getElementById("varResEditNewKeys")?.value || "").trim();
    const useMeta = document.getElementById("varResEditNewMeta")?.checked;
    const useSite = document.getElementById("varResEditNewSite")?.checked;
    if (!useMeta && !useSite) {
      if (fb) fb.textContent = "Marque pelo menos um destino: Meta/interno ou Site.";
      return;
    }
    if (!name || !raw) {
      if (fb) fb.textContent = "Preencha o nome da variável e as chaves do JSON.";
      return;
    }
    if (!_VR_NAME_RE.test(name)) {
      if (fb) fb.textContent = "Nome de variável inválido: use a-z, 0-9 e _ (começando com letra).";
      return;
    }
    if (LEAD_RESOLVABLE_SLOTS.includes(name)) {
      if (fb) fb.textContent = "Esse nome é um campo padrão — abra o cartão dessa variável na grelha.";
      return;
    }
    if (VR_EXT_FORBIDDEN.has(name)) {
      if (fb) fb.textContent = `O nome «${name}» é reservado ao sistema.`;
      return;
    }
    const keys = raw
      .split(/[,;]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    if (!keys.length) {
      if (fb) fb.textContent = "Indique pelo menos uma chave de JSON.";
      return;
    }
    const joined = keys.join(", ");
    if (useMeta && (state.vrDraftMeta[name] || "").trim()) {
      if (fb) fb.textContent = `Já existe rascunho para «${name}» em Meta/interno. Abra o cartão para editar.`;
      return;
    }
    if (useSite && (state.vrDraftSite[name] || "").trim()) {
      if (fb) fb.textContent = `Já existe rascunho para «${name}» em Site. Abra o cartão para editar.`;
      return;
    }
    if (useMeta) state.vrDraftMeta[name] = joined;
    if (useSite) state.vrDraftSite[name] = joined;
    closeVrEditDialog();
    renderVariableResolutionPanel();
    if (fb) fb.textContent = `«${name}» adicionado ao rascunho. Clique em «Salvar chaves de origem» para gravar no servidor.`;
    return;
  }

  const key = vrModalSlot;
  if (!isVrConfigurableSlot(key)) {
    closeVrEditDialog();
    return;
  }
  const m = (metaInp?.value || "").trim();
  const s = (siteInp?.value || "").trim();
  if (m) state.vrDraftMeta[key] = m;
  else delete state.vrDraftMeta[key];
  if (s) state.vrDraftSite[key] = s;
  else delete state.vrDraftSite[key];
  closeVrEditDialog();
  renderVariableResolutionPanel();
  if (fb) fb.textContent = "Alteração aplicada ao rascunho. Clique em «Salvar chaves de origem» para gravar.";
}

function removeVrExtraFromDraft() {
  const key = vrModalSlot;
  if (!key || LEAD_RESOLVABLE_SLOTS.includes(key)) return;
  delete state.vrDraftMeta[key];
  delete state.vrDraftSite[key];
  closeVrEditDialog();
  renderVariableResolutionPanel();
  const fb = document.getElementById("varResFeedback");
  if (fb) fb.textContent = `«${key}» removido do rascunho. Salve para persistir no servidor.`;
}

let vrEditDialogBound = false;

function setupVarResolutionEditDialog() {
  if (vrEditDialogBound) return;
  vrEditDialogBound = true;
  const dlg = document.getElementById("varResolutionEditDialog");
  if (!dlg) return;
  dlg.querySelectorAll("[data-vr-edit-dismiss]").forEach((el) => {
    el.addEventListener("click", (ev) => {
      ev.preventDefault();
      closeVrEditDialog();
    });
  });
  document.getElementById("varResModalApplyBtn")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    applyVrEditDialog();
  });
  document.getElementById("varResModalRemoveBtn")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    removeVrExtraFromDraft();
  });
  document.getElementById("varResModalCopyBtn")?.addEventListener("click", (ev) => {
    ev.preventDefault();
    const ph = vrModalIsNew ? "" : `{{${vrModalSlot}}}`;
    if (!ph) return;
    copyTextToClipboard(ph).then((ok) => {
      const fb = document.getElementById("varResFeedback");
      if (ok && fb) fb.textContent = `Copiado: ${ph}`;
    });
  });
  document.addEventListener("keydown", (ev) => {
    if (ev.key !== "Escape") return;
    if (dlg.hidden) return;
    closeVrEditDialog();
  });
}

function renderVariableOriginPills() {
  const wrap = document.getElementById("varResolutionPills");
  const hint = document.getElementById("varResolutionPillsHint");
  const empty = document.getElementById("varResolutionEmpty");
  if (!wrap) return;
  wrap.innerHTML = "";
  if (hint) {
    hint.hidden = false;
    hint.textContent = "Clique numa variável para abrir o editor de chaves JSON (Meta/interno e Site).";
  }
  const chans = listLeadOriginChannels();
  if (!chans.length) {
    if (empty) {
      empty.hidden = false;
      empty.textContent =
        "Nenhum canal de lead no catálogo. A origem de campos aplica-se a Meta, Site e interno (P12).";
    }
    return;
  }
  if (empty) empty.hidden = true;

  const sorted = allLeadOriginVariableKeys();
  sorted.forEach((key) => {
    const pill = document.createElement("button");
    pill.type = "button";
    pill.className = "var-origin-pill";
    if (!isVrConfigurableSlot(key)) pill.classList.add("var-origin-pill--readonly");
    const code = document.createElement("code");
    code.textContent = `{{${key}}}`;
    const badges = document.createElement("div");
    badges.className = "var-origin-badges";
    chans.forEach((ch) => {
      if (state.templates.variables?.[ch]?.[key] == null) return;
      const sp = document.createElement("span");
      sp.className = "var-origin-channel-tag";
      sp.textContent = ch === "meta_lead" ? "Meta" : ch === "site_lead" ? "Site" : "Interno";
      badges.appendChild(sp);
    });
    const tip =
      state.templates.variables?.meta_lead?.[key] ||
      state.templates.variables?.site_lead?.[key] ||
      state.templates.variables?.internal_lead?.[key] ||
      "";
    const conf = isVrConfigurableSlot(key);
    pill.title = conf
      ? (tip ? `${tip} — clique para editar origem` : "Clique para editar chaves de origem")
      : (tip ? `${tip} — só consulta (não editável aqui)` : "Só consulta");
    pill.append(code, badges);
    pill.addEventListener("click", () => openVrEditDialog(key, false));
    wrap.appendChild(pill);
  });
}

function renderVariableResolutionPanel() {
  renderVariableOriginPills();
}

function renderCustomVariablesPanel() {
  const channelSelect = document.getElementById("customVarsChannel");
  const channels = listTemplateChannels();
  ensureCustomVarsChannelOptions(channelSelect, channels, channelSelect?.value || "meta_lead");
  const ch = channelSelect?.value || channels[0] || "meta_lead";
  const bucket = customVariablesStorageKey(ch);
  const note = document.getElementById("customVarsChannelNote");
  if (note) {
    const nvars = Object.keys(state.templates.variables?.[ch] || {}).length;
    note.textContent = nvars
      ? "Passo 1: defina a origem dos campos; passo 2: personalize exibição (fonte, mapa, ordem). Variáveis de contexto = placeholders já resolvidos do canal."
      : "Nenhuma variável de contexto listada para este canal — ainda pode usar chaves de payload JSON (fonte «Chaves no JSON»).";
  }
  const list = document.getElementById("customVarsList");
  if (!list) return;
  list.innerHTML = "";
  const items = state.templates.custom_variables?.[bucket] || [];
  items.forEach((item, idx) => {
    list.appendChild(buildCustomVarCardEl(item, idx, ch, bucket));
  });
}

function buildCustomVarCardEl(item, idx, templateChannel, _bucket) {
  const source = (item?.source || "payload") === "context" ? "context" : "payload";
  const article = document.createElement("article");
  article.className = "custom-var-card";
  article.dataset.cvIndex = String(idx);
  article.dataset.cvSource = source;

  const head = document.createElement("div");
  head.className = "custom-var-card-head";
  const headLeft = document.createElement("div");
  headLeft.className = "custom-var-card-head-left";
  const h = document.createElement("div");
  h.className = "custom-var-card-title";
  h.innerHTML = `<span class="field-micro">#${idx + 1}</span> <code class="cv-head-preview"></code> <span class="cv-head-status is-warn">incompleto</span>`;
  headLeft.appendChild(h);
  const actions = document.createElement("div");
  actions.className = "custom-var-card-actions";
  const dup = document.createElement("button");
  dup.type = "button";
  dup.className = "ghost small";
  dup.textContent = "Duplicar";
  dup.addEventListener("click", () => {
    const list = document.getElementById("customVarsList");
    if (!list) return;
    const n = list.querySelectorAll(".custom-var-card").length;
    const snap = {
      key: (article.querySelector(".cv-key")?.value || "").trim() + "_copy",
      source: article.dataset.cvSource === "context" ? "context" : "payload",
      source_keys: parseCsvValue(article.querySelector(".cv-source-keys")?.value || ""),
      mappings: (() => {
        try {
          return JSON.parse(article.querySelector(".cv-mappings")?.value || "{}");
        } catch {
          return {};
        }
      })(),
      default: article.querySelector(".cv-default")?.value ?? "",
      normalize: {
        trim: !!article.querySelector(".cv-trim")?.checked,
        lower: !!article.querySelector(".cv-lower")?.checked,
      },
    };
    list.appendChild(buildCustomVarCardEl(snap, n, templateChannel, _bucket));
  });
  const up = document.createElement("button");
  up.type = "button";
  up.className = "ghost small";
  up.textContent = "↑";
  up.setAttribute("aria-label", "Mover para cima");
  up.addEventListener("click", () => {
    const p = article.previousElementSibling;
    if (p) article.parentNode?.insertBefore(article, p);
  });
  const down = document.createElement("button");
  down.type = "button";
  down.className = "ghost small";
  down.textContent = "↓";
  down.setAttribute("aria-label", "Mover para baixo");
  down.addEventListener("click", () => {
    const n = article.nextElementSibling;
    if (n) n.parentNode?.insertBefore(n, article);
  });
  const rm = document.createElement("button");
  rm.type = "button";
  rm.className = "ghost small";
  rm.textContent = "Remover";
  rm.addEventListener("click", () => article.remove());
  actions.append(dup, up, down, rm);
  head.append(headLeft, actions);

  const flow = document.createElement("p");
  flow.className = "field-micro custom-var-flow";
  flow.textContent =
    "Passo 1: Origem dos campos (secção acima) → contexto base. Passo 2: aqui escolhe fonte (JSON vs contexto) e transformação.";

  const seg = document.createElement("div");
  seg.className = "cv-source-seg";
  const bPayload = document.createElement("button");
  bPayload.type = "button";
  bPayload.className = `cv-seg${source === "payload" ? " is-active" : ""}`;
  bPayload.textContent = "Chaves no JSON";
  const bCtx = document.createElement("button");
  bCtx.type = "button";
  bCtx.className = `cv-seg${source === "context" ? " is-active" : ""}`;
  bCtx.textContent = "Variável já resolvida";
  seg.append(bPayload, bCtx);

  const ctxHint = document.createElement("p");
  ctxHint.className = "field-micro cv-ctx-hint";
  const vars = state.templates.variables?.[templateChannel] || {};
  const varKeys = Object.keys(vars);
  ctxHint.textContent = varKeys.length
    ? `Exemplos: ${varKeys.slice(0, 8).join(", ")}${varKeys.length > 8 ? "…" : ""}. Ordem: primeira com valor vence.`
    : "Sem lista de contexto; use a fonte «Chaves no JSON».";

  const keyLb = document.createElement("label");
  keyLb.innerHTML = "Nome no template <span class='field-micro'>(a-z, 0-9, _; ex. bairro_amigavel)</span>";
  const keyIn = document.createElement("input");
  keyIn.type = "text";
  keyIn.className = "cv-key";
  keyIn.value = String(item?.key || "");
  keyIn.autocomplete = "off";
  keyIn.addEventListener("input", () => updateCvCardHead(article));
  keyLb.appendChild(keyIn);

  const skLb = document.createElement("div");
  skLb.className = "cv-source-block";
  const skLabel = document.createElement("div");
  skLabel.className = "cv-source-label";
  const chipsWrap = document.createElement("div");
  chipsWrap.className = "chips-control cv-chips-wrap";
  const skHidden = document.createElement("input");
  skHidden.type = "hidden";
  skHidden.className = "cv-source-keys";
  skHidden.value = Array.isArray(item?.source_keys) ? item.source_keys.join(", ") : "";
  const listEl = document.createElement("div");
  listEl.className = "chips-list cv-chips-list";
  const skEntry = document.createElement("input");
  skEntry.type = "text";
  skEntry.className = "chips-entry cv-chips-entry";
  skEntry.placeholder = source === "context" ? "ex.: nome e Enter" : "ex.: referencia e Enter";
  chipsWrap.append(skHidden, listEl, skEntry);
  skLabel.innerHTML = source === "context" ? "<span>Chaves de contexto</span>" : "<span>Chaves no JSON</span>";
  skLb.append(skLabel, chipsWrap);
  initCvChipsControl(chipsWrap);
  skHidden.addEventListener("input", () => updateCvCardHead(article));
  skEntry.addEventListener("blur", () => updateCvCardHead(article));

  const setSource = (mode) => {
    article.dataset.cvSource = mode;
    bPayload.className = `cv-seg${mode === "payload" ? " is-active" : ""}`;
    bCtx.className = `cv-seg${mode === "context" ? " is-active" : ""}`;
    skEntry.placeholder = mode === "context" ? "ex.: nome e Enter" : "ex.: referencia e Enter";
    const lab = skLabel.querySelector("span");
    if (lab) lab.textContent = mode === "context" ? "Chaves de contexto" : "Chaves no JSON";
  };
  bPayload.addEventListener("click", () => setSource("payload"));
  bCtx.addEventListener("click", () => setSource("context"));

  const defLb = document.createElement("label");
  defLb.innerHTML = "Texto padrão se o mapa não bater (ou vazio)";
  const defIn = document.createElement("input");
  defIn.type = "text";
  defIn.className = "cv-default";
  defIn.value = String(item?.default ?? "");
  defLb.appendChild(defIn);

  const normWrap = document.createElement("label");
  normWrap.className = "custom-var-card-full";
  const trimCk = document.createElement("input");
  trimCk.type = "checkbox";
  trimCk.className = "cv-trim";
  trimCk.checked = item?.normalize?.trim !== false;
  const lowerCk = document.createElement("input");
  lowerCk.type = "checkbox";
  lowerCk.className = "cv-lower";
  lowerCk.checked = !!item?.normalize?.lower;
  normWrap.appendChild(trimCk);
  normWrap.appendChild(document.createTextNode(" Trim ao comparar "));
  normWrap.appendChild(lowerCk);
  normWrap.appendChild(document.createTextNode(" Lowercase ao comparar "));

  const mapTableWrap = document.createElement("div");
  mapTableWrap.className = "cv-map-table-wrap";
  const mapToolbar = document.createElement("div");
  mapToolbar.className = "cv-map-toolbar";
  const mapAdd = document.createElement("button");
  mapAdd.type = "button";
  mapAdd.className = "ghost small";
  mapAdd.textContent = "Adicionar linha no mapa";
  const mapJsonToggle = document.createElement("button");
  mapJsonToggle.type = "button";
  mapJsonToggle.className = "ghost small";
  mapJsonToggle.textContent = "Ver / editar JSON";
  mapToolbar.append(mapAdd, mapJsonToggle);
  const mapTable = document.createElement("div");
  mapTable.className = "cv-map-table";
  const mapTa = document.createElement("textarea");
  mapTa.className = "cv-mappings";
  mapTa.style.display = "none";
  try {
    mapTa.value = JSON.stringify(item?.mappings && typeof item.mappings === "object" ? item.mappings : {}, null, 2);
  } catch {
    mapTa.value = "{}";
  }
  const syncTableFromJson = () => {
    mapTable.innerHTML = "";
    let obj = {};
    try {
      const p = JSON.parse(mapTa.value || "{}");
      if (p && typeof p === "object" && !Array.isArray(p)) obj = p;
    } catch {
      obj = {};
    }
    Object.entries(obj).forEach(([k, v]) => {
      const row = document.createElement("div");
      row.className = "cv-map-row";
      const kIn = document.createElement("input");
      kIn.type = "text";
      kIn.className = "cv-map-k";
      kIn.value = k;
      const vIn = document.createElement("input");
      vIn.type = "text";
      vIn.className = "cv-map-v";
      vIn.value = String(v);
      const rmB = document.createElement("button");
      rmB.type = "button";
      rmB.className = "ghost small";
      rmB.textContent = "×";
      rmB.addEventListener("click", () => {
        row.remove();
        syncJsonFromTable();
      });
      kIn.addEventListener("input", syncJsonFromTable);
      vIn.addEventListener("input", syncJsonFromTable);
      row.append(kIn, vIn, rmB);
      mapTable.appendChild(row);
    });
  };
  const syncJsonFromTable = () => {
    const rows = mapTable.querySelectorAll(".cv-map-row");
    const o = {};
    rows.forEach((row) => {
      const k = (row.querySelector(".cv-map-k")?.value || "").trim();
      if (!k) return;
      o[k] = row.querySelector(".cv-map-v")?.value ?? "";
    });
    try {
      mapTa.value = JSON.stringify(o, null, 2);
    } catch {
      mapTa.value = "{}";
    }
  };
  mapAdd.addEventListener("click", () => {
    if (mapTa.style.display === "block") {
      return;
    }
    const row = document.createElement("div");
    row.className = "cv-map-row";
    row.innerHTML = `<input type="text" class="cv-map-k" /><input type="text" class="cv-map-v" /><button type="button" class="ghost small" aria-label="remover">×</button>`;
    row.querySelector("button")?.addEventListener("click", () => {
      row.remove();
      syncJsonFromTable();
    });
    row.querySelector(".cv-map-k")?.addEventListener("input", syncJsonFromTable);
    row.querySelector(".cv-map-v")?.addEventListener("input", syncJsonFromTable);
    mapTable.appendChild(row);
  });
  let mapJsonMode = false;
  mapJsonToggle.addEventListener("click", () => {
    if (!mapJsonMode) {
      syncJsonFromTable();
      mapTa.style.display = "block";
      mapTable.style.display = "none";
      mapJsonToggle.textContent = "Voltar ao mapa (tabela)";
      mapJsonMode = true;
    } else {
      try {
        syncTableFromJson();
      } catch {
        /* se JSON inválido, mantém tabela vazia */
        mapTable.innerHTML = "";
      }
      mapTa.style.display = "none";
      mapTable.style.display = "";
      mapJsonToggle.textContent = "Ver / editar JSON";
      mapJsonMode = false;
    }
  });
  syncTableFromJson();

  const previewRow = document.createElement("div");
  previewRow.className = "cv-preview-row";
  const rawTest = document.createElement("input");
  rawTest.type = "text";
  rawTest.className = "cv-raw-test";
  rawTest.placeholder = "Valor bruto de teste (simula o que veio do JSON / contexto)";
  const prevBtn = document.createElement("button");
  prevBtn.type = "button";
  prevBtn.className = "ghost small";
  prevBtn.textContent = "Pré-visualizar transformação";
  const prevOut = document.createElement("code");
  prevOut.className = "cv-preview-out";
  prevOut.textContent = "—";
  prevBtn.addEventListener("click", async () => {
    if (mapTable.style.display !== "none" && mapTa.style.display === "none") {
      syncJsonFromTable();
    }
    const rawJ = mapTa.value || "{}";
    let mappings = {};
    try {
      const p = JSON.parse(rawJ);
      if (p && typeof p === "object" && !Array.isArray(p)) mappings = p;
    } catch {
      prevOut.textContent = "JSON de mapa inválido";
      return;
    }
    const r = await dashFetch(apiUrl("/api/message-templates/custom-variable-preview"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        raw: String(rawTest.value || ""),
        mappings,
        default: defIn.value ?? "",
        normalize: { trim: trimCk.checked, lower: lowerCk.checked },
      }),
    });
    let b = {};
    try {
      b = await r.json();
    } catch {
      prevOut.textContent = `HTTP ${r.status} (rota /dash/api/message-templates/custom-variable-preview em falta no servidor?)`;
      return;
    }
    prevOut.textContent = b.ok && b.result != null ? b.result : b.error || "falha";
  });
  previewRow.append(document.createTextNode("Teste: "), rawTest, prevBtn, document.createTextNode(" → "), prevOut);

  const mapHead = document.createElement("p");
  mapHead.className = "field-micro";
  mapHead.textContent = "Mapa: valor bruto → texto exibido";
  mapTableWrap.append(mapHead, mapToolbar, mapTable, mapTa);

  const grid = document.createElement("div");
  grid.className = "custom-var-card-grid";
  grid.append(flow, seg, keyLb, ctxHint, skLb, defLb, normWrap, mapTableWrap, previewRow);

  article.append(head, grid);
  if (source === "context") setSource("context");
  updateCvCardHead(article);
  return article;
}

function appendEmptyCustomVarCard() {
  const list = document.getElementById("customVarsList");
  if (!list) return;
  const n = list.querySelectorAll(".custom-var-card").length;
  const ch = document.getElementById("customVarsChannel")?.value || "meta_lead";
  const bucket = customVariablesStorageKey(ch);
  list.appendChild(
    buildCustomVarCardEl(
      {
        key: "",
        source: "payload",
        source_keys: [],
        mappings: {},
        default: "",
        normalize: { trim: true, lower: false },
      },
      n,
      ch,
      bucket,
    ),
  );
}

function collectVariableResolutionPayload(storageKey) {
  const label = storageKey === "meta_lead" ? "Meta/interno" : "Site";
  const draft = storageKey === "meta_lead" ? state.vrDraftMeta : state.vrDraftSite;
  const payload = {};

  for (const [name, csv] of Object.entries(draft || {})) {
    const raw = String(csv || "").trim();
    if (!raw) continue;
    const keys = raw
      .split(/[,;]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    if (!keys.length) continue;
    if (!_VR_NAME_RE.test(name)) {
      return { error: `${label}: nome inválido «${name}».` };
    }
    if (LEAD_RESOLVABLE_SLOTS.includes(name)) {
      payload[name] = { source_keys: keys };
      continue;
    }
    if (VR_EXT_FORBIDDEN.has(name)) {
      return { error: `${label}: «${name}» é reservado ao sistema.` };
    }
    payload[name] = { source_keys: keys };
  }
  return { payload };
}

async function saveVariableResolution() {
  const fb = document.getElementById("varResFeedback");
  if (!fb) return;
  fb.textContent = "Salvando...";
  const m = collectVariableResolutionPayload("meta_lead");
  if (m.error) {
    fb.textContent = m.error;
    return;
  }
  const s = collectVariableResolutionPayload("site_lead");
  if (s.error) {
    fb.textContent = s.error;
    return;
  }

  const targets = [
    ["meta_lead", m.payload],
    ["site_lead", s.payload],
  ];
  const errors = [];
  for (const [ch, payload] of targets) {
    const r = await dashFetch(apiUrl(`/api/message-templates/variable-resolution/${encodeURIComponent(ch)}`), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    let body = {};
    try {
      body = await r.json();
    } catch {
      errors.push(`${ch}: resposta inválida (HTTP ${r.status})`);
      continue;
    }
    if (!r.ok || !body.ok) {
      errors.push(`${ch}: ${body.error || "falha ao salvar"}`);
    }
  }
  if (errors.length) {
    fb.textContent = `Erro: ${errors.join(" · ")}`;
    return;
  }
  fb.textContent = "Chaves de origem salvas (Meta/interno e Site).";
  await fetchTemplates();
}

async function saveCustomVariables() {
  const fb = document.getElementById("customVarsFeedback");
  const ch = document.getElementById("customVarsChannel")?.value || "meta_lead";
  if (!fb) return;
  fb.textContent = "Salvando...";
  const list = document.getElementById("customVarsList");
  const items = [];
  const cards = list?.querySelectorAll(".custom-var-card") || [];
  for (const card of cards) {
    const key = card.querySelector(".cv-key")?.value?.trim() || "";
    const sk = card.querySelector(".cv-source-keys")?.value || "";
    const source_keys = sk
      .split(/[,;]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    const source = card.dataset.cvSource === "context" ? "context" : "payload";
    const mapTa = card.querySelector(".cv-mappings");
    const mapTable = card.querySelector(".cv-map-table");
    let mappings = {};
    if (mapTa && mapTa.style.display === "block") {
      const rawJ = (mapTa.value || "").trim() || "{}";
      try {
        const parsed = JSON.parse(rawJ);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) mappings = parsed;
        else {
          fb.textContent = "JSON do mapa inválido numa das variáveis.";
          return;
        }
      } catch {
        fb.textContent = "JSON do mapa inválido numa das variáveis.";
        return;
      }
    } else if (mapTable) {
      mapTable.querySelectorAll(".cv-map-row").forEach((row) => {
        const mk = (row.querySelector(".cv-map-k")?.value || "").trim();
        if (!mk) return;
        mappings[mk] = String(row.querySelector(".cv-map-v")?.value ?? "");
      });
    }
    const def = card.querySelector(".cv-default")?.value ?? "";
    const trim = !!card.querySelector(".cv-trim")?.checked;
    const lower = !!card.querySelector(".cv-lower")?.checked;
    if (!key || !source_keys.length) continue;
    items.push({
      key,
      source,
      source_keys,
      mappings,
      default: String(def),
      normalize: { trim, lower },
    });
  }
  const r = await dashFetch(apiUrl(`/api/message-templates/custom-variables/${encodeURIComponent(ch)}`), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items }),
  });
  let body = {};
  try {
    body = await r.json();
  } catch {
    fb.textContent = `Erro: resposta inválida (HTTP ${r.status}). A rota /dash/api/… existe no servidor?`;
    return;
  }
  if (!r.ok || !body.ok) {
    fb.textContent = `Erro: ${body.error || "falha ao salvar"}`;
    return;
  }
  fb.textContent = "Variáveis personalizadas salvas.";
  await fetchTemplates();
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

function normalizeFilterRules(rules) {
  const src = rules && typeof rules === "object" ? rules : {};
  return {
    exclude_exact: Array.isArray(src.exclude_exact) ? src.exclude_exact : [],
    exclude_contains: Array.isArray(src.exclude_contains) ? src.exclude_contains : [],
    exclude_regex: Array.isArray(src.exclude_regex) ? src.exclude_regex : [],
  };
}

function syncFiltersAddChannelOptions() {
  const select = document.getElementById("filtersAddChannel");
  if (!select) return;
  const channels = listTemplateChannels();
  const used = new Set(state.activeFilterChannels || []);
  const available = channels.filter((channel) => !used.has(channel));
  select.innerHTML = "";
  if (!available.length) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "Todos os canais já adicionados";
    select.appendChild(opt);
    select.disabled = true;
    return;
  }
  available.forEach((channel) => {
    const opt = document.createElement("option");
    opt.value = channel;
    opt.textContent = channel;
    select.appendChild(opt);
  });
  select.disabled = false;
}

function renderGlobalFiltersSummary() {
  const root = document.getElementById("globalFiltersSummary");
  if (!root) return;
  root.innerHTML = "";
  const filters = state.templates.filters || {};
  const channels = state.activeFilterChannels || [];
  if (!channels.length) {
    const empty = document.createElement("span");
    empty.className = "field-micro";
    empty.textContent = "Nenhum canal com filtros globais adicionado.";
    root.appendChild(empty);
    return;
  }
  channels.forEach((channel) => {
    const rules = normalizeFilterRules(filters[channel]);
    const total = rules.exclude_exact.length + rules.exclude_contains.length + rules.exclude_regex.length;
    const pill = document.createElement("span");
    pill.className = "global-filters-summary-pill";
    pill.textContent = `${channel} · ${total} regra(s)`;
    root.appendChild(pill);
  });
}

function removeFilterChannel(channel) {
  state.activeFilterChannels = (state.activeFilterChannels || []).filter((item) => item !== channel);
  renderFiltersForm();
}

function addFilterChannel() {
  const select = document.getElementById("filtersAddChannel");
  if (!select || !select.value) return;
  const next = select.value;
  const current = new Set(state.activeFilterChannels || []);
  current.add(next);
  state.activeFilterChannels = Array.from(current).sort((a, b) => a.localeCompare(b, "pt-BR"));
  if (!state.templates.filters[next]) {
    state.templates.filters[next] = { exclude_exact: [], exclude_contains: [], exclude_regex: [] };
  }
  renderFiltersForm();
}

function renderGlobalFilterCard(channel, rules) {
  const tpl = document.getElementById("globalFilterCardTemplate");
  if (!(tpl instanceof HTMLTemplateElement)) return null;
  const frag = tpl.content.cloneNode(true);
  const form = frag.querySelector(".global-filter-card");
  if (!form) return null;
  form.dataset.filterChannel = channel;
  const title = frag.querySelector(".global-filter-channel-title");
  if (title) title.textContent = channel;
  form.elements.exclude_exact.value = (rules.exclude_exact || []).join(", ");
  form.elements.exclude_contains.value = (rules.exclude_contains || []).join(", ");
  form.elements.exclude_regex.value = (rules.exclude_regex || []).join(", ");
  setupChipFields(form, ["exclude_exact", "exclude_contains", "exclude_regex"]);
  const removeBtn = frag.querySelector(".global-filter-remove-btn");
  removeBtn?.addEventListener("click", () => removeFilterChannel(channel));
  form.addEventListener("submit", (ev) => saveFilters(ev, channel));
  return frag;
}

function renderFiltersForm() {
  const root = document.getElementById("globalFiltersList");
  if (!root) return;
  const allChannels = listTemplateChannels();
  const filters = state.templates.filters || {};
  const channelsWithFilters = Object.keys(filters || {});
  const baseline = channelsWithFilters.length ? channelsWithFilters : allChannels.slice(0, 2);
  const currentSet = new Set(state.activeFilterChannels || baseline);
  state.activeFilterChannels = Array.from(currentSet)
    .filter((ch) => allChannels.includes(ch))
    .sort((a, b) => a.localeCompare(b, "pt-BR"));
  root.innerHTML = "";
  (state.activeFilterChannels || []).forEach((channel) => {
    const rules = normalizeFilterRules(filters[channel]);
    const card = renderGlobalFilterCard(channel, rules);
    if (card) root.appendChild(card);
  });
  syncFiltersAddChannelOptions();
  renderGlobalFiltersSummary();
}

async function fetchMetaClients() {
  const r = await dashFetch(apiUrl("/api/clients"));
  if (!r.ok) throw new Error(await dashFetchErrorMessage(r, "/api/clients"));
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
  if (!r.ok) throw new Error(await dashFetchErrorMessage(r, "/api/google-clients"));
  const data = await r.json();
  state.googleClients = data.clients || [];
  buildStats("googleStatsRow", state.googleClients);
  renderGoogleClients();
  syncCatalogGroupSelects();
}

function siteRouteChecks(route) {
  const codi = String(route?.codi_id || route?.form_id || "").trim();
  const groupId = String(route?.group_id || "").trim();
  const phone = String(route?.lead_phone_number || "").trim();
  const internalGroupId = String(route?.internal_notify_group_id || "").trim();
  const leadTpl = String(route?.lead_template || "default").trim() || "default";
  const intTpl = String(route?.internal_lead_template || "").trim();
  const channelSite = state.templates?.channels?.site_lead || {};
  const channelInternal = state.templates?.channels?.internal_lead || {};
  const hasLabel = Boolean(
    String(route?.cliente_origem || "").trim() || String(route?.origem_anuncio || "").trim() || String(route?.target_client_name || "").trim(),
  );
  return {
    codiOk: /^\d{28,36}$/.test(codi),
    labelOk: hasLabel,
    groupOk: !groupId || /^\d+(-\d+)?@g\.us$/.test(groupId),
    phoneOk: true,
    internalGroupOk: !internalGroupId || /^\d+(-\d+)?@g\.us$/.test(internalGroupId),
    leadTemplateOk: !!channelSite[leadTpl],
    internalTemplateOk: !intTpl || !!channelInternal[intTpl],
  };
}

function renderSiteLeadRoutes() {
  const wrap = document.getElementById("siteLeadRoutesWrap");
  if (!wrap) return;
  const rows = Array.isArray(state.siteLeadRoutes) ? state.siteLeadRoutes : [];
  if (!rows.length) {
    wrap.innerHTML = `<div class="catalog-empty-state" role="status">
      <div class="catalog-empty-orb" aria-hidden="true">◎</div>
      <h3 class="catalog-empty-title">Sem clientes por codi_id</h3>
      <p class="catalog-empty-text">Cadastre cliente e templates para evitar envio errado quando o lead vier do site.</p>
    </div>`;
    return;
  }
  wrap.innerHTML = rows
    .map((r) => {
      const id = Number(r.id || 0);
      const formId = escHtml(r.codi_id || r.form_id || "");
      const clienteOrigem = escHtml(r.cliente_origem || r.target_client_name || "");
      const origemAnuncio = escHtml(r.origem_anuncio || "");
      const displayName = escHtml(
        (r.cliente_origem || r.target_client_name || r.origem_anuncio || r.codi_id || "").toString().trim() || "(sem rótulo)",
      );
      const groupId = escHtml(r.group_id || "");
      const leadPhone = escHtml(r.lead_phone_number || "");
      const internalNotifyGroup = escHtml(r.internal_notify_group_id || "");
      const leadTemplate = escHtml(r.lead_template || "default");
      const internalLeadTemplate = escHtml(r.internal_lead_template || "");
      const notes = escHtml(r.notes || "");
      const targetType = escHtml((r.target_type || "").toString().trim() || "site");
      const sourceType = escHtml((r.source_type || "").toString().trim());
      const exFields = escHtml(((r.lead_exclude_fields || []).map((x) => String(x).trim()).filter(Boolean)).join(", "));
      const exContains = escHtml(((r.lead_exclude_contains || []).map((x) => String(x).trim()).filter(Boolean)).join(", "));
      const exRegex = escHtml(((r.lead_exclude_regex || []).map((x) => String(x).trim()).filter(Boolean)).join(", "));
      const corsOrigins = escHtml(
        ((r.cors_allowed_origins || []).map((x) => String(x).trim()).filter(Boolean)).join("\n"),
      );
      const enabled = !!r.enabled;
      const checks = siteRouteChecks(r);
      const statusLabel =
        !enabled
          ? "Pausado"
          : checks.codiOk &&
              checks.groupOk &&
              checks.phoneOk &&
              checks.internalGroupOk &&
              checks.leadTemplateOk
            ? "Ativo completo"
            : "Inconsistente";
      const statusClass = statusPillClass(statusLabel);
      return `<article class="client-card site-client-card" data-route-id="${id}">
        <div class="client-main">
          <div class="client-head">
            <h3 class="client-name">${displayName}</h3>
            <div class="head-actions">
              <span class="status-pill ${statusClass}">${statusLabel}</span>
              <button
                type="button"
                class="gear-btn"
                data-action="toggle-edit-site"
                title="Editar cliente do site"
                aria-label="Editar cliente do site"
              >
                ⚙
              </button>
            </div>
          </div>
          <dl class="meta-grid">
            <div><dt data-field-key="codi_id">${fieldCopyDtLabel("site", "codi_id")}</dt><dd><code>${formId}</code></dd></div>
            <div><dt data-field-key="cliente_origem">${fieldCopyDtLabel("site", "cliente_origem")}</dt><dd>${clienteOrigem || "—"}</dd></div>
            <div><dt data-field-key="origem_anuncio">${fieldCopyDtLabel("site", "origem_anuncio")}</dt><dd>${origemAnuncio || "—"}</dd></div>
            <div><dt data-field-key="group_id">${fieldCopyDtLabel("site", "group_id")}</dt><dd>${groupId || "—"}</dd></div>
            <div><dt data-field-key="lead_phone_number">${fieldCopyDtLabel("site", "lead_phone_number")}</dt><dd>${leadPhone || "—"}</dd></div>
            <div><dt data-field-key="internal_notify_group_id">${fieldCopyDtLabel("site", "internal_notify_group_id")}</dt><dd>${internalNotifyGroup || "—"}</dd></div>
            <div><dt data-field-key="lead_template">${fieldCopyDtLabel("site", "lead_template")}</dt><dd>${leadTemplate}</dd></div>
            <div><dt data-field-key="internal_lead_template">${fieldCopyDtLabel("site", "internal_lead_template")}</dt><dd>${internalLeadTemplate || "Nenhum"}</dd></div>
            <div><dt>Tipo de rota</dt><dd>${targetType}</dd></div>
            <div><dt>Tipo de origem</dt><dd>${sourceType || "—"}</dd></div>
            <div><dt data-field-key="lead_exclude_fields">${fieldCopyDtLabel("site", "lead_exclude_fields")}</dt><dd>${exFields || "—"}</dd></div>
            <div><dt data-field-key="lead_exclude_contains">${fieldCopyDtLabel("site", "lead_exclude_contains")}</dt><dd>${exContains || "—"}</dd></div>
            <div><dt data-field-key="lead_exclude_regex">${fieldCopyDtLabel("site", "lead_exclude_regex")}</dt><dd>${exRegex || "—"}</dd></div>
            <div><dt data-field-key="cors_allowed_origins">${fieldCopyDtLabel("site", "cors_allowed_origins")}</dt><dd>${corsOrigins ? `<pre class="cors-origins-preview">${corsOrigins}</pre>` : "—"}</dd></div>
            <div><dt data-field-key="notes">${fieldCopyDtLabel("site", "notes")}</dt><dd>${notes || "—"}</dd></div>
            <div><dt data-field-key="enabled">${fieldCopyDtLabel("site", "enabled")}</dt><dd>${enabled ? "true" : "false"}</dd></div>
          </dl>
          <div class="checks">
            <span class="check-pill ${checks.codiOk ? "ok" : "error"}">${checks.codiOk ? "OK" : "ERRO"} · codi_id</span>
            <span class="check-pill ${checks.labelOk ? "ok" : "error"}">${checks.labelOk ? "OK" : "aviso"} · rótulo interno</span>
            <span class="check-pill ${checks.groupOk ? "ok" : "error"}">${checks.groupOk ? "OK" : "ERRO"} · group_id</span>
            <span class="check-pill ok">OK · telefone_cliente (opcional)</span>
            <span class="check-pill ${checks.internalGroupOk ? "ok" : "error"}">${checks.internalGroupOk ? "OK" : "ERRO"} · grupo_interno</span>
            <span class="check-pill ${checks.leadTemplateOk ? "ok" : "error"}">${checks.leadTemplateOk ? "OK" : "ERRO"} · template site</span>
            <span class="check-pill ${checks.internalTemplateOk ? "ok" : "error"}">${checks.internalTemplateOk ? "OK" : "ERRO"} · template interno</span>
          </div>
          <form class="edit-form edit-sheet hidden" data-field-context="site">
            <header class="edit-sheet-head">
              <h4 class="edit-sheet-title">Editar cliente do site</h4>
              <p class="edit-sheet-sub">Ajuste codi_id, rótulos internos, grupos e templates. O envio roteia só por codi_id.</p>
            </header>
            <div class="edit-field-grid">
              <label class="edit-field">
                CODI ID
                <input
                  name="codi_id"
                  required
                  inputmode="numeric"
                  pattern="[0-9]{28,36}"
                  minlength="28"
                  maxlength="36"
                />
              </label>
              <label class="edit-field edit-field--full">
                Rótulo interno — identificação / campanha
                <input name="cliente_origem" type="text" placeholder="Identificação no painel e no template" />
              </label>
              <label class="edit-field edit-field--full">
                Rótulo interno — origem do anúncio
                <input name="origem_anuncio" type="text" placeholder="Ex.: PMax / Modal / Remarketing" />
              </label>
              <label class="edit-field">
                Grupo cliente
                <select name="group_id" class="field-select catalog-group-select" required data-catalog-optional="0">
                  <option value="">— Escolher do catálogo —</option>
                </select>
              </label>
              <label class="edit-field">
                Telefone cliente
                <input name="lead_phone_number" inputmode="tel" placeholder="Ex.: 5511999999999" />
              </label>
              <label class="edit-field">
                Grupo mensagem interna
                <select name="internal_notify_group_id" class="field-select catalog-group-select" required data-catalog-optional="0">
                  <option value="">— Escolher do catálogo —</option>
                </select>
              </label>
              <label class="edit-field">
                Template de mensagem
                <select name="lead_template" class="field-select lead-template-select" required></select>
              </label>
              <label class="edit-field">
                Template de mensagem interno
                <select name="internal_lead_template" class="field-select internal-lead-template-select">
                  <option value="">Nenhum</option>
                </select>
              </label>
              <label class="edit-field edit-field--full">
                Observações
                <input name="notes" placeholder="Opcional" />
              </label>
              <label class="edit-field edit-field--full">
                Origens CORS permitidas
                <span class="field-micro">Uma por linha ou vírgula · ex.: https://www.cliente.com.br</span>
                <textarea name="cors_allowed_origins" rows="3" placeholder="https://www.exemplo.com.br"></textarea>
              </label>
            </div>
            <div class="new-client-filters">
              <div class="filters-block-head">
                <div class="filters-block-head-text">
                  <h3 class="new-client-filters-title">Filtros do bloco "Formulário" (Leads Site)</h3>
                  <p class="new-client-filters-desc">
                    Remove linhas de <code>{{respostas}}</code> pelo nome do campo.
                  </p>
                </div>
              </div>
              <div class="new-client-filters-grid">
                <label>
                  Excluir perguntas (nome exato)
                  <span class="field-micro">Igual ao nome/chave da pergunta (ignora maiúsculas).</span>
                  <div class="chips-control" data-chip-for="lead_exclude_fields">
                    <div class="chips-list"></div>
                    <input type="text" class="chips-entry" placeholder="Palavra-chave e Enter" />
                  </div>
                  <input type="hidden" name="lead_exclude_fields" />
                </label>
                <label>
                  Excluir se o nome contiver
                  <span class="field-micro">Se o nome da pergunta contiver este trecho.</span>
                  <div class="chips-control" data-chip-for="lead_exclude_contains">
                    <div class="chips-list"></div>
                    <input type="text" class="chips-entry" placeholder="Palavra-chave e Enter" />
                  </div>
                  <input type="hidden" name="lead_exclude_contains" />
                </label>
                <label>
                  Excluir por regex
                  <span class="field-micro">Regex sobre o nome; inválida é ignorada no envio.</span>
                  <div class="chips-control" data-chip-for="lead_exclude_regex">
                    <div class="chips-list"></div>
                    <input type="text" class="chips-entry" placeholder="Regex e Enter" />
                  </div>
                  <input type="hidden" name="lead_exclude_regex" />
                </label>
              </div>
            </div>
            <div class="edit-bar">
              <label class="check edit-check">
                <input type="checkbox" name="enabled" />
                <span>Cadastro ativo</span>
              </label>
              <div class="edit-actions">
                <button type="submit" class="btn-edit-save">Salvar alterações</button>
                <button type="button" class="btn-edit-cancel" data-action="cancel-edit-site">Cancelar</button>
                <button type="button" class="small action-err" data-action="delete-site">Remover</button>
              </div>
            </div>
            <p class="edit-feedback"></p>
          </form>
        </div>
      </article>`;
    })
    .join("");

  wrap.querySelectorAll(".site-client-card").forEach((card) => {
    const routeId = Number(card.dataset.routeId || 0);
    const route = state.siteLeadRoutes.find((x) => Number(x.id) === routeId);
    if (!route) return;
    const editForm = card.querySelector(".edit-form");
    const feedback = card.querySelector(".edit-feedback");
    if (!editForm) return;

    editForm.elements.codi_id.value = route.codi_id || route.form_id || "";
    if (editForm.elements.cliente_origem) {
      editForm.elements.cliente_origem.value = route.cliente_origem || route.target_client_name || "";
    }
    if (editForm.elements.origem_anuncio) {
      editForm.elements.origem_anuncio.value = route.origem_anuncio || "";
    }
    if (editForm.elements.group_id) {
      editForm.elements.group_id.dataset.currentValue = route.group_id || "";
      editForm.elements.group_id.value = route.group_id || "";
    }
    if (editForm.elements.lead_phone_number) editForm.elements.lead_phone_number.value = route.lead_phone_number || "";
    if (editForm.elements.internal_notify_group_id) {
      editForm.elements.internal_notify_group_id.dataset.currentValue = route.internal_notify_group_id || "";
      editForm.elements.internal_notify_group_id.value = route.internal_notify_group_id || "";
    }
    if (editForm.elements.lead_template) {
      populateSiteLeadTemplateSelect(editForm.elements.lead_template, route.lead_template || "default");
    }
    if (editForm.elements.internal_lead_template) {
      populateChannelTemplateSelect(
        editForm.elements.internal_lead_template,
        "internal_lead",
        INTERNAL_LEAD_BUILTIN_IDS,
        route.internal_lead_template || "",
        true
      );
    }
    if (editForm.elements.lead_exclude_fields) {
      editForm.elements.lead_exclude_fields.value = (route.lead_exclude_fields || []).join(", ");
    }
    if (editForm.elements.lead_exclude_contains) {
      editForm.elements.lead_exclude_contains.value = (route.lead_exclude_contains || []).join(", ");
    }
    if (editForm.elements.lead_exclude_regex) {
      editForm.elements.lead_exclude_regex.value = (route.lead_exclude_regex || []).join(", ");
    }
    setupChipFields(editForm, ["lead_exclude_fields", "lead_exclude_contains", "lead_exclude_regex"]);
    editForm.elements.notes.value = route.notes || "";
    if (editForm.elements.cors_allowed_origins) {
      editForm.elements.cors_allowed_origins.value = (route.cors_allowed_origins || []).join("\n");
    }
    editForm.elements.enabled.checked = !!route.enabled;

    card.querySelector('[data-action="toggle-edit-site"]')?.addEventListener("click", () => {
      editForm.classList.toggle("hidden");
      if (feedback) feedback.textContent = "";
    });
    card.querySelector('[data-action="cancel-edit-site"]')?.addEventListener("click", () => {
      editForm.classList.add("hidden");
      if (feedback) feedback.textContent = "";
    });
    card.querySelector('[data-action="delete-site"]')?.addEventListener("click", async () => {
      const ok = window.confirm("Remover este cadastro por codi_id?");
      if (!ok) return;
      const res = await dashFetch(apiUrl(`/api/site-lead-routes/${routeId}`), { method: "DELETE" });
      const body = await res.json().catch(() => ({}));
      const fb = document.getElementById("siteLeadRouteFeedback");
      if (!res.ok || !body.ok) {
        if (fb) fb.textContent = `Erro: ${body.error || "nao foi possível remover"}`;
        return;
      }
      if (fb) fb.textContent = "Cadastro removido com sucesso.";
      await fetchSiteLeadRoutes();
    });

    applyFieldCopy(card, "site");

    editForm.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const badRx = invalidRegexPatterns(editForm.querySelector('input[name="lead_exclude_regex"]'));
      if (badRx.length) {
        if (feedback) feedback.textContent = `Corrija a(s) regex inválida(s): ${badRx.join(", ")}`;
        return;
      }
      if (feedback) feedback.textContent = "Salvando alterações...";
      const fd = new FormData(editForm);
      const payload = Object.fromEntries(fd.entries());
      payload.enabled = !!fd.get("enabled");
      const ok = await saveSiteLeadRoute(payload, routeId);
      if (!ok) {
        if (feedback) feedback.textContent = "Erro ao salvar alterações.";
        return;
      }
      editForm.classList.add("hidden");
    });
  });
  syncCatalogGroupSelects();
}

function fillSiteLeadRouteForm(route) {
  const form = document.getElementById("siteLeadRouteForm");
  if (!form || !route) return;
  form.dataset.editId = String(route.id || "");
  form.elements.codi_id.value = route.codi_id || route.form_id || "";
  if (form.elements.cliente_origem) {
    form.elements.cliente_origem.value = route.cliente_origem || route.target_client_name || "";
  }
  if (form.elements.origem_anuncio) {
    form.elements.origem_anuncio.value = route.origem_anuncio || "";
  }
  if (form.elements.group_id) {
    form.elements.group_id.dataset.currentValue = route.group_id || "";
    form.elements.group_id.value = route.group_id || "";
  }
  if (form.elements.lead_phone_number) form.elements.lead_phone_number.value = route.lead_phone_number || "";
  if (form.elements.internal_notify_group_id) {
    form.elements.internal_notify_group_id.dataset.currentValue = route.internal_notify_group_id || "";
    form.elements.internal_notify_group_id.value = route.internal_notify_group_id || "";
  }
  if (form.elements.lead_template) form.elements.lead_template.value = route.lead_template || "default";
  if (form.elements.internal_lead_template) {
    form.elements.internal_lead_template.value = route.internal_lead_template || "";
  }
  if (form.elements.lead_exclude_fields) form.elements.lead_exclude_fields.value = (route.lead_exclude_fields || []).join(", ");
  if (form.elements.lead_exclude_contains)
    form.elements.lead_exclude_contains.value = (route.lead_exclude_contains || []).join(", ");
  if (form.elements.lead_exclude_regex) form.elements.lead_exclude_regex.value = (route.lead_exclude_regex || []).join(", ");
  setupChipFields(form, ["lead_exclude_fields", "lead_exclude_contains", "lead_exclude_regex"]);
  form.elements.notes.value = route.notes || "";
  if (form.elements.cors_allowed_origins) {
    form.elements.cors_allowed_origins.value = (route.cors_allowed_origins || []).join("\n");
  }
  form.elements.enabled.checked = !!route.enabled;
  const submitBtn = form.querySelector('button[type="submit"]');
  if (submitBtn) submitBtn.textContent = "Atualizar cliente do site";
}

function resetSiteLeadRouteForm() {
  const form = document.getElementById("siteLeadRouteForm");
  if (!form) return;
  form.reset();
  form.dataset.editId = "";
  form.elements.enabled.checked = true;
  if (form.elements.cliente_origem) form.elements.cliente_origem.value = "";
  if (form.elements.origem_anuncio) form.elements.origem_anuncio.value = "";
  if (form.elements.group_id) {
    form.elements.group_id.dataset.currentValue = "";
    form.elements.group_id.value = "";
  }
  if (form.elements.lead_phone_number) form.elements.lead_phone_number.value = "";
  if (form.elements.internal_notify_group_id) {
    form.elements.internal_notify_group_id.dataset.currentValue = "";
    form.elements.internal_notify_group_id.value = "";
  }
  if (form.elements.lead_template) form.elements.lead_template.value = "default";
  if (form.elements.internal_lead_template) form.elements.internal_lead_template.value = "";
  if (form.elements.lead_exclude_fields) form.elements.lead_exclude_fields.value = "";
  if (form.elements.lead_exclude_contains) form.elements.lead_exclude_contains.value = "";
  if (form.elements.lead_exclude_regex) form.elements.lead_exclude_regex.value = "";
  if (form.elements.cors_allowed_origins) form.elements.cors_allowed_origins.value = "";
  setupChipFields(form, ["lead_exclude_fields", "lead_exclude_contains", "lead_exclude_regex"]);
  const submitBtn = form.querySelector('button[type="submit"]');
  if (submitBtn) submitBtn.textContent = "Salvar cliente do site";
}

async function fetchSiteLeadRoutes() {
  const res = await dashFetch(apiUrl("/api/site-lead-routes"));
  if (!res.ok) throw new Error(await dashFetchErrorMessage(res, "/api/site-lead-routes"));
  const body = await res.json().catch(() => ({}));
  state.siteLeadRoutes = Array.isArray(body.routes) ? body.routes : [];
  renderSiteLeadRoutes();
}

async function saveSiteLeadRoute(payload, routeId = null) {
  const fb = document.getElementById("siteLeadRouteFeedback");
  if (fb) fb.textContent = routeId ? "Atualizando cliente do site..." : "Salvando cliente do site...";
  const url = routeId ? apiUrl(`/api/site-lead-routes/${routeId}`) : apiUrl("/api/site-lead-routes");
  const method = routeId ? "PUT" : "POST";
  const res = await dashFetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok || !body.ok) {
    if (fb) fb.textContent = `Erro: ${body.error || "nao foi possível salvar cadastro"}`;
    return false;
  }
  if (fb) fb.textContent = routeId ? "Cadastro atualizado com sucesso." : "Cadastro criado com sucesso.";
  await fetchSiteLeadRoutes();
  resetSiteLeadRouteForm();
  return true;
}

async function submitSiteLeadRoute(ev) {
  ev.preventDefault();
  const form = ev.currentTarget;
  const badRx = invalidRegexPatterns(form.querySelector('input[name="lead_exclude_regex"]'));
  if (badRx.length) {
    const fb = document.getElementById("siteLeadRouteFeedback");
    if (fb) fb.textContent = `Corrija a(s) regex inválida(s): ${badRx.join(", ")}`;
    return;
  }
  const fd = new FormData(form);
  const payload = Object.fromEntries(fd.entries());
  payload.enabled = !!fd.get("enabled");
  const routeId = Number(form.dataset.editId || 0) || null;
  await saveSiteLeadRoute(payload, routeId);
}

async function fetchTemplates() {
  const r = await dashFetch(apiUrl("/api/message-templates"));
  if (!r.ok) throw new Error(await dashFetchErrorMessage(r, "/api/message-templates"));
  const data = await r.json();
  state.templates = {
    ...data,
    lead_source_key_defaults: data.lead_source_key_defaults || {},
  };
  hydrateVrDraftFromTemplates();
  renderTemplateVariables(document.getElementById("tplChannel").value);
  renderTemplatesCatalog();
  renderFiltersForm();
  renderVariableResolutionPanel();
  renderCustomVariablesPanel();
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
  populateLeadTemplateSelect(document.getElementById("newClientLeadTemplate"), "");
  setupChipFields(form, ["lead_exclude_fields", "lead_exclude_contains", "lead_exclude_regex"]);
  syncCatalogGroupSelects();
  syncMetaCatalogSelects();
  refreshInternalTemplateSelects();
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
  refreshInternalTemplateSelects();
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
  const unk = body.unknown_placeholders;
  if (Array.isArray(unk) && unk.length) {
    feedback.textContent = `Template salvo. Aviso: placeholders não reconhecidos: ${unk.join(", ")}`;
  } else {
    feedback.textContent = "Template salvo com sucesso.";
  }
  await fetchTemplates();
}

async function saveFilters(ev, forcedChannel = "") {
  ev.preventDefault();
  const form = ev.currentTarget;
  const channel = String(forcedChannel || form?.dataset?.filterChannel || "").trim();
  const feedback = form?.querySelector(".global-filter-feedback") || document.getElementById("filtersFeedback");
  if (!channel) {
    if (feedback) feedback.textContent = "Canal inválido para salvar filtros.";
    return;
  }
  const badRx = invalidRegexPatterns(form.querySelector('input[name="exclude_regex"]'));
  if (badRx.length) {
    feedback.textContent = `Corrija a(s) regex inválida(s): ${badRx.join(", ")}`;
    return;
  }
  feedback.textContent = "Salvando filtros...";
  const payload = Object.fromEntries(new FormData(form).entries());
  const r = await dashFetch(apiUrl(`/api/message-filters/${encodeURIComponent(channel)}`), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await r.json();
  if (!r.ok || !body.ok) {
    feedback.textContent = `Erro: ${body.error || "falha ao salvar filtros"}`;
    return;
  }
  feedback.textContent = `Filtros globais de ${channel} salvos com sucesso.`;
  await fetchTemplates();
}

async function generateTemplatePreview() {
  const form = document.getElementById("templateForm");
  const payload = Object.fromEntries(new FormData(form).entries());
  const ch = String(payload.channel || "meta_lead");
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
    chegada_em: "20/04/2026 11:42:00",
    customer_id: "253-906-3374",
    period_start_br: "01/04/2026",
    period_end_br: "07/04/2026",
    conversions_block: "- Formulário: 12\n- WhatsApp: 8",
    campaigns_block: "1) *Campanha Busca*\n👁️ Impressoes: 12.300\n🖱️ Cliques: 550",
  };
  const bucket = customVariablesStorageKey(ch);
  const customs = state.templates.custom_variables?.[bucket] || [];
  customs.forEach((ent) => {
    const k = String(ent?.key || "").trim();
    if (!k) return;
    const map0 = ent.mappings && typeof ent.mappings === "object" ? Object.values(ent.mappings)[0] : "";
    sampleContext[k] = map0 != null && map0 !== "" ? String(map0) : "(exemplo)";
  });
  const r = await dashFetch(apiUrl("/api/message-templates/preview"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content: payload.content || "", context: sampleContext, channel: ch }),
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
  refreshOpenFlowModal();
}

function connectStream() {
  if (state.eventStream) state.eventStream.close();
  const es = new EventSource(apiUrl("/api/events/stream"));
  state.eventStream = es;
  es.addEventListener("open", () => setStreamStatus("live", "Stream: ao vivo (SSE)"));
  es.addEventListener("error", () => setStreamStatus("offline", "Stream: desligado (reconexão automática)"));
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
    "site-leads": document.getElementById("tab-site-leads"),
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
      if (btn.dataset.tab === "site-leads") {
        fetchSiteLeadRoutes().catch((e) => console.error(e));
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
    if (!prev) {
      const seeded = String(sel.dataset.currentValue || "").trim();
      if (seeded) prev = seeded;
    }
    const card = sel.closest(".client-card");
    const fieldName = String(sel.name || "").trim();
    if (card?.dataset?.clientId) {
      const cid = String(card.dataset.clientId);
      const metaClient = state.metaClients.find((x) => String(x.id) === cid);
      const googleClient = state.googleClients.find((x) => String(x.id) === cid);
      if (metaClient) {
        if (fieldName === "group_id") prev = String(metaClient.group_id || "").trim();
        else if (fieldName === "p12_report_group_id") prev = String(metaClient.p12_report_group_id || "").trim();
        else if (fieldName === "internal_notify_group_id") prev = String(metaClient.internal_notify_group_id || "").trim();
      }
      if (googleClient) {
        if (fieldName === "group_id") prev = String(googleClient.group_id || "").trim();
        else if (fieldName === "p12_report_group_id") prev = String(googleClient.p12_report_group_id || "").trim();
        else if (fieldName === "internal_notify_group_id") prev = String(googleClient.internal_notify_group_id || "").trim();
      }
    }
    const siteCard = sel.closest(".site-client-card");
    if (siteCard?.dataset?.routeId) {
      const rid = Number(siteCard.dataset.routeId || 0);
      const route = state.siteLeadRoutes.find((x) => Number(x.id) === rid);
      if (route) {
        if (fieldName === "group_id") prev = String(route.group_id || "").trim();
        else if (fieldName === "internal_notify_group_id") prev = String(route.internal_notify_group_id || "").trim();
      }
    }
    const known = new Set([""]);
    sel.replaceChildren();
    const ph = document.createElement("option");
    ph.value = "";
    ph.textContent = "Nenhum";
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
    sel.dataset.currentValue = "";
  });
}

/** Carrega grupos do catálogo; em falha lança Error com mensagem do servidor. */
async function loadCatalogGroupsPayload() {
  const res = await dashFetch(apiUrl("/api/catalog-groups"));
  if (!res.ok) throw new Error(await dashFetchErrorMessage(res, "/api/catalog-groups"));
  const data = await res.json().catch(() => ({}));
  state.catalogGroups = Array.isArray(data.groups) ? data.groups : [];
  renderCatalogGroups();
  syncCatalogGroupSelects();
}

async function fetchCatalogGroups() {
  setCatalogFeedback("", "");
  setGroupsStatus("A sincronizar…");
  const btn = document.getElementById("refreshCatalogGroupsBtn");
  if (btn) btn.disabled = true;
  try {
    try {
      await loadCatalogGroupsPayload();
    } catch (e) {
      const msg = e?.message || "Falha ao carregar grupos.";
      setCatalogFeedback(msg, "error");
      setGroupsStatus("");
      updateGroupsCount(state.catalogGroups.length);
      return;
    }
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
  bindMessagesSectionHelpModal();
  bindFlowModal();
  bindBootChecklistTooltip();
  document.getElementById("newClientForm").addEventListener("submit", submitNewMetaClient);
  document.getElementById("newGoogleClientForm").addEventListener("submit", submitNewGoogleClient);
  document.getElementById("templateForm").addEventListener("submit", saveTemplate);
  document.getElementById("filtersAddChannelBtn")?.addEventListener("click", addFilterChannel);
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
  document.getElementById("siteLeadRouteForm")?.addEventListener("submit", submitSiteLeadRoute);
  setupChipFields(document.getElementById("siteLeadRouteForm"), [
    "lead_exclude_fields",
    "lead_exclude_contains",
    "lead_exclude_regex",
  ]);
  document.getElementById("refreshSiteRoutesBtn")?.addEventListener("click", () =>
    fetchSiteLeadRoutes().catch((e) => console.error(e)),
  );
  setupChipFields(document.getElementById("newClientForm"), [
    "lead_exclude_fields",
    "lead_exclude_contains",
    "lead_exclude_regex",
  ]);
  applyAllFieldCopy();
  document.getElementById("varResSaveBtn")?.addEventListener("click", () =>
    saveVariableResolution().catch((e) => console.error(e)),
  );
  document.getElementById("varResAddVarBtn")?.addEventListener("click", () => openVrEditDialog("", true));
  setupVarResolutionEditDialog();
  document.getElementById("customVarsChannel")?.addEventListener("change", renderCustomVariablesPanel);
  document.getElementById("customVarsAddBtn")?.addEventListener("click", appendEmptyCustomVarCard);
  document.getElementById("customVarsSaveBtn")?.addEventListener("click", () =>
    saveCustomVariables().catch((e) => console.error(e)),
  );
}

async function runBootStep(name, fn) {
  try {
    const extra = await fn();
    if (extra && extra.state === "warn") {
      return { name, state: "warn", detail: extra.detail || "" };
    }
    return { name, state: "ok", detail: (extra && extra.detail) || "" };
  } catch (e) {
    return { name, state: "err", detail: e?.message || String(e) };
  }
}

function renderBootChecklistLoading() {
  const host = document.getElementById("bootChecklist");
  if (!host) return;
  host.hidden = false;
  const p = document.createElement("p");
  p.className = "boot-checklist-loading";
  p.textContent = "Carga inicial: a obter dados em paralelo…";
  host.replaceChildren(p);
  setBootChecklistButtonState([]);
}

function renderBootChecklist(steps) {
  const host = document.getElementById("bootChecklist");
  if (!host) return;
  if (!steps || !steps.length) {
    host.hidden = true;
    host.innerHTML = "";
    setBootChecklistButtonState([]);
    return;
  }
  host.hidden = false;
  const ul = document.createElement("ul");
  ul.className = "boot-checklist-list";
  for (const s of steps) {
    const li = document.createElement("li");
    li.className = `boot-check-item boot-state-${s.state}`;
    const mark = document.createElement("span");
    mark.className = "boot-check-mark";
    mark.textContent = s.state === "ok" ? "OK" : s.state === "warn" ? "Aviso" : "ERRO";
    const nm = document.createElement("span");
    nm.className = "boot-check-name";
    nm.textContent = s.name;
    li.append(mark, document.createTextNode(" "), nm);
    if (s.detail) {
      const det = document.createElement("span");
      det.className = "boot-check-detail";
      det.textContent = s.detail;
      li.appendChild(det);
    }
    ul.appendChild(li);
  }
  host.replaceChildren(ul);
  setBootChecklistButtonState(steps);
}

async function boot() {
  bindUI();
  setStreamStatus("pending", "Stream: a aguardar carga inicial…");
  renderBootChecklistLoading();

  const [s1, s2, s3, s4, s5, s6] = await Promise.all([
    runBootStep("Clientes Meta", () => fetchMetaClients()),
    runBootStep("Clientes Google", () => fetchGoogleClients()),
    runBootStep("Templates e filtros", () => fetchTemplates()),
    runBootStep("Grupos catálogo", () => loadCatalogGroupsPayload()),
    runBootStep("Catálogo Meta (contas/páginas)", async () => {
      await fetchMetaCatalogs();
      const wa = (state.metaCatalog.warningsAccounts || []).filter(Boolean);
      const wp = (state.metaCatalog.warningsPages || []).filter(Boolean);
      const w = [...wa, ...wp];
      if (w.length) return { state: "warn", detail: w.join(" · ") };
      return {};
    }),
    runBootStep("Rotas leads site", () => fetchSiteLeadRoutes()),
  ]);

  const steps = [s1, s2, s3, s4, s5, s6];
  renderBootChecklist(steps);

  const anyErr = steps.some((s) => s.state === "err");
  if (anyErr) {
    setStreamStatus("offline", "Stream: inativo (corrija erros na carga)");
  } else {
    setStreamStatus("pending", "Stream: a ligar…");
  }

  resetSiteLeadRouteForm();
  connectStream();
}

boot().catch((err) => {
  console.error(err);
  renderBootChecklist([{ name: "Inicialização (UI ou bind)", state: "err", detail: err?.message || String(err) }]);
  setStreamStatus("offline", "Stream: falha antes da carga API");
});
