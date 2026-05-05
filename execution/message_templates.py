"""
Gerenciamento de templates de mensagem, variáveis e filtros de campos.
"""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from execution.project_paths import ensure_data_dir, message_templates_json_path

_VAR_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")

# Chaves do documento JSON/DB que não são canais de template
_DOC_KEYS = frozenset({"filters", "variable_resolution", "custom_variables"})

# Variáveis de lead cujo valor vem de chaves do payload (não derivadas). Usado em meta_lead / site_lead.
LEAD_RESOLVABLE_SLOTS = frozenset(
    {
        "nome",
        "email",
        "whatsapp",
        "telefone_digitos",
        "page_path",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
    }
)

# Defaults alinhados a execution/meta_lead_webhook.py (ordem de tentativa)
_DEFAULT_LEAD_SOURCE_KEYS: Dict[str, Tuple[str, ...]] = {
    "nome": ("nome_completo", "nome", "full_name", "name"),
    "email": ("email",),
    "whatsapp": ("telefone", "phone_number", "phone", "mobile", "celular"),
    "telefone_digitos": ("telefone", "phone_number", "phone", "mobile", "celular"),
    "page_path": (
        "pagina",
        "page",
        "page_path",
        "pagePath",
        "path",
        "pathname",
        "url",
        "url_path",
        "landing_page",
        "landingPage",
        "landing_url",
    ),
    "utm_source": ("utm_source", "utmSource"),
    "utm_medium": ("utm_medium", "utmMedium"),
    "utm_campaign": ("utm_campaign", "utmCampaign"),
    "utm_term": ("utm_term", "utmTerm"),
    "utm_content": ("utm_content", "utmContent"),
}

def _is_extended_origin_var_name(name: str) -> bool:
    """Nome extra em «Origem dos campos» (além dos slots padrão). Torna-se `{{name}}` no contexto de lead."""
    s = (name or "").strip()
    if not s or not _VAR_NAME_RE.match(s):
        return False
    if s in LEAD_RESOLVABLE_SLOTS:
        return False
    if s in _RESERVED_LEAD_RENDER_KEYS:
        return False
    return True


# Nomes de variável já usados no contexto de lead (não permitir colisão com variável personalizada)
_RESERVED_LEAD_RENDER_KEYS = frozenset(
    {
        "client_name",
        "page_id",
        "template_id",
        "nome",
        "email",
        "whatsapp",
        "telefone_digitos",
        "form_name",
        "page_path",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "traffic_source",
        "traffic_origin_url",
        "origem_anuncio",
        "cliente_origem",
        "respostas",
        "respostas_filtradas",
        "respostas_raw",
        "respostas_omitidas",
        "respostas_count",
        "respostas_raw_count",
        "respostas_omitidas_count",
        "received_at",
        "chegada_em",
    }
)

_VAR_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")

DEFAULT_TEMPLATES: Dict[str, Dict[str, Dict[str, str]]] = {
    "meta_lead": {
        "default": {
            "name": "Meta Lead - Padrão",
            "description": "Mensagem padrão de novo lead para WhatsApp.",
            "content": (
                "Novo lead - {{client_name}}\n"
                "Recebido em: {{chegada_em}}\n"
                "Nome do Lead: {{nome}}\n"
                "WhatsApp do Lead: {{whatsapp}}\n"
                "E-mail do Lead: {{email}}\n\n"
                "==========\n\n"
                "Respostas do Lead:\n"
                "{{respostas}}"
            ),
        },
        "pratical_life": {
            "name": "Meta Lead - Pratical Life",
            "description": "Formato detalhado usado para Pratical Life.",
            "content": (
                "Novo lead recebido - {{client_name}}\n"
                "- Recebido em: {{chegada_em}}\n"
                "Contato:\n"
                "- Nome: {{nome}}\n"
                "- WhatsApp: {{whatsapp}}\n"
                "- E-mail: {{email}}\n"
                "- Nome do formulario: {{form_name}}\n\n"
                "Formulario:\n"
                "{{respostas}}"
            ),
        },
    },
    "site_lead": {
        "default": {
            "name": "Lead Site - Padrão",
            "description": "Template padrão de lead vindo do site (roteado por codi_id).",
            "content": (
                "Novo lead do site - {{client_name}}\n"
                "Recebido em: {{chegada_em}}\n"
                "Tráfego (inferido): {{traffic_source}}\n"
                "URL origem: {{traffic_origin_url}}\n"
                "Nome do Lead: {{nome}}\n"
                "WhatsApp do Lead: {{whatsapp}}\n"
                "E-mail do Lead: {{email}}\n\n"
                "==========\n\n"
                "Respostas do Lead:\n"
                "{{respostas}}"
            ),
        },
    },
    "google_report": {
        "default": {
            "name": "Google Report - Padrão",
            "description": "Template padrão para relatório Google Ads.",
            "content": (
                "*{{client_name}}*\n\n"
                "📊 *Relatorio Google Ads*\n"
                "🆔 *Conta:* {{customer_id}}\n"
                "📅 *Periodo (7 dias):* {{period_start_br}} a {{period_end_br}}\n\n"
                "🎯 *Conversoes primarias:*\n"
                "{{conversions_block}}\n\n"
                "📌 *Campanhas ativas (metricas por campanha):*\n"
                "{{campaigns_block}}"
            ),
        },
        "p12_resumo": {
            "name": "P12 — Resumo Google",
            "description": "Relatório semanal enxuto para grupo interno P12.",
            "content": (
                "📊 *P12 · Google Ads* — *{{client_name}}*\n"
                "🆔 {{customer_id}} · 📅 {{period_start_br}} a {{period_end_br}}\n\n"
                "🎯 *Conversoes primarias:*\n"
                "{{conversions_block}}\n"
            ),
        },
        "p12_dados": {
            "name": "P12 — Dados (campanhas)",
            "description": "Métricas por campanha para grupo interno P12.",
            "content": (
                "📌 *P12 · Campanhas* — *{{client_name}}*\n"
                "🆔 {{customer_id}} · 📅 {{period_start_br}} a {{period_end_br}}\n"
                "{{campaigns_block}}"
            ),
        },
    },
    "meta_report": {
        "default": {
            "name": "Meta — Relatório semanal (padrão)",
            "description": "Bloco da semana + comparativo; use no grupo do cliente ou P12.",
            "content": (
                "*{{client_name}}*\n\n"
                "{{week_report_block}}\n\n"
                "{{compare_report_block}}"
            ),
        },
        "p12_resumo": {
            "name": "P12 — Resumo Meta",
            "description": "Resumo da semana corrente para grupo P12.",
            "content": (
                "📊 *P12 · Meta Ads* — *{{client_name}}*\n"
                "📅 *Semana:* {{period_a_start_br}} a {{period_a_end_br}}\n\n"
                "{{week_report_block}}"
            ),
        },
        "p12_dados": {
            "name": "P12 — Comparativo Meta",
            "description": "Comparativo com a semana anterior (dados).",
            "content": (
                "📉 *P12 · Comparativo* — *{{client_name}}*\n"
                "📅 *Semana anterior:* {{period_b_start_br}} a {{period_b_end_br}}\n\n"
                "{{compare_report_block}}"
            ),
        },
    },
    "internal_lead": {
        "default": {
            "name": "Interno — cópia de lead",
            "description": "Mensagem para o grupo interno ao receber lead; variáveis iguais ao template de lead.",
            "content": (
                "🔔 *Interno* · {{client_name}}\n"
                "{{nome}} · {{chegada_em}}\n\n"
                "{{respostas}}"
            ),
        },
    },
    "internal_report": {
        "default": {
            "name": "Interno — aviso após relatório semanal",
            "description": (
                "Ping ao grupo interno após enviar relatório Meta/Google. "
                "Use variáveis conforme o canal (Meta: períodos A/B e report_full; Google: customer_id e período)."
            ),
            "content": (
                "📎 Relatório enviado ao cliente — *{{client_name}}*\n"
                "📅 {{period_label}}\n"
            ),
        },
    },
}

TEMPLATE_VARIABLES: Dict[str, Dict[str, str]] = {
    "meta_lead": {
        "client_name": "Nome do cliente",
        "page_id": "ID da página Meta",
        "page_path": "Path/rota da página onde ocorreu a conversão (ex.: /contato)",
        "utm_source": "UTM Source",
        "utm_medium": "UTM Medium",
        "utm_campaign": "UTM Campaign",
        "utm_term": "UTM Term",
        "utm_content": "UTM Content",
        "template_id": "ID do template aplicado",
        "nome": "Nome do lead (nome_completo, nome, full_name ou name no payload)",
        "email": "Email do lead",
        "whatsapp": "Link wa.me (telefone, phone_number, phone, mobile ou celular)",
        "telefone_digitos": "Telefone só dígitos (mesmas chaves que whatsapp)",
        "form_name": "Nome do formulário",
        "respostas": "Bloco de respostas filtradas",
        "respostas_filtradas": "Alias de respostas filtradas",
        "respostas_raw": "Bloco bruto sem filtros",
        "respostas_omitidas": "Perguntas removidas pelos filtros",
        "respostas_count": "Quantidade de respostas enviadas",
        "respostas_raw_count": "Quantidade de respostas brutas",
        "respostas_omitidas_count": "Quantidade de respostas removidas",
        "received_at": "Data/hora de recebimento do webhook (alias técnico)",
        "chegada_em": "Data/hora em que o lead chegou ao servidor (mesmo instante que received_at)",
        "traffic_source": "Origem inferida: meta, google ou unknown",
        "traffic_origin_url": "URL de origem (pagina/page_path) usada na inferência",
        "origem_anuncio": "Rótulo interno cadastrado no Leads Site (campanha/origem)",
        "cliente_origem": "Rótulo interno cadastrado no Leads Site (identificação / nome exibido)",
    },
    "site_lead": {
        "client_name": "Nome exibido (geralmente cliente_origem + origem_anuncio do cadastro Leads Site)",
        "page_id": "ID da página Meta (quando existir)",
        "page_path": "Path/rota da página onde ocorreu a conversão (ex.: /contato)",
        "utm_source": "UTM Source",
        "utm_medium": "UTM Medium",
        "utm_campaign": "UTM Campaign",
        "utm_term": "UTM Term",
        "utm_content": "UTM Content",
        "template_id": "ID do template aplicado",
        "nome": "Nome do lead (nome_completo, nome, full_name ou name no payload)",
        "email": "Email do lead",
        "whatsapp": "Link wa.me (telefone, phone_number, phone, mobile ou celular)",
        "telefone_digitos": "Telefone só dígitos (mesmas chaves que whatsapp)",
        "form_name": "Nome do formulário",
        "respostas": "Bloco de respostas filtradas",
        "respostas_filtradas": "Alias de respostas filtradas",
        "respostas_raw": "Bloco bruto sem filtros",
        "respostas_omitidas": "Perguntas removidas pelos filtros",
        "respostas_count": "Quantidade de respostas enviadas",
        "respostas_raw_count": "Quantidade de respostas brutas",
        "respostas_omitidas_count": "Quantidade de respostas removidas",
        "received_at": "Data/hora de recebimento do webhook (alias técnico)",
        "chegada_em": "Data/hora em que o lead chegou ao servidor (mesmo instante que received_at)",
        "traffic_source": "Origem inferida: meta, google ou unknown",
        "traffic_origin_url": "URL de origem (pagina/page_path) usada na inferência",
        "origem_anuncio": "Rótulo interno cadastrado no Leads Site (campanha/origem)",
        "cliente_origem": "Rótulo interno cadastrado no Leads Site (identificação / nome exibido)",
    },
    "google_report": {
        "client_name": "Nome do cliente",
        "customer_id": "ID formatado da conta Google Ads",
        "period_start_br": "Data início em DD/MM/AAAA",
        "period_end_br": "Data fim em DD/MM/AAAA",
        "period_label": "Intervalo do relatório (texto) — notificação interna",
        "conversions_block": "Lista formatada de conversões",
        "campaigns_block": "Lista formatada de campanhas",
    },
    "meta_report": {
        "client_name": "Nome do cliente",
        "period_a_start_br": "Início do período A (7 dias) em DD/MM/AAAA",
        "period_a_end_br": "Fim do período A em DD/MM/AAAA",
        "period_b_start_br": "Início do período B (semana anterior) em DD/MM/AAAA",
        "period_b_end_br": "Fim do período B em DD/MM/AAAA",
        "period_label": "Resumo do intervalo (ex.: DD/MM/AAAA a DD/MM/AAAA) — notificação interna",
        "week_report_block": "Texto formatado da semana atual",
        "compare_report_block": "Texto formatado do comparativo",
        "report_full": "Semana atual + comparativo já concatenados",
    },
    "internal_lead": {
        "client_name": "Nome do cliente",
        "page_id": "ID da página Meta",
        "page_path": "Path/rota da página onde ocorreu a conversão (ex.: /contato)",
        "utm_source": "UTM Source",
        "utm_medium": "UTM Medium",
        "utm_campaign": "UTM Campaign",
        "utm_term": "UTM Term",
        "utm_content": "UTM Content",
        "template_id": "ID do template de lead aplicado ao cliente",
        "nome": "Nome do lead",
        "email": "E-mail do lead",
        "whatsapp": "Link wa.me",
        "telefone_digitos": "Telefone só dígitos",
        "form_name": "Nome do formulário",
        "respostas": "Bloco de respostas (filtrado)",
        "respostas_filtradas": "Alias respostas filtradas",
        "respostas_raw": "Bloco bruto",
        "respostas_omitidas": "Perguntas omitidas",
        "respostas_count": "Qtd. respostas",
        "respostas_raw_count": "Qtd. respostas raw",
        "respostas_omitidas_count": "Qtd. omitidas",
        "received_at": "Data/hora recebimento",
        "chegada_em": "Data/hora chegada do lead",
        "traffic_source": "Origem inferida: meta, google ou unknown",
        "traffic_origin_url": "URL de origem (pagina/page_path) usada na inferência",
        "origem_anuncio": "Rótulo interno cadastrado no Leads Site (campanha/origem)",
        "cliente_origem": "Rótulo interno cadastrado no Leads Site (identificação / nome exibido)",
    },
    "internal_report": {
        "client_name": "Nome do cliente",
        "period_label": "Intervalo do relatório (texto livre)",
        "period_start_br": "Início (Google ou período único)",
        "period_end_br": "Fim",
        "period_a_start_br": "Meta — início período A",
        "period_a_end_br": "Meta — fim período A",
        "period_b_start_br": "Meta — início período B",
        "period_b_end_br": "Meta — fim período B",
        "customer_id": "Google Ads — ID da conta formatado",
        "report_full": "Meta — relatório completo (texto)",
        "week_report_block": "Meta — bloco semana atual",
        "compare_report_block": "Meta — bloco comparativo",
    },
}

DEFAULT_FILTER_RULES: Dict[str, Dict[str, Any]] = {
    "meta_lead": {
        "exclude_exact": [],
        "exclude_contains": ["utm_", "referencia"],
        "exclude_regex": [],
    }
}


def _templates_path() -> str:
    return message_templates_json_path()


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(base)
    for key, value in override.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = deepcopy(value)
    return out


def _use_db_templates() -> bool:
    try:
        from execution.persistence import db_enabled

        return db_enabled()
    except Exception:
        return False


def _channels_from_raw(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in raw.items() if k not in _DOC_KEYS and isinstance(v, dict)}


def load_templates() -> Dict[str, Dict[str, Dict[str, str]]]:
    if _use_db_templates():
        from execution.persistence import ensure_db_ready, get_message_templates_body

        ensure_db_ready()
        raw = get_message_templates_body()
        if not isinstance(raw, dict):
            return deepcopy(DEFAULT_TEMPLATES)
        merged = _deep_merge(DEFAULT_TEMPLATES, _channels_from_raw(raw))
        merged.pop("filters", None)
        return merged

    path = _templates_path()
    if not os.path.exists(path):
        return deepcopy(DEFAULT_TEMPLATES)
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return deepcopy(DEFAULT_TEMPLATES)
    if not isinstance(raw, dict):
        return deepcopy(DEFAULT_TEMPLATES)
    merged = _deep_merge(DEFAULT_TEMPLATES, _channels_from_raw(raw))
    merged.pop("filters", None)
    return merged


def save_templates(data: Dict[str, Dict[str, Dict[str, str]]]) -> None:
    if _use_db_templates():
        from execution.persistence import save_template_channels

        save_template_channels(data)
        return

    path = _templates_path()
    ensure_data_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def list_templates_payload() -> Dict[str, Any]:
    templates = load_templates()
    filters = load_filter_rules()
    base_vars: Dict[str, Dict[str, str]] = {k: dict(v) for k, v in TEMPLATE_VARIABLES.items()}
    label_extra = "Campo de origem adicional (chaves do payload, como os campos padrão)"
    vr = load_merged_variable_resolution()
    for ch, bucket in vr.items():
        if ch not in base_vars:
            continue
        t = base_vars[ch]
        for slot in bucket:
            if slot in LEAD_RESOLVABLE_SLOTS:
                continue
            if _is_extended_origin_var_name(slot) and slot not in t:
                t[slot] = label_extra
    if "meta_lead" in base_vars and "internal_lead" in base_vars:
        for k, v in base_vars["meta_lead"].items():
            if k not in base_vars["internal_lead"] and _is_extended_origin_var_name(k):
                base_vars["internal_lead"][k] = v
    return {
        "channels": templates,
        "variables": base_vars,
        "filters": filters,
        "variable_resolution": vr,
        "custom_variables": load_merged_custom_variables(),
    }


def upsert_template(channel: str, template_id: str, *, name: str, content: str, description: str = "") -> Dict[str, Any]:
    channel = (channel or "").strip()
    template_id = (template_id or "").strip()
    if not channel:
        raise ValueError("channel_obrigatorio")
    if not template_id:
        raise ValueError("template_id_obrigatorio")
    if not content.strip():
        raise ValueError("content_obrigatorio")
    templates = load_templates()
    channel_bucket = templates.setdefault(channel, {})
    channel_bucket[template_id] = {
        "name": (name or template_id).strip(),
        "description": (description or "").strip(),
        "content": content,
    }
    save_templates(templates)
    return channel_bucket[template_id]


def render_template_text(content: str, context: Dict[str, Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        value = context.get(key, "")
        if value is None:
            return ""
        return str(value)

    return _VAR_RE.sub(repl, content or "")


def get_template_content(channel: str, template_id: str) -> str:
    templates = load_templates()
    data = templates.get(channel, {}).get(template_id, {})
    if isinstance(data, dict):
        return str(data.get("content", "")).strip()
    return ""


def render_internal_lead_notify(client_or_route: Dict[str, Any], context: Dict[str, Any]) -> str:
    """Template do canal internal_lead, com fallback para internal_notify_message (legado)."""
    apply_custom_variables("internal_lead", context, resolve_payload=None)
    tid = str(client_or_route.get("internal_lead_template") or "").strip()
    if tid:
        body = get_template_content("internal_lead", tid)
        if body:
            return render_template_text(body, context)
    legacy = str(client_or_route.get("internal_notify_message") or "").strip()
    if legacy:
        return render_template_text(legacy, context)
    return ""


def render_internal_weekly_notify(client: Dict[str, Any], context: Dict[str, Any]) -> str:
    """Template do canal internal_report, com fallback para internal_notify_message (legado)."""
    apply_custom_variables("internal_report", context, resolve_payload=None)
    tid = str(client.get("internal_weekly_template") or "").strip()
    if tid:
        body = get_template_content("internal_report", tid)
        if body:
            return render_template_text(body, context)
    legacy = str(client.get("internal_notify_message") or "").strip()
    if legacy:
        return render_template_text(legacy, context)
    return ""


def load_filter_rules() -> Dict[str, Dict[str, Any]]:
    base = deepcopy(DEFAULT_FILTER_RULES)
    if _use_db_templates():
        from execution.persistence import ensure_db_ready, get_message_templates_body

        ensure_db_ready()
        raw = get_message_templates_body()
        if not isinstance(raw, dict):
            return base
        candidate = raw.get("filters")
        if isinstance(candidate, dict):
            for channel, channel_rules in candidate.items():
                if not isinstance(channel_rules, dict):
                    continue
                merged = base.setdefault(
                    channel,
                    {"exclude_exact": [], "exclude_contains": [], "exclude_regex": []},
                )
                for key in ("exclude_exact", "exclude_contains", "exclude_regex"):
                    vals = channel_rules.get(key)
                    if isinstance(vals, list):
                        merged[key] = [str(v).strip() for v in vals if str(v).strip()]
        return base

    path = _templates_path()
    if not os.path.exists(path):
        return base
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return base
    if not isinstance(raw, dict):
        return base
    candidate = raw.get("filters")
    if isinstance(candidate, dict):
        for channel, channel_rules in candidate.items():
            if not isinstance(channel_rules, dict):
                continue
            merged = base.setdefault(
                channel,
                {"exclude_exact": [], "exclude_contains": [], "exclude_regex": []},
            )
            for key in ("exclude_exact", "exclude_contains", "exclude_regex"):
                vals = channel_rules.get(key)
                if isinstance(vals, list):
                    merged[key] = [str(v).strip() for v in vals if str(v).strip()]
    return base


def get_filter_rules(channel: str) -> Dict[str, Any]:
    all_rules = load_filter_rules()
    channel_rules = all_rules.get(channel, {})
    if not isinstance(channel_rules, dict):
        return {"exclude_exact": [], "exclude_contains": [], "exclude_regex": []}
    return {
        "exclude_exact": [str(v).strip().lower() for v in channel_rules.get("exclude_exact", []) if str(v).strip()],
        "exclude_contains": [str(v).strip().lower() for v in channel_rules.get("exclude_contains", []) if str(v).strip()],
        "exclude_regex": [str(v).strip() for v in channel_rules.get("exclude_regex", []) if str(v).strip()],
    }


def upsert_filter_rules(
    channel: str,
    *,
    exclude_exact: list[str],
    exclude_contains: list[str],
    exclude_regex: list[str],
) -> Dict[str, Any]:
    filters_entry = {
        "exclude_exact": [str(v).strip() for v in exclude_exact if str(v).strip()],
        "exclude_contains": [str(v).strip() for v in exclude_contains if str(v).strip()],
        "exclude_regex": [str(v).strip() for v in exclude_regex if str(v).strip()],
    }

    if _use_db_templates():
        from execution.persistence import get_message_templates_body, save_message_templates_body

        data = get_message_templates_body()
        if not isinstance(data, dict):
            data = {}
        filters = data.get("filters")
        if not isinstance(filters, dict):
            filters = {}
        filters[channel] = filters_entry
        data["filters"] = filters
        save_message_templates_body(data)
        return filters_entry

    path = _templates_path()
    data = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                data = raw
        except (OSError, json.JSONDecodeError):
            data = {}
    filters = data.get("filters")
    if not isinstance(filters, dict):
        filters = {}
    filters[channel] = filters_entry
    data["filters"] = filters
    ensure_data_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return filters_entry


def _load_full_templates_document() -> Dict[str, Any]:
    if _use_db_templates():
        from execution.persistence import ensure_db_ready, get_message_templates_body

        ensure_db_ready()
        raw = get_message_templates_body()
        return raw if isinstance(raw, dict) else {}
    path = _templates_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _save_full_templates_document(data: Dict[str, Any]) -> None:
    if _use_db_templates():
        from execution.persistence import save_message_templates_body

        save_message_templates_body(data)
        return
    ensure_data_dir()
    path = _templates_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _default_variable_resolution() -> Dict[str, Dict[str, Dict[str, Any]]]:
    slot_defaults = {
        slot: {"source_keys": list(keys)} for slot, keys in _DEFAULT_LEAD_SOURCE_KEYS.items()
    }
    return {"meta_lead": deepcopy(slot_defaults), "site_lead": deepcopy(slot_defaults)}


def variable_resolution_storage_channel(template_channel: str) -> Optional[str]:
    """ONde a origem de campos é guardada. Só canais de lead; `internal_lead` → `meta_lead` (P12)."""
    c = (template_channel or "").strip() or "meta_lead"
    if c in ("meta_lead", "internal_lead"):
        return "meta_lead"
    if c == "site_lead":
        return "site_lead"
    return None


def _variable_resolution_target_channel(stored_key: str) -> Optional[str]:
    """Chave no JSON → bucket de merge (só meta_lead / site_lead). Ignora chaves alheias (ex. relatórios)."""
    if not isinstance(stored_key, str):
        return "meta_lead"
    c = (stored_key or "").strip()
    if c in ("meta_lead", "internal_lead"):
        return "meta_lead"
    if c == "site_lead":
        return "site_lead"
    return None


def load_merged_variable_resolution() -> Dict[str, Dict[str, Dict[str, Any]]]:
    out = _default_variable_resolution()
    raw_doc = _load_full_templates_document()
    stored = raw_doc.get("variable_resolution")
    if not isinstance(stored, dict):
        return out
    # Reagrupar chaves (ex.: variable_resolution["internal_lead"] → meta_lead)
    normalized: Dict[str, Dict[str, Any]] = {}
    for ch, over in stored.items():
        if not isinstance(ch, str) or not isinstance(over, dict):
            continue
        target = _variable_resolution_target_channel(ch)
        if not target:
            continue
        dest = normalized.setdefault(target, {})
        for var_name, meta in over.items():
            if not isinstance(var_name, str) or not isinstance(meta, dict):
                continue
            if var_name in LEAD_RESOLVABLE_SLOTS or _is_extended_origin_var_name(var_name):
                sk = meta.get("source_keys")
                if isinstance(sk, list) and sk:
                    clean = [str(x).strip() for x in sk if str(x).strip()]
                    if clean:
                        dest[var_name] = {"source_keys": clean}
    for ch, bucket in normalized.items():
        out_b = out.setdefault(ch, {})
        for var_name, meta in bucket.items():
            out_b[var_name] = meta
    return out


def resolution_channel_for_lead(template_channel: str) -> str:
    """internal_lead partilha resolução com meta_lead."""
    c = (template_channel or "meta_lead").strip() or "meta_lead"
    if c == "internal_lead":
        return "meta_lead"
    if c == "site_lead":
        return "site_lead"
    return "meta_lead"


def custom_variables_storage_channel(template_channel: str) -> str:
    """Bucket em `custom_variables` (não confundir com `resolution_channel_for_lead`)."""
    c = (template_channel or "meta_lead").strip() or "meta_lead"
    if c == "internal_lead":
        return "meta_lead"
    return c


def get_effective_source_keys(channel: str, slot: str) -> Tuple[str, ...]:
    if slot not in LEAD_RESOLVABLE_SLOTS and not _is_extended_origin_var_name(slot):
        return ()
    ch = resolution_channel_for_lead(channel)
    merged = load_merged_variable_resolution()
    bucket = merged.get(ch) or {}
    entry = bucket.get(slot) or {}
    sk = entry.get("source_keys")
    if isinstance(sk, list) and sk:
        t = tuple(str(x).strip() for x in sk if str(x).strip())
        if t:
            return t
    if slot in LEAD_RESOLVABLE_SLOTS:
        return _DEFAULT_LEAD_SOURCE_KEYS.get(slot, ())
    return ()


def _forbidden_custom_var_output_key(storage_ch: str, key: str) -> bool:
    t = TEMPLATE_VARIABLES.get(storage_ch)
    if isinstance(t, dict) and t:
        return key in t
    return key in _RESERVED_LEAD_RENDER_KEYS


def _validate_custom_variable_entry(
    item: Dict[str, Any],
    *,
    storage_ch: str,
    template_ch: str,
    sibling_keys: Set[str],
) -> Optional[Dict[str, Any]]:
    key = str(item.get("key", "")).strip()
    if not key or not _VAR_NAME_RE.match(key):
        return None
    if _forbidden_custom_var_output_key(storage_ch, key):
        return None
    src_raw = str(item.get("source", "payload") or "payload").strip().lower()
    if src_raw not in ("payload", "context"):
        src_raw = "payload"
    sk = item.get("source_keys")
    if not isinstance(sk, list) or not sk:
        return None
    source_keys = [str(x).strip() for x in sk if str(x).strip()]
    if not source_keys:
        return None
    if src_raw == "context":
        vr_merge = load_merged_variable_resolution().get(resolution_channel_for_lead(template_ch), {}) or {}
        from_vr = set(vr_merge.keys()) if isinstance(vr_merge, dict) else set()
        allowed = set(TEMPLATE_VARIABLES.get(template_ch, {}) or {}) | (sibling_keys - {key}) | from_vr
        for ckey in source_keys:
            if ckey not in allowed:
                return None
    mappings = item.get("mappings")
    if mappings is None:
        mappings = {}
    if not isinstance(mappings, dict):
        return None
    map_str = {str(kk): str(v) for kk, v in mappings.items()}
    default = str(item.get("default", ""))
    norm = item.get("normalize")
    if not isinstance(norm, dict):
        norm = {"trim": True, "lower": False}
    return {
        "key": key,
        "source": src_raw,
        "source_keys": source_keys,
        "mappings": map_str,
        "default": default,
        "normalize": norm,
    }


def _custom_vars_from_stored_list(
    storage_ch: str,
    template_ch: str,
    raw_list: list,
) -> List[Dict[str, Any]]:
    sib: Set[str] = set()
    for it in raw_list or []:
        if isinstance(it, dict) and str(it.get("key", "")).strip():
            sib.add(str(it.get("key", "")).strip())
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for it in raw_list or []:
        if not isinstance(it, dict):
            continue
        ent = _validate_custom_variable_entry(
            it, storage_ch=storage_ch, template_ch=template_ch, sibling_keys=sib
        )
        if not ent or ent["key"] in seen:
            continue
        seen.add(ent["key"])
        out.append(ent)
    return out


def load_merged_custom_variables() -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    raw_doc = _load_full_templates_document()
    stored = raw_doc.get("custom_variables")
    if not isinstance(stored, dict):
        for ch in ("meta_lead", "site_lead", "google_report", "meta_report", "internal_report"):
            out[ch] = []
        return out
    legacy_internal: Optional[List[Any]] = None
    for ch, raw_list in stored.items():
        if ch == "internal_lead" and isinstance(raw_list, list):
            legacy_internal = raw_list
            continue
        if not isinstance(ch, str) or not isinstance(raw_list, list):
            continue
        out[ch] = _custom_vars_from_stored_list(ch, ch, raw_list)
    if legacy_internal is not None:
        extra = _custom_vars_from_stored_list("meta_lead", "internal_lead", legacy_internal)
        bucket = out.setdefault("meta_lead", [])
        seen = {d["key"] for d in bucket}
        for ent in extra:
            if ent["key"] not in seen:
                bucket.append(ent)
                seen.add(ent["key"])
    for ch in ("meta_lead", "site_lead", "google_report", "meta_report", "internal_report"):
        out.setdefault(ch, [])
    return out


def get_custom_variable_defs_for_channel(template_channel: str) -> List[Dict[str, Any]]:
    ch = custom_variables_storage_channel(template_channel)
    all_cv = load_merged_custom_variables()
    return list(all_cv.get(ch, []))


def _context_value_to_raw_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float, bool)):
        return str(v).strip()
    if isinstance(v, (list, dict)):
        return str(v).strip() if v else ""
    return str(v).strip()


def apply_custom_variables(
    template_channel: str,
    render_ctx: Dict[str, Any],
    *,
    resolve_payload: Optional[Callable[[Tuple[str, ...]], str]] = None,
) -> None:
    """Aplica definições de `custom_variables` a `render_ctx` (in-place, ordem + multi-passo).

    * `source=payload` (padrão): requer `resolve_payload`; se `None` (p.ex. preview sem payload),
      a entrada é ignorada (mantém o que já estiver no contexto, p.ex. exemplos do dashboard).
    * `source=context`: procura a primeira chave de `source_keys` com valor não vazio no contexto.
    """
    defs = get_custom_variable_defs_for_channel(template_channel)
    if not defs:
        return
    max_rounds = max(8, min(48, max(len(defs) * 3, 8)))
    for _ in range(max_rounds):
        progressed = False
        for d in defs:
            out_key = d["key"]
            src = d.get("source") or "payload"
            if src == "context":
                raw = ""
                for ckey in d.get("source_keys") or ():
                    s = _context_value_to_raw_str(render_ctx.get(ckey))
                    if s:
                        raw = s
                        break
            else:
                if resolve_payload is None:
                    continue
                raw = (resolve_payload(tuple(d.get("source_keys") or ())) or "").strip()
            new_val = map_custom_variable_display(
                raw,
                d.get("mappings") or {},
                default=str(d.get("default", "")),
                normalize=d.get("normalize"),
            )
            prev_s = _context_value_to_raw_str(render_ctx.get(out_key)) if out_key in render_ctx else ""
            if new_val != prev_s:
                render_ctx[out_key] = new_val
                progressed = True
        if not progressed:
            break


def map_custom_variable_display(
    raw: str,
    mappings: Dict[str, str],
    *,
    default: str = "",
    normalize: Optional[Dict[str, Any]] = None,
) -> str:
    norm = normalize if isinstance(normalize, dict) else {}
    use_lower = bool(norm.get("lower"))
    use_trim = bool(norm.get("trim", True))

    def norm_key(s: str) -> str:
        t = s.strip() if use_trim else s
        return t.lower() if use_lower else t

    raw_stripped = (raw or "").strip()
    if not mappings:
        return raw_stripped if raw_stripped else default

    if not raw_stripped:
        return default

    rk = norm_key(raw_stripped)
    for mk, mv in mappings.items():
        if norm_key(str(mk)) == rk:
            return str(mv)
    return default if default else raw_stripped


def upsert_variable_resolution_channel(channel: str, payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    ch = (channel or "").strip()
    if not ch:
        raise ValueError("channel_obrigatorio")
    storage = variable_resolution_storage_channel(ch)
    if not storage:
        raise ValueError("canal_sem_origem_de_campos_lead")
    bucket: Dict[str, Dict[str, Any]] = {}
    for var_name, meta in (payload or {}).items():
        if not isinstance(var_name, str) or not isinstance(meta, dict):
            continue
        if not (var_name in LEAD_RESOLVABLE_SLOTS or _is_extended_origin_var_name(var_name)):
            continue
        sk = meta.get("source_keys")
        if not isinstance(sk, list):
            continue
        clean = [str(x).strip() for x in sk if str(x).strip()]
        if not clean:
            continue
        bucket[var_name] = {"source_keys": clean}

    data = _load_full_templates_document()
    vr = data.get("variable_resolution")
    if not isinstance(vr, dict):
        vr = {}
    vr[storage] = bucket
    if storage == "meta_lead" and "internal_lead" in vr:
        del vr["internal_lead"]
    data["variable_resolution"] = vr
    _save_full_templates_document(data)
    merged = load_merged_variable_resolution()
    return merged.get(storage, {})


def upsert_custom_variables_channel(channel: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ch = (channel or "").strip()
    if not ch:
        raise ValueError("channel_obrigatorio")
    storage_ch = custom_variables_storage_channel(ch)
    sibling: Set[str] = set()
    for item in items or []:
        if isinstance(item, dict) and str(item.get("key", "")).strip():
            sibling.add(str(item.get("key", "")).strip())
    cleaned: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in items or []:
        if not isinstance(item, dict):
            continue
        ent = _validate_custom_variable_entry(
            item, storage_ch=storage_ch, template_ch=ch, sibling_keys=sibling
        )
        if not ent or ent["key"] in seen:
            continue
        seen.add(ent["key"])
        cleaned.append(ent)

    data = _load_full_templates_document()
    cv = data.get("custom_variables")
    if not isinstance(cv, dict):
        cv = {}
    cv[storage_ch] = cleaned
    if storage_ch == "meta_lead" and "internal_lead" in cv:
        del cv["internal_lead"]
    data["custom_variables"] = cv
    _save_full_templates_document(data)
    return list(load_merged_custom_variables().get(storage_ch, []))


def list_template_placeholder_keys(content: str) -> List[str]:
    if not content:
        return []
    return list(dict.fromkeys(_VAR_RE.findall(content or "")))
