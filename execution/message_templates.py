"""
Gerenciamento de templates de mensagem, variáveis e filtros de campos.
"""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from typing import Any, Dict

from execution.project_paths import ensure_data_dir, message_templates_json_path

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
        "lorena": {
            "name": "Meta Lead - Lorena",
            "description": "Template legado para Lorena (usa conteúdo padrão).",
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
    return {k: v for k, v in raw.items() if k != "filters" and isinstance(v, dict)}


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
    return {
        "channels": templates,
        "variables": TEMPLATE_VARIABLES,
        "filters": filters,
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
