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
        }
    },
}

TEMPLATE_VARIABLES: Dict[str, Dict[str, str]] = {
    "meta_lead": {
        "client_name": "Nome do cliente",
        "page_id": "ID da página Meta",
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
        "received_at": "Data/hora de recebimento do webhook",
    },
    "google_report": {
        "client_name": "Nome do cliente",
        "customer_id": "ID formatado da conta Google Ads",
        "period_start_br": "Data início em DD/MM/AAAA",
        "period_end_br": "Data fim em DD/MM/AAAA",
        "conversions_block": "Lista formatada de conversões",
        "campaigns_block": "Lista formatada de campanhas",
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
