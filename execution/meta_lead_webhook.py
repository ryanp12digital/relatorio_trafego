"""
Webhook HTTP: leads -> mensagem formatada no grupo WhatsApp.

Rotas públicas de lead (cada uma com segredo opcional próprio no .env):
- POST /meta-new-lead   — só Meta (page_id → clientes Meta na Pulseboard)
- POST /google-new-lead — só Google Ads (google_customer_id → google_clients)
- POST /site-new-lead   — só site (codi_id → site_lead_routes)

Outros POST no mesmo processo:
- POST /evolution-webhook — catálogo de grupos Evolution (EVOLUTION_CATALOG_WEBHOOK_SECRET)
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import sys
import unicodedata
import requests
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, redirect, render_template, request, session

# Raiz do projeto no path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.evolution_client import get_evolution_client
from execution.persistence import is_valid_site_codi_id
from execution import dashboard_app as dashboard_module
from execution.flask_server import serve_flask_app
from execution.live_events import publish_event
from execution.message_templates import (
    LEAD_RESOLVABLE_SLOTS,
    apply_custom_variables,
    get_effective_source_keys,
    get_filter_rules,
    get_template_content,
    load_merged_variable_resolution,
    render_internal_lead_notify,
    render_template_text,
    resolution_channel_for_lead,
)
from execution.pretty_logging import configure_process_logging, pipes_to_lines
from execution.project_paths import clients_json_path, google_clients_json_path
from execution.secret_strings import constant_time_str_equal

log_dir = os.path.join(os.path.dirname(__file__), "..", ".tmp")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "execution.log")
configure_process_logging(log_file=log_file, level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "..", "templates"),
    static_folder=os.path.join(os.path.dirname(__file__), "..", "static"),
)
app.secret_key = (
    os.environ.get("DASHBOARD_SESSION_SECRET")
    or os.environ.get("FLASK_SECRET_KEY")
    or secrets.token_hex(32)
)

LOG_PREFIX = "[P12_META_LEAD_WEBHOOK]"
if not (os.environ.get("DASHBOARD_SESSION_SECRET") or os.environ.get("FLASK_SECRET_KEY")):
    logger.warning(
        "%s DASHBOARD_SESSION_SECRET/FLASK_SECRET_KEY ausentes: cookie de sessão muda a cada arranque. "
        "Defina em produção.",
        LOG_PREFIX,
    )
_LEAD_WEBHOOK_PATHS = frozenset({"/meta-new-lead", "/google-new-lead", "/site-new-lead"})
# Campos promovidos ao cabeçalho ({{nome}}, {{whatsapp}}, etc.) — não repetir no bloco "Respostas".
_EXCLUDE_RESPOSTAS = frozenset(
    {
        "nome_completo",
        "nome",
        "full_name",
        "name",
        "email",
        "telefone",
        "phone_number",
        "phone",
        "mobile",
        "celular",
        "page_id",
        "pageId",
    }
)
_TRAFFIC_EXPLICIT_KEYS = ("traffic_source", "fonte", "trafego", "canal_trafego", "source")
_URL_HINT_KEYS = (
    "pagina",
    "page",
    "url",
    "landing_url",
    "page_path",
    "pagePath",
    "path",
    "referrer",
    "referer",
    "referrer_url",
    "landing_page",
    "landingPage",
)
# Tokens para heurística (normalizado sem acentos, minúsculo).
_GOOGLE_TOKENS = (
    "google",
    "gads",
    "adwords",
    "googleads",
    "gclid",
    "gbraid",
    "wbraid",
    "youtube",
    "youtu",
    "dv360",
    "gmail",
    "gdoubleclick",
)
_META_TOKENS = (
    "meta",
    "facebook",
    "face",
    "instagram",
    "insta",
    "fb",
    "ig",
    "mta",
    "lfacebook",
    "fbinternal",
    "fbanalytics",
    "fbbusiness",
    "fbinstagram",
)
_LEAD_BODY_SIGNAL_KEYS = frozenset(
    {"email", "nome_completo", "telefone", "nome", "full_name", "name", "phone_number", "phone", "mobile", "celular"}
)
# Envelope típico Evolution/Make quando o POST errado chega ao /meta-new-lead (event, instance, data…).
_EVOLUTION_ENVELOPE_MARKERS = frozenset(
    ("event", "instance", "server_url", "sender", "apikey", "destination", "date_time")
)
_WHATSAPP_MSG_MAX = 4000
_META_GRAPH_BASE_URL = "https://graph.facebook.com/v18.0"
_FORM_NAME_CACHE: Dict[str, str] = {}

# Chaves em body/data/mappable para nome do formulário (Make, Meta, instant forms).
_FORM_NAME_PAYLOAD_KEYS: Tuple[str, ...] = (
    "form_name",
    "formName",
    "formulario",
    "formulário",
    "nome_formulario",
    "nome_form",
    "form_title",
    "formTitle",
    "form_label",
    "formLabel",
    "nome_do_formulario",
    "nome_do_formulário",
    "titulo_formulario",
    "titulo_formulário",
    "titulo_do_formulario",
    "titulo_do_formulário",
    "leadgen_form_name",
    "leadgenFormName",
    "form_name_display",
    "nome_formulario_meta",
    "instant_form_name",
)
_CODI_ID_PAYLOAD_KEYS: Tuple[str, ...] = (
    "codi_id",
    "codiid",
    "codiId",
    "form_id",
    "formid",
    "formId",
    "lead_form_id",
    "leadFormId",
)
_NATIVE_FORM_ID_KEYS: Tuple[str, ...] = ("form_id", "formid", "formId", "lead_form_id", "leadFormId")
_GOOGLE_CUSTOMER_ID_KEYS: Tuple[str, ...] = (
    "google_customer_id",
    "googleCustomerId",
    "customer_id",
    "customerId",
    "google_ads_customer_id",
    "googleAdsCustomerId",
    "gads_customer_id",
    "gadsCustomerId",
)


def _emoji_for_log(message: str) -> str:
    text = message.upper()
    if "SERVICO_INICIADO" in text:
        return "🚀"
    if "NEGADO_AUTH" in text or "ERRO_AUTH" in text or "WEBHOOK_SECRET_NEGADO" in text:
        return "🔒"
    if "ERRO_JSON" in text or "PAYLOAD_NAO_E_JSON" in text:
        return "📛"
    if "IGNORADO" in text:
        return "⏭️"
    if "RECEBIDO" in text:
        return "📥"
    if "PAYLOAD_OK" in text:
        return "📦"
    if "ROTA_RESOLVIDA" in text or "ROUTE_RESOLVED" in text:
        return "🧭"
    if "DRY_RUN" in text:
        return "🧪"
    if "WHATSAPP_ENVIADO_OK" in text or "OK_WHATSAPP" in text:
        return "📤"
    if "ERRO_WHATSAPP" in text or "WHATSAPP_FALHA" in text or "EVOLUTION_SEND" in text and "FALHOU" in text:
        return "📵"
    if "ERRO_EXCECAO" in text or "EXCECAO_ENVIO" in text:
        return "💥"
    if "ERRO_RESPOSTA" in text:
        return "🔴"
    if "CONCLUIDO_OK" in text:
        return "✅"
    if text.startswith("LEAD_") or "| LEAD_" in text:
        return "👤"
    if "ERRO" in text or "FALHA" in text or "EXCECAO" in text:
        return "❌"
    return "ℹ️"


def _wh_log(message: str, level: int = logging.INFO) -> None:
    emoji = _emoji_for_log(message)
    body = pipes_to_lines(message)
    logger.log(level, "%s %s\n%s", LOG_PREFIX, emoji, body)


def _emit_runtime_event(
    *,
    stage: str,
    status: str,
    detail: str,
    client_name: str = "",
    page_id: str = "",
    group_id: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    publish_event(
        source="meta_lead_webhook",
        stage=stage,
        status=status,
        detail=detail,
        client_name=client_name,
        page_id=page_id,
        group_id=group_id,
        payload=payload,
    )


def _client_ip() -> str:
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    xri = (request.headers.get("X-Real-IP") or "").strip()
    if xri:
        return xri
    return request.remote_addr or "?"


def _load_env() -> None:
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)


def _digits_only(phone: Optional[str]) -> str:
    if not phone:
        return ""
    return re.sub(r"\D", "", str(phone))


def _fallback_whatsapp_text() -> str:
    return (os.getenv("META_LEAD_FALLBACK_WHATSAPP") or "").strip()


def _meta_access_token() -> str:
    return (os.getenv("META_ACCESS_TOKEN") or "").strip()


def _graph_get_object_fields(object_id: str, fields: str) -> Dict[str, Any]:
    token = _meta_access_token()
    if not token or not object_id or not fields:
        return {}
    try:
        response = requests.get(
            f"{_META_GRAPH_BASE_URL}/{object_id}",
            params={"fields": fields, "access_token": token},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("Falha ao consultar Graph API para object_id=%s: %s", object_id, e)
        return {}


def _format_whatsapp_line(raw_phone: Optional[str]) -> str:
    digits = _digits_only(raw_phone)
    if digits:
        return f"https://wa.me/{digits}"
    fallback = _fallback_whatsapp_text()
    if fallback:
        return fallback
    return "(nao informado)"


def _mappable_lookup(mappable: List[Dict[str, Any]], name: str) -> str:
    target = (name or "").strip()
    if not target:
        return ""
    target_cf = target.casefold()
    for row in mappable:
        if not isinstance(row, dict):
            continue
        rn = str(row.get("name", "")).strip()
        if rn == target or rn.casefold() == target_cf:
            v = row.get("value")
            return _format_field_value(v)
    return ""


def _pick_ci(container: Any, key: str) -> Any:
    """Busca chave em dicionário sem diferenciar maiúsculas/minúsculas."""
    if not isinstance(container, dict):
        return None
    target = str(key or "").strip()
    if not target:
        return None
    direct = container.get(target)
    if direct is not None:
        return direct
    target_cf = target.casefold()
    for k, v in container.items():
        if str(k).strip().casefold() == target_cf:
            return v
    return None


def _first_field_from_data_and_mappable(
    keys: Tuple[str, ...],
    data: Dict[str, Any],
    mappable: List[Dict[str, Any]],
) -> str:
    """Primeiro valor não vazio: chave em `data` ou em `mappable_field_data` (ordem de `keys`)."""
    for key in keys:
        v = _format_field_value(data.get(key))
        if v:
            return v
        m = _mappable_lookup(mappable, key)
        if m:
            return m
    return ""


def _format_field_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        parts = [_format_field_value(x) for x in v]
        return ", ".join(p for p in parts if p)
    return str(v).strip()


def _build_respostas_text(mappable: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for row in mappable:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if not name or name in _EXCLUDE_RESPOSTAS:
            continue
        val = _format_field_value(row.get("value"))
        lines.append(f"*{name}:* {val}")
    return "\n".join(lines)


def _normalize_field_name(name: str) -> str:
    return str(name or "").strip().lower()


def _is_field_excluded(
    field_name: str,
    *,
    global_rules: Dict[str, Any],
    client_rules: Dict[str, List[str]],
) -> bool:
    normalized = _normalize_field_name(field_name)
    if not normalized:
        return True
    if normalized in _EXCLUDE_RESPOSTAS:
        return True

    exact = set(global_rules.get("exclude_exact", [])) | set(client_rules.get("exclude_exact", []))
    if normalized in exact:
        return True

    contains_rules = list(global_rules.get("exclude_contains", [])) + list(client_rules.get("exclude_contains", []))
    for token in contains_rules:
        token = _normalize_field_name(token)
        if token and token in normalized:
            return True

    regex_rules = list(global_rules.get("exclude_regex", [])) + list(client_rules.get("exclude_regex", []))
    for pattern in regex_rules:
        try:
            if pattern and re.search(pattern, normalized, re.IGNORECASE):
                return True
        except re.error:
            continue
    return False


def _build_respostas_bundle(
    mappable: List[Dict[str, Any]],
    *,
    global_rules: Dict[str, Any],
    client_rules: Dict[str, List[str]],
) -> Dict[str, Any]:
    filtered_lines: List[str] = []
    raw_lines: List[str] = []
    omitted_names: List[str] = []

    for row in mappable:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        val = _format_field_value(row.get("value"))
        line = f"*{name}:* {val}"
        raw_lines.append(line)

        if _is_field_excluded(name, global_rules=global_rules, client_rules=client_rules):
            omitted_names.append(name)
            continue
        filtered_lines.append(line)

    filtered_text = "\n".join(filtered_lines) if filtered_lines else "(nenhuma resposta adicional)"
    raw_text = "\n".join(raw_lines) if raw_lines else "(nenhuma resposta adicional)"
    omitted_text = ", ".join(omitted_names) if omitted_names else "(nenhuma)"
    return {
        "filtered_text": filtered_text,
        "raw_text": raw_text,
        "omitted_text": omitted_text,
        "filtered_count": len(filtered_lines),
        "raw_count": len(raw_lines),
        "omitted_count": len(omitted_names),
    }


def _unwrap_json_strings(raw: Any, max_depth: int = 8) -> Any:
    cur = raw
    for _ in range(max_depth):
        if not isinstance(cur, str):
            break
        s = cur.strip().lstrip("\ufeff")
        if not s:
            cur = None
            break
        try:
            cur = json.loads(s)
        except json.JSONDecodeError:
            break
    return cur


def _is_meta_lead_body(d: Dict[str, Any]) -> bool:
    top_level_keys = {str(k).strip().lower() for k in d.keys()}
    if any(sig in top_level_keys for sig in _LEAD_BODY_SIGNAL_KEYS):
        return True
    fd = d.get("field_data")
    if isinstance(fd, list) and len(fd) > 0:
        return True
    data = d.get("data")
    if not isinstance(data, dict):
        return False
    if d.get("leadgenId") is not None:
        return True
    if isinstance(d.get("mappable_field_data"), list):
        return True
    data_keys_lower = {str(k).strip().lower() for k in data.keys()}
    if any(sig in data_keys_lower for sig in _LEAD_BODY_SIGNAL_KEYS):
        return True
    # Lead site: codi / form id dentro de `data`
    for ck in _CODI_ID_PAYLOAD_KEYS:
        if str(ck).strip().lower() in data_keys_lower:
            return True
    return False


def _unwrap_evolution_style_envelope(d: Dict[str, Any]) -> Optional[Any]:
    """
    Quando o Make/n8n envia o envelope da Evolution (apikey, event, instance, data…)
    mas o lead real está em `data` (objeto ou string JSON), extrai o interior.
    Evita PAYLOAD_SEM_LEAD_RECONHECIDO quando só falta desembrulhar.
    """
    if "data" not in d:
        return None
    keys_l = {str(k).strip().lower() for k in d.keys()}
    # Pelo menos 2 marcas OU o par típico event+instance (webhook Evolution)
    if len(keys_l & _EVOLUTION_ENVELOPE_MARKERS) < 2 and not (
        "event" in keys_l and "instance" in keys_l
    ):
        return None
    inner = d.get("data")
    inner = _unwrap_json_strings(inner) if isinstance(inner, str) else inner
    return inner


def _looks_like_evolution_whatsapp_event(inner: Any) -> bool:
    """
    True se `data` do envelope Evolution é evento de chat/grupo (Baileys), não um lead.
    Evita 400 quando o URL do Make aponta /meta-new-lead em vez de /evolution-webhook.
    """
    if isinstance(inner, list):
        if not inner:
            return False
        sample = [x for x in inner[:5] if isinstance(x, dict)]
        return bool(sample) and all(_looks_like_evolution_whatsapp_event(x) for x in sample)
    if not isinstance(inner, dict):
        return False
    lk = {str(k).strip().lower() for k in inner.keys()}
    if "messages" in lk and isinstance(inner.get("messages"), list):
        return True
    if "key" in lk and "message" in lk:
        return True
    if inner.get("message") is not None and inner.get("key") is not None:
        return True
    ev = str(inner.get("event") or inner.get("type") or "").lower()
    if ev.startswith("messages.") or ev.startswith("chats.") or ev.startswith("contacts."):
        return True
    if ev in ("presence.update", "groups.update", "group-participants.update"):
        return True
    return False


def _coerce_inner_body(val: Any) -> Optional[Dict[str, Any]]:
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        unwrapped = _unwrap_json_strings(val)
        if isinstance(unwrapped, dict):
            return unwrapped
    return None


def _inject_field_data_as_mappable(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte resposta Meta (Graph) com `field_data` [{name, values}] em
    `data` + `mappable_field_data` usados pelo resto do pipeline.
    """
    fd = body.get("field_data")
    if not isinstance(fd, list) or not fd:
        return body
    mappable_existing = body.get("mappable_field_data")
    if isinstance(mappable_existing, list) and len(mappable_existing) > 0:
        return body
    data_existing = body.get("data")
    if isinstance(data_existing, dict) and len(data_existing) > 0:
        return body  # ja ha campos em data (nao sobrescrever)
    data: Dict[str, Any] = {}
    mappable: List[Dict[str, Any]] = []
    for row in fd:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        values = row.get("values")
        if isinstance(values, list) and len(values) > 0:
            val = values[0]
        else:
            val = row.get("value")
        data[name] = val
        mappable.append({"name": name, "value": val})
    if not mappable:
        return body
    out = dict(body)
    out["data"] = data
    out["mappable_field_data"] = mappable
    return out


def _inject_flat_payload_as_data(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Suporta payloads "flat" (ex.: n8n) onde os campos do lead ficam no topo do body:
    {"NOME":"...", "TELEFONE":"...", ...}
    """
    if not isinstance(body, dict):
        return body

    if isinstance(body.get("data"), dict):
        return body
    if isinstance(body.get("mappable_field_data"), list) and body.get("mappable_field_data"):
        return body

    lowered = {str(k).strip().lower(): k for k in body.keys()}
    has_signal = any(sig in lowered for sig in _LEAD_BODY_SIGNAL_KEYS)
    if not has_signal:
        return body

    data: Dict[str, Any] = {}
    mappable: List[Dict[str, Any]] = []
    for key, value in body.items():
        if not isinstance(key, str):
            continue
        name = key.strip()
        if not name:
            continue
        data[name] = value
        mappable.append({"name": name, "value": value})

    if not data:
        return body

    out = dict(body)
    out["data"] = data
    out["mappable_field_data"] = mappable
    return out


def _first_non_empty(*values: Any) -> str:
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _extract_page_id_from_dict(d: Dict[str, Any]) -> str:
    data = d.get("data") if isinstance(d.get("data"), dict) else {}
    return _first_non_empty(
        d.get("page_id"),
        d.get("pageId"),
        data.get("page_id"),
        data.get("pageId"),
    )


def _extract_lead_event(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    # Make (HTTP): raiz com "json" = lead, sem "body"
    if "body" not in item and isinstance(item.get("json"), dict):
        page_outer = _first_non_empty(item.get("page_id"), item.get("pageId"))
        synthetic: Dict[str, Any] = {"body": item["json"]}
        if page_outer:
            synthetic["page_id"] = page_outer
        return _extract_lead_event(synthetic)

    # Envelope no formato {"body": {...}, ...}
    if "body" in item:
        inner = _coerce_inner_body(item.get("body"))
        if not inner:
            return None
        inner = _inject_field_data_as_mappable(inner)
        inner = _inject_flat_payload_as_data(inner)
        page_id = _first_non_empty(
            item.get("page_id"),
            item.get("pageId"),
            _extract_page_id_from_dict(inner),
        )
        if _is_meta_lead_body(inner) or isinstance(inner.get("data"), dict):
            return {"body": inner, "page_id": page_id}
        return None

    # Lead direto
    item = _inject_field_data_as_mappable(item)
    item = _inject_flat_payload_as_data(item)
    if _is_meta_lead_body(item):
        return {"body": item, "page_id": _extract_page_id_from_dict(item)}
    return None


def normalize_lead_events(raw: Any) -> List[Dict[str, Any]]:
    """
    Extrai lista de eventos de lead: [{"body": <lead>, "page_id": "..."}].
    """
    if raw is None:
        return []

    raw = _unwrap_json_strings(raw)

    if isinstance(raw, dict):
        unwrapped = _unwrap_evolution_style_envelope(raw)
        if unwrapped is not None:
            nested = normalize_lead_events(unwrapped)
            if nested:
                return nested
        for wrap_key in ("items", "results", "records", "bundles"):
            inner = raw.get(wrap_key)
            if isinstance(inner, list):
                return normalize_lead_events(inner)
            if isinstance(inner, dict):
                out = normalize_lead_events(inner)
                if out:
                    return out

        one = _extract_lead_event(raw)
        return [one] if one else []

    if isinstance(raw, list):
        out: List[Dict[str, Any]] = []
        for item in raw:
            lead = _extract_lead_event(item)
            if lead:
                out.append(lead)
        return out

    return []


def parse_incoming_payload() -> Tuple[Optional[Any], str]:
    raw = request.get_json(silent=True, force=True)

    if raw is None:
        text: Optional[str] = None
        try:
            text = request.get_data(as_text=True)
        except Exception:
            text = None
        if (not text) and request.data:
            try:
                text = request.data.decode("utf-8-sig")
            except UnicodeDecodeError:
                text = None
        if text:
            text = text.lstrip("\ufeff").strip()
        if text:
            try:
                raw = json.loads(text)
            except json.JSONDecodeError:
                raw = None

    if raw is None and request.form:
        for key in ("body", "payload", "data", "json", "items"):
            val = request.form.get(key)
            if not val:
                continue
            try:
                raw = json.loads(val)
            except json.JSONDecodeError:
                raw = _unwrap_json_strings(val.strip())
            if raw is not None:
                break

    if raw is None:
        return None, "invalid_json"

    raw = _unwrap_json_strings(raw)
    if raw is None:
        return None, "invalid_json"
    if not isinstance(raw, (dict, list)):
        return None, "invalid_json"

    return raw, ""


def _mappable_from_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for k, v in data.items():
        rows.append({"name": str(k), "value": v})
    return rows


def _csv_to_list(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return [str(v).strip() for v in raw if str(v).strip()]
    if isinstance(raw, str):
        return [part.strip() for part in raw.split(",") if part.strip()]
    return []


def _ensure_mappable(body: Dict[str, Any], data: Dict[str, Any]) -> List[Dict[str, Any]]:
    mappable = body.get("mappable_field_data")
    if isinstance(mappable, list) and len(mappable) > 0:
        return mappable
    return _mappable_from_data(data)


def _load_clients() -> List[Dict[str, Any]]:
    try:
        from execution.persistence import db_enabled, ensure_db_ready, list_meta_clients

        if db_enabled():
            ensure_db_ready()
            rows = list_meta_clients()
            return [{k: v for k, v in r.items() if k != "id"} for r in rows]
    except Exception as e:
        logger.warning("Falha ao carregar clientes Meta do Postgres: %s", e)
    clients_path = clients_json_path()
    try:
        with open(clients_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [c for c in data if isinstance(c, dict)]
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Falha ao carregar clients.json para roteamento de leads: %s", e)
    return []


def _load_google_clients() -> List[Dict[str, Any]]:
    try:
        from execution.persistence import db_enabled, ensure_db_ready, list_google_clients

        if db_enabled():
            ensure_db_ready()
            rows = list_google_clients()
            return [{k: v for k, v in r.items() if k != "id"} for r in rows]
    except Exception as e:
        logger.warning("Falha ao carregar clientes Google do Postgres: %s", e)
    path = google_clients_json_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [c for c in data if isinstance(c, dict)]
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Falha ao carregar google_clients.json para roteamento de leads: %s", e)
    return []


def _payload_shape_hint_lead(raw: Any, max_keys: int = 18) -> str:
    """Resumo não sensível do JSON recebido (debug de formato Make/Meta)."""
    if raw is None:
        return "shape=tipo_null"
    if isinstance(raw, list):
        if not raw:
            return "shape=lista_vazia"
        first = raw[0]
        if isinstance(first, dict):
            keys = ",".join(sorted(str(k) for k in list(first.keys())[:max_keys]))
            more = "+" if len(first) > max_keys else ""
            return f"shape=lista[n={len(raw)}] primeiro_obj_keys=[{keys}{more}]"
        return f"shape=lista[n={len(raw)}] primeiro_tipo={type(first).__name__}"
    if isinstance(raw, dict):
        keys = ",".join(sorted(str(k) for k in list(raw.keys())[:max_keys]))
        more = "+" if len(raw) > max_keys else ""
        return f"shape=dict keys=[{keys}{more}]"
    return f"shape=tipo_{type(raw).__name__}"


def _configured_meta_pages_hint(max_pairs: int = 10) -> str:
    """Lista cliente:page_id configurados para roteamento (ajuda a corrigir IGNORADO_ROUTE)."""
    try:
        clients = _load_clients()
        parts: List[str] = []
        for c in clients:
            if c.get("enabled", True) is False:
                continue
            pid = str(c.get("meta_page_id", "")).strip()
            nm = str(c.get("client_name", "")).strip() or "Cliente"
            if pid:
                parts.append(f"{nm}:{pid}")
        if not parts:
            return "meta_pages_configuradas=(nenhuma com meta_page_id)"
        head = parts[:max_pairs]
        tail = f" ...(+{len(parts) - max_pairs})" if len(parts) > max_pairs else ""
        return "meta_pages_configuradas=" + "; ".join(head) + tail
    except Exception as exc:
        return f"meta_pages_configuradas=(erro_leitura:{exc!s})"


def _evolution_instance_label() -> str:
    inst = (os.getenv("EVOLUTION_INSTANCE") or "").strip()
    return inst if inst else "(sem EVOLUTION_INSTANCE)"


def _skipped_leads_summary(skipped: List[str], max_len: int = 260) -> str:
    if not skipped:
        return "(nenhum)"
    s = " | ".join(skipped)
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


def _resolve_lead_route(page_id: str) -> Optional[Dict[str, Any]]:
    if not page_id:
        return None
    clients = _load_clients()
    for c in clients:
        if c.get("enabled", True) is False:
            continue
        cid = str(c.get("meta_page_id", "")).strip()
        if cid != page_id:
            continue
        group_id = str(c.get("group_id") or "").strip()
        return {
            "client_name": str(c.get("client_name", "")).strip() or "Cliente",
            "group_id": group_id,
            "phone_number": str(c.get("lead_phone_number", "")).strip(),
            "template": str(c.get("lead_template", "")).strip() or "default",
            "exclude_exact": _csv_to_list(c.get("lead_exclude_fields")),
            "exclude_contains": _csv_to_list(c.get("lead_exclude_contains")),
            "exclude_regex": _csv_to_list(c.get("lead_exclude_regex")),
            "internal_notify_group_id": str(c.get("internal_notify_group_id", "")).strip(),
            "internal_lead_template": str(c.get("internal_lead_template", "")).strip(),
            "internal_weekly_template": str(c.get("internal_weekly_template", "")).strip(),
            "internal_notify_message": str(c.get("internal_notify_message", "")).strip(),
        }
    return None


def _route_from_meta_client(c: Dict[str, Any]) -> Dict[str, Any]:
    group_id = str(c.get("group_id") or "").strip()
    return {
        "client_name": str(c.get("client_name", "")).strip() or "Cliente",
        "group_id": group_id,
        "phone_number": str(c.get("lead_phone_number", "")).strip(),
        "template": str(c.get("lead_template", "")).strip() or "default",
        "exclude_exact": _csv_to_list(c.get("lead_exclude_fields")),
        "exclude_contains": _csv_to_list(c.get("lead_exclude_contains")),
        "exclude_regex": _csv_to_list(c.get("lead_exclude_regex")),
        "internal_notify_group_id": str(c.get("internal_notify_group_id", "")).strip(),
        "internal_lead_template": str(c.get("internal_lead_template", "")).strip(),
        "internal_weekly_template": str(c.get("internal_weekly_template", "")).strip(),
        "internal_notify_message": str(c.get("internal_notify_message", "")).strip(),
    }


def _route_from_google_client(c: Dict[str, Any]) -> Dict[str, Any]:
    tpl = str(c.get("google_template", "")).strip() or "default"
    return {
        "client_name": str(c.get("client_name", "")).strip() or "Cliente",
        "group_id": str(c.get("group_id") or "").strip(),
        "phone_number": str(c.get("lead_phone_number", "")).strip(),
        "template": tpl,
        "exclude_exact": [],
        "exclude_contains": [],
        "exclude_regex": [],
        "internal_notify_group_id": str(c.get("internal_notify_group_id", "")).strip(),
        "internal_lead_template": str(c.get("internal_lead_template", "")).strip(),
        "internal_weekly_template": str(c.get("internal_weekly_template", "")).strip(),
        "internal_notify_message": str(c.get("internal_notify_message", "")).strip(),
        "template_channel": "meta_lead",
        "route_origin": "google_customer_id",
        "google_customer_id": str(c.get("google_customer_id", "")).strip(),
    }


def _configured_google_customers_hint(max_pairs: int = 10) -> str:
    """Lista cliente:customer_id Google configurados (ajuda a corrigir rota)."""
    try:
        clients = _load_google_clients()
        parts: List[str] = []
        for c in clients:
            if c.get("enabled", True) is False:
                continue
            cid = str(c.get("google_customer_id", "")).strip()
            nm = str(c.get("client_name", "")).strip() or "Cliente"
            if cid:
                parts.append(f"{nm}:{cid}")
        if not parts:
            return "google_customers_configurados=(nenhum com google_customer_id)"
        head = parts[:max_pairs]
        tail = f" ...(+{len(parts) - max_pairs})" if len(parts) > max_pairs else ""
        return "google_customers_configurados=" + "; ".join(head) + tail
    except Exception as exc:
        return f"google_customers_configurados=(erro_leitura:{exc!s})"


def _resolve_google_lead_route(customer_id_raw: str) -> Optional[Dict[str, Any]]:
    want = _normalize_google_customer_id_digits(customer_id_raw)
    if not want:
        return None
    for c in _load_google_clients():
        if c.get("enabled", True) is False:
            continue
        cid = _normalize_google_customer_id_digits(str(c.get("google_customer_id", "")))
        if cid and cid == want:
            return _route_from_google_client(c)
    return None


def _extract_codi_id_from_body(body: Dict[str, Any]) -> str:
    if not isinstance(body, dict):
        return ""
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)
    for key in _CODI_ID_PAYLOAD_KEYS:
        raw = _pick_ci(data, key)
        if raw not in (None, ""):
            txt = _format_field_value(raw).strip()
            if txt:
                return txt
    for key in _CODI_ID_PAYLOAD_KEYS:
        raw = _pick_ci(body, key)
        if raw not in (None, ""):
            txt = _format_field_value(raw).strip()
            if txt:
                return txt
    for key in _CODI_ID_PAYLOAD_KEYS:
        txt = _mappable_lookup(mappable, key).strip()
        if txt:
            return txt
    return ""


def _extract_native_form_id_from_body(body: Dict[str, Any]) -> str:
    """
    Chave de formulário nativa (Meta/Google). Não é usada para roteamento de site.
    Mantida para compatibilidade e futuras implementações.
    """
    if not isinstance(body, dict):
        return ""
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)
    for key in _NATIVE_FORM_ID_KEYS:
        raw = _pick_ci(data, key)
        if raw not in (None, ""):
            txt = _format_field_value(raw).strip()
            if txt:
                return txt
    for key in _NATIVE_FORM_ID_KEYS:
        raw = _pick_ci(body, key)
        if raw not in (None, ""):
            txt = _format_field_value(raw).strip()
            if txt:
                return txt
    for key in _NATIVE_FORM_ID_KEYS:
        txt = _mappable_lookup(mappable, key).strip()
        if txt:
            return txt
    return ""


def _normalize_google_customer_id_digits(raw: str) -> str:
    """Comparação de customer id Google Ads: só dígitos (aceita 123-456-7890 ou act_123)."""
    s = str(raw or "").strip()
    if s.lower().startswith("act_"):
        s = s[4:]
    return "".join(ch for ch in s if ch.isdigit())


def _extract_google_customer_id_from_body(body: Dict[str, Any]) -> str:
    if not isinstance(body, dict):
        return ""
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)
    for key in _GOOGLE_CUSTOMER_ID_KEYS:
        raw = _pick_ci(data, key)
        if raw not in (None, ""):
            txt = _format_field_value(raw).strip()
            if txt:
                return txt
    for key in _GOOGLE_CUSTOMER_ID_KEYS:
        raw = _pick_ci(body, key)
        if raw not in (None, ""):
            txt = _format_field_value(raw).strip()
            if txt:
                return txt
    for key in _GOOGLE_CUSTOMER_ID_KEYS:
        txt = _mappable_lookup(mappable, key).strip()
        if txt:
            return txt
    return ""


def _fold_ascii_lower(s: str) -> str:
    s = (s or "").lower()
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def _haystack_has_token(hay: str, tokens: Tuple[str, ...]) -> bool:
    h = _fold_ascii_lower(hay)
    return any(t in h for t in tokens)


def _strong_google_url(url: str) -> bool:
    u = (url or "").lower()
    if any(p in u for p in ("gclid=", "gbraid=", "wbraid=")):
        return True
    if "google." in u or "googleadservices" in u or "doubleclick" in u or "g.co/" in u:
        return True
    if "youtube.com" in u or "youtu.be" in u:
        return True
    return False


def _first_url_from_lead_body(
    body: Dict[str, Any], data: Dict[str, Any], mappable: List[Dict[str, Any]]
) -> str:
    for key in _URL_HINT_KEYS:
        if isinstance(data, dict):
            v = _format_field_value(data.get(key))
            if v:
                return v.strip()
        if isinstance(body, dict):
            v2 = _format_field_value(body.get(key))
            if v2:
                return v2.strip()
        m = _mappable_lookup(mappable, key)
        if m:
            return m.strip()
    return ""


def _infer_traffic_source_and_url(
    body: Dict[str, Any],
    data: Dict[str, Any],
    mappable: List[Dict[str, Any]],
    page_path: str,
    utm_source: str,
    utm_medium: str,
    utm_campaign: str,
    utm_term: str,
    utm_content: str,
) -> Tuple[str, str]:
    origin_url = _first_url_from_lead_body(body, data, mappable) or (page_path or "")
    for key in _TRAFFIC_EXPLICIT_KEYS:
        raw = _first_field_from_data_and_mappable((key,), data, mappable)
        if not raw and isinstance(body, dict):
            raw = _format_field_value(body.get(key))
        if not raw:
            continue
        t = _fold_ascii_lower(str(raw).strip())
        if t in ("google", "g", "gads", "adwords", "pmax", "search", "yt", "youtube"):
            return "google", origin_url
        if t in ("meta", "m", "fb", "ig", "face", "insta", "facebook", "instagram"):
            return "meta", origin_url
    utm_blob = " ".join(
        [utm_source, utm_medium, utm_campaign, utm_term, utm_content, origin_url]
    )
    if _strong_google_url(origin_url) or _haystack_has_token(utm_blob, _GOOGLE_TOKENS):
        return "google", origin_url
    if _haystack_has_token(utm_blob, _META_TOKENS):
        return "meta", origin_url
    if _haystack_has_token(origin_url, _GOOGLE_TOKENS):
        return "google", origin_url
    if _haystack_has_token(origin_url, _META_TOKENS):
        return "meta", origin_url
    return "unknown", origin_url


def _build_route_from_site_lead_target(target: Dict[str, Any], codi_id: str) -> Dict[str, Any]:
    """Rota de envio a partir do cadastro site_lead_routes (somente codi_id + campos da rota)."""
    oa = str(target.get("origem_anuncio", "")).strip()
    co = str(target.get("cliente_origem", "")).strip()
    legacy = str(target.get("target_client_name", "")).strip()
    display = co or oa or legacy or "Cliente"
    route_group_id = str(target.get("group_id", "")).strip()
    return {
        "client_name": display,
        "group_id": route_group_id,
        "phone_number": str(target.get("lead_phone_number", "")).strip(),
        "template": str(target.get("lead_template", "")).strip() or "default",
        "exclude_exact": _csv_to_list(target.get("lead_exclude_fields")),
        "exclude_contains": _csv_to_list(target.get("lead_exclude_contains")),
        "exclude_regex": _csv_to_list(target.get("lead_exclude_regex")),
        "internal_notify_group_id": str(target.get("internal_notify_group_id", "")).strip(),
        "internal_lead_template": str(target.get("internal_lead_template", "")).strip(),
        "internal_weekly_template": "",
        "internal_notify_message": "",
        "template_channel": "site_lead",
        "route_origin": "site_codi_id",
        "site_codi_id": codi_id,
        "origem_anuncio": oa,
        "cliente_origem": co,
        "route_target_type": str(target.get("target_type", "site") or "site").strip() or "site",
    }


def _resolve_site_lead_route(codi_id: str) -> Optional[Dict[str, Any]]:
    cid = (codi_id or "").strip()
    if not cid:
        return None
    try:
        from execution.persistence import db_enabled, ensure_db_ready, list_site_lead_routes

        if db_enabled():
            ensure_db_ready()
        routes = list_site_lead_routes()
    except Exception as e:
        logger.warning("Falha ao carregar site_lead_routes: %s", e)
        routes = []
    target = next(
        (
            r
            for r in routes
            if bool(r.get("enabled", True))
            and str(r.get("codi_id", r.get("form_id", ""))).strip().lower() == cid.lower()
        ),
        None,
    )
    if not target:
        return None
    return _build_route_from_site_lead_target(target, cid)


def _resolve_route_with_context(page_id: str, codi_id: str) -> Tuple[Optional[Dict[str, Any]], str, str, str]:
    """
    Organização explícita de contexto de roteamento:
    - Leads de site: codi_id tem prioridade sobre page_id (Make costuma enviar ambos).
    - native_ads: usa page_id (Meta/Google nativo) quando não há rota por codi_id.
    """
    if codi_id:
        if not is_valid_site_codi_id(codi_id):
            return (
                None,
                "site",
                "CODI_ID_INVALID_FORMAT",
                f"codi_id_recebido={codi_id} (esperado: 28–36 dígitos numéricos)",
            )
        route = _resolve_site_lead_route(codi_id)
        if route:
            return route, "site", "", ""
        return None, "site", "CODI_ID_ROUTE_NOT_FOUND", f"codi_id_recebido={codi_id}"
    if page_id:
        return _resolve_lead_route(page_id), "native_ads", "PAGE_ID_SEM_CLIENTE_NA_PULSEBOARD", _configured_meta_pages_hint()
    return None, "unknown", "ROUTING_KEY_MISSING", "Envie page_id (Meta/Ads) ou codi_id (Lead Site)"


def _resolve_route_with_mode(
    route_mode: str,
    page_id: str,
    codi_id: str,
    *,
    google_customer_id: str = "",
) -> Tuple[Optional[Dict[str, Any]], str, str, str]:
    """
    route_mode:
      - meta_only: só page_id (Meta); ignora codi_id
      - google_only: só google_customer_id (conta Google Ads)
      - site_only: só codi_id (lead site)
      - auto: site por codi_id, senão Meta por page_id (compat. com payloads antigos mistos)
    """
    mode = str(route_mode or "meta_only").strip().lower()
    if mode == "site_only":
        cid = (codi_id or "").strip()
        if not cid:
            return None, "site", "CODI_ID_OBRIGATORIO", "Envie codi_id (28–36 dígitos) para roteamento de lead site"
        if not is_valid_site_codi_id(cid):
            return (
                None,
                "site",
                "CODI_ID_INVALID_FORMAT",
                f"codi_id_recebido={cid} (esperado: 28–36 dígitos numéricos)",
            )
        route = _resolve_site_lead_route(cid)
        if route:
            return route, "site", "", ""
        return None, "site", "CODI_ID_ROUTE_NOT_FOUND", f"codi_id_recebido={cid}"
    if mode == "google_only":
        g = _normalize_google_customer_id_digits(google_customer_id)
        if not g:
            return (
                None,
                "google",
                "GOOGLE_CUSTOMER_ID_OBRIGATORIO",
                "Envie google_customer_id (ID da conta Google Ads, só dígitos ou formato 123-456-7890)",
            )
        route = _resolve_google_lead_route(google_customer_id)
        if route:
            return route, "google", "", ""
        return (
            None,
            "google",
            "GOOGLE_CUSTOMER_ID_ROUTE_NOT_FOUND",
            f"google_customer_id_recebido={google_customer_id!r} | {_configured_google_customers_hint()}",
        )
    if mode == "meta_only":
        pid = (page_id or "").strip()
        if not pid:
            return (
                None,
                "native_ads",
                "PAGE_ID_OBRIGATORIO",
                "Este endpoint aceita apenas page_id (Meta). Leads de site: POST /site-new-lead",
            )
        route = _resolve_lead_route(pid)
        if route:
            return route, "native_ads", "", ""
        return None, "native_ads", "PAGE_ID_SEM_CLIENTE_NA_PULSEBOARD", _configured_meta_pages_hint()
    return _resolve_route_with_context(page_id, codi_id)


def _human_route_error_detail(route_error_code: str) -> str:
    return {
        "PAGE_ID_SEM_CLIENTE_NA_PULSEBOARD": "Lead ignorado por page_id não mapeado na Pulseboard",
        "PAGE_ID_OBRIGATORIO": "Lead Meta sem page_id (este endpoint exige apenas page_id)",
        "CODI_ID_INVALID_FORMAT": "Lead de site ignorado por codi_id fora do padrão",
        "CODI_ID_ROUTE_NOT_FOUND": "Lead de site ignorado por codi_id sem rota cadastrada",
        "CODI_ID_OBRIGATORIO": "Lead de site sem codi_id (obrigatório neste endpoint)",
        "GOOGLE_CUSTOMER_ID_OBRIGATORIO": "Lead Google sem google_customer_id no payload",
        "GOOGLE_CUSTOMER_ID_ROUTE_NOT_FOUND": "Lead ignorado: google_customer_id sem cliente em google_clients",
        "ROUTING_KEY_MISSING": "Lead ignorado sem page_id e sem codi_id (payload misto: use o endpoint dedicado)",
    }.get(route_error_code, f"Lead ignorado (código: {route_error_code})")


def _lead_webhook_expected_secret(endpoint_label: str) -> str:
    """
    Segredo HTTP por endpoint (Meta / Google / Site).
    /site-new-lead: SITE_LEAD_WEBHOOK_SECRET; se vazio, usa META_LEAD_WEBHOOK_SECRET (mesmo segredo dos landings que já usam Meta).
    Vazio = esse POST não exige segredo.
    """
    if endpoint_label == "/site-new-lead":
        site = (os.getenv("SITE_LEAD_WEBHOOK_SECRET") or "").strip()
        if site:
            return site
        return (os.getenv("META_LEAD_WEBHOOK_SECRET") or "").strip()
    if endpoint_label == "/google-new-lead":
        return (os.getenv("GOOGLE_LEAD_WEBHOOK_SECRET") or "").strip()
    if endpoint_label == "/meta-new-lead":
        return (os.getenv("META_LEAD_WEBHOOK_SECRET") or "").strip()
    return ""


def _lead_webhook_secret_from_query() -> str:
    """
    Segredo na query string (útil quando a plataforma não envia cabeçalhos), alinhado à Evolution:
    ?secret=... ou ?webhook_secret=... (primeiro não vazio ganha).
    Atenção: URLs com segredo aparecem em logs de proxy e histórico do browser.
    """
    q = request.args or {}
    return (
        (q.get("webhook_secret") or "").strip()
        or (q.get("secret") or "").strip()
    )


def _check_webhook_secret(endpoint_label: str) -> Optional[Tuple[Any, int]]:
    secret = _lead_webhook_expected_secret(endpoint_label)
    if not secret:
        return None
    hdr = (request.headers.get("X-Webhook-Secret") or "").strip()
    auth = request.headers.get("Authorization") or ""
    bearer = ""
    if auth.lower().startswith("bearer "):
        bearer = auth[7:].strip()
    qs = _lead_webhook_secret_from_query()
    if (
        constant_time_str_equal(hdr, secret)
        or constant_time_str_equal(bearer, secret)
        or constant_time_str_equal(qs, secret)
    ):
        return None
    return jsonify({"ok": False, "error": "unauthorized"}), 401


def _inject_custom_variables_into_ctx(
    ctx: Dict[str, str],
    body: Dict[str, Any],
    route: Optional[Dict[str, Any]],
) -> None:
    """Preenche variáveis personalizadas (message_templates.custom_variables) no contexto de render."""
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)
    fc = str((route or {}).get("template_channel") or "meta_lead").strip() or "meta_lead"

    def _resolve_payload(keys: Tuple[str, ...]) -> str:
        return _first_field_from_data_and_mappable(keys, data, mappable) or ""

    apply_custom_variables(fc, ctx, resolve_payload=_resolve_payload)


def _base_message_fields(body: Dict[str, Any], route: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)
    fc = str((route or {}).get("template_channel") or "meta_lead").strip() or "meta_lead"
    filter_channel = fc if fc in ("meta_lead", "site_lead") else "meta_lead"
    global_rules = get_filter_rules(filter_channel)
    client_rules = {
        "exclude_exact": [str(v).strip().lower() for v in ((route or {}).get("exclude_exact") or []) if str(v).strip()],
        "exclude_contains": [str(v).strip().lower() for v in ((route or {}).get("exclude_contains") or []) if str(v).strip()],
        "exclude_regex": [str(v).strip() for v in ((route or {}).get("exclude_regex") or []) if str(v).strip()],
    }

    nome = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "nome")), data, mappable)
    email = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "email")), data, mappable)
    if not email:
        email = _format_field_value(data.get("email")) or _mappable_lookup(mappable, "email")
    telefone_raw = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "whatsapp")), data, mappable)
    telefone_raw_d = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "telefone_digitos")), data, mappable)
    if not telefone_raw_d:
        telefone_raw_d = telefone_raw
    wa_link = _format_whatsapp_line(telefone_raw)
    telefone_digitos = _digits_only(telefone_raw_d)
    page_path = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "page_path")), data, mappable)
    utm_source = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "utm_source")), data, mappable)
    utm_medium = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "utm_medium")), data, mappable)
    utm_campaign = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "utm_campaign")), data, mappable)
    utm_term = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "utm_term")), data, mappable)
    utm_content = _first_field_from_data_and_mappable(tuple(get_effective_source_keys(fc, "utm_content")), data, mappable)

    respostas_bundle = _build_respostas_bundle(
        mappable,
        global_rules=global_rules,
        client_rules=client_rules,
    )

    received_ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    traffic_source, traffic_origin_url = _infer_traffic_source_and_url(
        body,
        data,
        mappable,
        page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_term,
        utm_content,
    )
    site_routed = str((route or {}).get("route_origin", "")).strip() == "site_codi_id"
    google_routed = str((route or {}).get("route_origin", "")).strip() == "google_customer_id"
    out: Dict[str, str] = {
        "nome": nome or "(nao informado)",
        "email": email or "(nao informado)",
        "whatsapp": wa_link,
        "telefone_digitos": telefone_digitos or "(nao informado)",
        "form_name": (
            (
                _first_field_from_data_and_mappable(
                    tuple(get_effective_source_keys(fc, "form_name")), data, mappable
                )
                or ""
            ).strip()
            or _extract_form_name(body, allow_meta_graph=not (site_routed or google_routed))
        ),
        "page_path": page_path,
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": utm_campaign,
        "utm_term": utm_term,
        "utm_content": utm_content,
        "traffic_source": traffic_source,
        "traffic_origin_url": traffic_origin_url,
        "respostas": respostas_bundle["filtered_text"],
        "respostas_filtradas": respostas_bundle["filtered_text"],
        "respostas_raw": respostas_bundle["raw_text"],
        "respostas_omitidas": respostas_bundle["omitted_text"],
        "respostas_count": str(respostas_bundle["filtered_count"]),
        "respostas_raw_count": str(respostas_bundle["raw_count"]),
        "respostas_omitidas_count": str(respostas_bundle["omitted_count"]),
        "received_at": received_ts,
        "chegada_em": received_ts,
    }
    merged_vr = load_merged_variable_resolution()
    vr_b = merged_vr.get(resolution_channel_for_lead(fc), {}) or {}
    for slot, _m in vr_b.items():
        if slot in LEAD_RESOLVABLE_SLOTS:
            continue
        tkeys = get_effective_source_keys(fc, str(slot))
        if not tkeys:
            continue
        out[str(slot)] = (_first_field_from_data_and_mappable(tkeys, data, mappable) or "")
    return out


def _form_name_from_nested_objects(body: Dict[str, Any], data: Dict[str, Any]) -> str:
    """Objetos aninhados comuns no Make: `form`, `lead.form`, etc."""
    for container in (body, data):
        if not isinstance(container, dict):
            continue
        form_obj = container.get("form")
        if isinstance(form_obj, dict):
            n = _first_non_empty(form_obj.get("name"), form_obj.get("title"), form_obj.get("label"))
            if n:
                return n
        lead = container.get("lead")
        if isinstance(lead, dict):
            lf = lead.get("form")
            if isinstance(lf, dict):
                n = _first_non_empty(lf.get("name"), lf.get("title"), lf.get("label"))
                if n:
                    return n
    return ""


def _form_name_from_flat_and_mappable(
    data: Dict[str, Any],
    body: Dict[str, Any],
    mappable: List[Dict[str, Any]],
) -> str:
    for key in _FORM_NAME_PAYLOAD_KEYS:
        v = _first_non_empty(
            data.get(key),
            body.get(key),
            _mappable_lookup(mappable, key),
        )
        if v:
            return v
    return ""


# Nomes de linha em mappable_field_data que costumam ser metadado do formulário (não resposta do usuário).
_FORM_NAME_MAPPABLE_ROW_ALIASES_CF = frozenset(
    {
        "form_name",
        "formname",
        "nome_formulario",
        "nome_form",
        "nome_do_formulario",
        "nome_do_formulário",
        "form_title",
        "formtitle",
        "form_label",
        "formlabel",
        "titulo_formulario",
        "titulo_formulário",
        "leadgen_form_name",
        "leadgenformname",
        "nome do formulario",
        "nome do formulário",
        "nome do formulario no meta",
        "nome do formulário no meta",
    }
)


def _mappable_form_name_by_row_alias(mappable: List[Dict[str, Any]]) -> str:
    for row in mappable:
        if not isinstance(row, dict):
            continue
        name_raw = str(row.get("name", "")).strip()
        if not name_raw:
            continue
        name_cf = _normalize_field_name(name_raw)
        if name_cf in _FORM_NAME_MAPPABLE_ROW_ALIASES_CF:
            val = _format_field_value(row.get("value"))
            if val:
                return val
    return ""


def _extract_form_name(body: Dict[str, Any], *, allow_meta_graph: bool = True) -> str:
    """
    Nome amigável do formulário para templates ({{form_name}}).

    Leads roteados por `site_codi_id` não devem consultar a Graph: `form_id` no JSON do site
    costuma ser texto (ex.: nome do cliente), não um object id numérico do Meta.
    """
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)

    name_from_nested = _form_name_from_nested_objects(body, data)
    if name_from_nested:
        return name_from_nested

    name_from_payload = _form_name_from_flat_and_mappable(data, body, mappable)
    if name_from_payload:
        return name_from_payload

    name_from_mappable_alias = _mappable_form_name_by_row_alias(mappable)
    if name_from_mappable_alias:
        return name_from_mappable_alias

    if not allow_meta_graph:
        cid = _extract_codi_id_from_body(body).strip()
        if cid:
            return cid
        return "(nao informado)"

    # Fallback robusto: resolve nome do formulário na Graph API via form_id/leadgenId.
    nested_form_id = ""
    nested_leadgen_id = ""
    for container in (body, data):
        if not isinstance(container, dict):
            continue
        lead = container.get("lead")
        if isinstance(lead, dict):
            nested_form_id = nested_form_id or _first_non_empty(
                lead.get("form_id"),
                lead.get("formId"),
            )
            nested_leadgen_id = nested_leadgen_id or _first_non_empty(
                lead.get("leadgen_id"),
                lead.get("leadgenId"),
                lead.get("id"),
            )

    form_id = _first_non_empty(
        body.get("form_id"),
        body.get("formId"),
        body.get("leadgen_form_id"),
        body.get("leadgenFormId"),
        data.get("form_id"),
        data.get("formId"),
        data.get("leadgen_form_id"),
        data.get("leadgenFormId"),
        nested_form_id,
        _mappable_lookup(mappable, "form_id"),
        _mappable_lookup(mappable, "formId"),
        _mappable_lookup(mappable, "leadgen_form_id"),
        _mappable_lookup(mappable, "leadgenFormId"),
    )
    if form_id:
        if form_id in _FORM_NAME_CACHE:
            return _FORM_NAME_CACHE[form_id]
        form_obj = _graph_get_object_fields(form_id, "name")
        form_name = _first_non_empty(form_obj.get("name"))
        if form_name:
            _FORM_NAME_CACHE[form_id] = form_name
            return form_name

    leadgen_id = _first_non_empty(
        body.get("leadgenId"),
        body.get("leadgen_id"),
        body.get("leadgenID"),
        data.get("leadgenId"),
        data.get("leadgen_id"),
        data.get("leadgenID"),
        nested_leadgen_id,
        _mappable_lookup(mappable, "leadgenId"),
        _mappable_lookup(mappable, "leadgen_id"),
    )
    if leadgen_id:
        lead_obj = _graph_get_object_fields(leadgen_id, "form_id")
        lead_form_id = _first_non_empty(lead_obj.get("form_id"))
        if lead_form_id:
            if lead_form_id in _FORM_NAME_CACHE:
                return _FORM_NAME_CACHE[lead_form_id]
            form_obj = _graph_get_object_fields(lead_form_id, "name")
            form_name = _first_non_empty(form_obj.get("name"))
            if form_name:
                _FORM_NAME_CACHE[lead_form_id] = form_name
                return form_name

    logger.warning(
        "Nao foi possivel resolver nome do formulario. form_id=%s leadgen_id=%s",
        form_id or "(vazio)",
        leadgen_id or "(vazio)",
    )
    # Fallback final: quando não há nome amigável, expõe o form_id para uso em {{form_name}}.
    fallback_form_id = _first_non_empty(
        form_id,
        body.get("form_id"),
        body.get("formId"),
        data.get("form_id"),
        data.get("formId"),
        _extract_native_form_id_from_body(body),
    )
    if fallback_form_id:
        return str(fallback_form_id).strip()
    return "(nao informado)"


def _truncate_message(msg: str) -> str:
    if len(msg) <= _WHATSAPP_MSG_MAX:
        return msg
    cut = _WHATSAPP_MSG_MAX - 20
    logger.warning("Mensagem de lead truncada para %s caracteres", _WHATSAPP_MSG_MAX)
    return msg[:cut] + "\n...(truncado)"


def _format_default_lead_message(body: Dict[str, Any], client_name: str, route: Optional[Dict[str, Any]] = None) -> str:
    base = _base_message_fields(body, route=route)
    msg = (
        f"Novo lead - {client_name}\n"
        f"Recebido em: {base['chegada_em']}\n"
        f"Nome do Lead: {base['nome']}\n"
        f"WhatsApp do Lead: {base['whatsapp']}\n"
        f"E-mail do Lead: {base['email']}\n"
        f"\n"
        f"==========\n"
        f"\n"
        f"Respostas do Lead:\n"
        f"{base['respostas']}"
    )
    return _truncate_message(msg)


def _format_pratical_life_lead_message(
    body: Dict[str, Any],
    client_name: str,
    route: Optional[Dict[str, Any]] = None,
) -> str:
    base = _base_message_fields(body, route=route)
    msg = (
        f"Novo lead recebido - {client_name}\n"
        f"- Recebido em: {base['chegada_em']}\n"
        f"Contato:\n"
        f"- Nome: {base['nome']}\n"
        f"- WhatsApp: {base['whatsapp']}\n"
        f"- E-mail: {base['email']}\n"
        f"- Nome do formulario: {base['form_name']}\n"
        f"\n"
        f"Formulario:\n"
        f"{base['respostas']}"
    )
    return _truncate_message(msg)


TEMPLATE_FORMATTERS: Dict[str, Callable[[Dict[str, Any], str, Optional[Dict[str, Any]]], str]] = {
    "default": _format_default_lead_message,
    "pratical_life": _format_pratical_life_lead_message,
}


def _format_lead_message(
    body: Dict[str, Any],
    template_id: str,
    client_name: str,
    route: Optional[Dict[str, Any]] = None,
    page_id: str = "",
) -> str:
    template_channel = str((route or {}).get("template_channel") or "meta_lead").strip() or "meta_lead"
    custom_content = get_template_content(template_channel, template_id)
    if custom_content:
        base = _base_message_fields(body, route=route)
        render_ctx: Dict[str, str] = {
            "client_name": client_name,
            "page_id": page_id,
            "template_id": template_id,
            "nome": base["nome"],
            "email": base["email"],
            "whatsapp": base["whatsapp"],
            "telefone_digitos": base["telefone_digitos"],
            "form_name": base["form_name"],
            "page_path": base["page_path"],
            "utm_source": base["utm_source"],
            "utm_medium": base["utm_medium"],
            "utm_campaign": base["utm_campaign"],
            "utm_term": base["utm_term"],
            "utm_content": base["utm_content"],
            "traffic_source": base.get("traffic_source", "unknown"),
            "traffic_origin_url": base.get("traffic_origin_url", ""),
            "origem_anuncio": str((route or {}).get("origem_anuncio", "")).strip(),
            "cliente_origem": str((route or {}).get("cliente_origem", "")).strip(),
            "respostas": base["respostas"],
            "respostas_filtradas": base["respostas_filtradas"],
            "respostas_raw": base["respostas_raw"],
            "respostas_omitidas": base["respostas_omitidas"],
            "respostas_count": base["respostas_count"],
            "respostas_raw_count": base["respostas_raw_count"],
            "respostas_omitidas_count": base["respostas_omitidas_count"],
            "received_at": base["received_at"],
            "chegada_em": base["chegada_em"],
        }
        _inject_custom_variables_into_ctx(render_ctx, body, route)
        rendered = render_template_text(
            custom_content,
            render_ctx,
        )
        return _truncate_message(rendered)
    formatter = TEMPLATE_FORMATTERS.get(template_id, TEMPLATE_FORMATTERS["default"])
    return formatter(body, client_name, route)


def _handle_meta_new_lead(
    endpoint_label: str,
    route_mode: str = "meta_only",
    channel_label: str = "leads_meta",
) -> Tuple[Any, int]:
    denied = _check_webhook_secret(endpoint_label)
    if denied:
        _wh_log(
            f"POST {endpoint_label} | ERRO_AUTH | canal={channel_label} | cod=WEBHOOK_SECRET_NEGADO | "
            f"ip={_client_ip()} | content_length={request.content_length}",
            level=logging.WARNING,
        )
        _emit_runtime_event(
            stage="NEGADO_AUTH",
            status="warning",
            detail=f"{endpoint_label} bloqueado por segredo do webhook",
            payload={"endpoint": endpoint_label, "ip": _client_ip()},
        )
        return denied

    _wh_log(
        f"POST {endpoint_label} | RECEBIDO | canal={channel_label} | "
        f"ip={_client_ip()} | content_type={request.content_type!r} | content_length={request.content_length}"
    )
    _emit_runtime_event(
        stage="RECEBIDO",
        status="info",
        detail=f"{endpoint_label} recebeu requisição",
        payload={
            "endpoint": endpoint_label,
            "ip": _client_ip(),
            "content_type": request.content_type or "",
            "content_length": request.content_length or 0,
        },
    )

    raw, parse_err = parse_incoming_payload()
    if raw is None:
        _wh_log(
            f"POST {endpoint_label} | ERRO_JSON | canal={channel_label} | cod=PAYLOAD_NAO_E_JSON | "
            f"ip={_client_ip()} | motivo={parse_err}",
            level=logging.WARNING,
        )
        _emit_runtime_event(
            stage="ERRO_JSON",
            status="error",
            detail=f"payload inválido em {endpoint_label}",
            payload={"endpoint": endpoint_label, "parse_err": parse_err},
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": parse_err,
                    "hint": "Envie JSON (array ou objeto) com lead em body ou lead direto.",
                }
            ),
            400,
        )

    events = normalize_lead_events(raw)
    if not events:
        if raw in ({}, []):
            return jsonify({"ok": True, "ignored": True, "reason": "empty_payload"}), 200
        if isinstance(raw, dict):
            uw = _unwrap_evolution_style_envelope(raw)
            if uw is not None:
                if _looks_like_evolution_whatsapp_event(uw):
                    _wh_log(
                        f"POST {endpoint_label} | IGNORADO | canal={channel_label} | cod=EVOLUTION_EVENTO_NAO_E_LEAD | "
                        f"envelope Evolution com evento WhatsApp/grupo em data (nao e lead Meta). "
                        f"Catalogo Evolution: POST /evolution-webhook",
                        level=logging.INFO,
                    )
                    _emit_runtime_event(
                        stage="IGNORADO",
                        status="info",
                        detail=f"{endpoint_label}: evento Evolution/WhatsApp, nao lead",
                        payload={
                            "endpoint": endpoint_label,
                            "cod": "EVOLUTION_EVENTO_NAO_E_LEAD",
                        },
                    )
                    return (
                        jsonify(
                            {
                                "ok": True,
                                "ignored": True,
                                "reason": "evolution_whatsapp_event",
                                "hint": "Este URL e para leads (Meta/Make). Eventos de mensagem/grupo da Evolution "
                                "devem ir para POST /evolution-webhook.",
                            }
                        ),
                        200,
                    )
                u_sample = (
                    list(uw.keys())[:24]
                    if isinstance(uw, dict)
                    else type(uw).__name__
                )
                _wh_log(
                    f"POST {endpoint_label} | AVISO | canal={channel_label} | cod=ENVELOPE_SEM_LEAD_RECONHECIDO | "
                    f"inner_tipo={type(uw).__name__} inner_keys_sample={u_sample!r}",
                    level=logging.WARNING,
                )
        _wh_log(
            f"POST {endpoint_label} | ERRO_LEAD | canal={channel_label} | cod=PAYLOAD_SEM_LEAD_RECONHECIDO | "
            f"{_payload_shape_hint_lead(raw)} | "
            f"dica=body.data/mappable_field_data ou field_data (Graph) ou envelope Make com json",
            level=logging.WARNING,
        )
        _emit_runtime_event(
            stage="ERRO_PAYLOAD",
            status="error",
            detail=f"nenhum lead extraído em {endpoint_label}",
            payload={
                "endpoint": endpoint_label,
                "cod": "PAYLOAD_SEM_LEAD_RECONHECIDO",
                "shape": _payload_shape_hint_lead(raw),
            },
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "missing_body",
                    "hint": "Esperado: body com data/mappable_field_data ou lead direto.",
                }
            ),
            400,
        )

    _wh_log(f"POST {endpoint_label} | PAYLOAD_OK | canal={channel_label} | cod=LEAD_NORMALIZADO | leads={len(events)}")
    _emit_runtime_event(
        stage="PAYLOAD_OK",
        status="ok",
        detail=f"{len(events)} lead(s) normalizado(s)",
        payload={"endpoint": endpoint_label, "leads": len(events)},
    )

    dry = os.getenv("DRY_RUN", "false").lower() == "true"
    sent = 0
    errors: List[str] = []
    skipped: List[str] = []

    for idx, event in enumerate(events):
        body = event["body"]
        page_id = str(event.get("page_id", "")).strip()
        codi_id = _extract_codi_id_from_body(body)
        google_cid = _extract_google_customer_id_from_body(body)
        native_form_id = _extract_native_form_id_from_body(body)
        # Guard rail (payloads mistos com codi_id + form_id Meta): só em modo auto.
        if route_mode == "auto":
            if (
                codi_id
                and native_form_id
                and codi_id == native_form_id
                and not is_valid_site_codi_id(codi_id)
            ):
                _wh_log(
                    f"LEAD_{idx} | AVISO_ROTA | canal={channel_label} | cod=IGNORA_CODI_ID_EQ_FORM_ID_META | "
                    f"form_id={native_form_id} | page_id={page_id or 'vazio'} | acao=usar_page_id"
                )
                codi_id = ""
        route, route_context, route_error_code, route_error_hint = _resolve_route_with_mode(
            route_mode, page_id, codi_id, google_customer_id=google_cid
        )
        route_error_detail = _human_route_error_detail(route_error_code)
        if route and route_context == "site":
            _wh_log(
                f"LEAD_{idx} | ROTA_CODI_ID_OK | canal={channel_label} | cod=CODI_ID_ROUTE_MATCH | "
                f"codi_id={codi_id} | cliente={route.get('client_name','')}"
            )
        if route and route_context == "google":
            _wh_log(
                f"LEAD_{idx} | ROTA_GOOGLE_OK | canal={channel_label} | cod=GOOGLE_CUSTOMER_ROUTE_MATCH | "
                f"google_customer_id={google_cid} | cliente={route.get('client_name','')}"
            )
        if not route:
            skipped.append(
                f"lead_index_{idx}: rota_nao_mapeada (page_id={page_id or 'vazio'}; codi_id={codi_id or 'vazio'}; "
                f"google_customer_id={google_cid or 'vazio'})"
            )
            _wh_log(
                f"LEAD_{idx} | ERRO_ROTA_CLIENTE | canal={channel_label} | cod={route_error_code} | "
                f"page_id_recebido={page_id or 'vazio'} | codi_id_recebido={codi_id or 'vazio'} | "
                f"google_customer_id_recebido={google_cid or 'vazio'} | "
                f"native_form_id={native_form_id or 'vazio'} | contexto={route_context} | "
                f"cliente_mapeado=(nenhum) | {route_error_hint}",
                level=logging.WARNING,
            )
            _emit_runtime_event(
                stage="IGNORADO_ROUTE",
                status="warning",
                detail=route_error_detail,
                page_id=page_id,
                payload={
                    "lead_index": idx,
                    "endpoint": endpoint_label,
                    "cod": route_error_code,
                    "meta_pages_hint": route_error_hint,
                    "codi_id": codi_id,
                    "google_customer_id": google_cid,
                    "native_form_id": native_form_id,
                    "route_context": route_context,
                },
            )
            continue

        group_id = route["group_id"]
        _emit_runtime_event(
            stage="ROTA_RESOLVIDA",
            status="ok",
            detail=f"Cliente resolvido: {route['client_name']}",
            client_name=route["client_name"],
            page_id=page_id,
            group_id=group_id,
            payload={
                "lead_index": idx,
                "template": route.get("template", ""),
                "codi_id": codi_id,
                "google_customer_id": google_cid,
                "native_form_id": native_form_id,
                "route_origin": route.get("route_origin", "meta_page_id"),
                "route_target_type": route.get("route_target_type", "meta"),
                "route_context": route_context,
            },
        )
        if not group_id:
            skipped.append(f"lead_index_{idx}: group_id_ausente ({route['client_name']})")
            _wh_log(
                f"LEAD_{idx} | ERRO_CONFIG_CLIENTE | canal={channel_label} | cod=LEAD_GROUP_ID_VAZIO | "
                f"cliente={route['client_name']} | page_id={page_id}",
                level=logging.WARNING,
            )
            _emit_runtime_event(
                stage="IGNORADO_CONFIG",
                status="error",
                detail="Cliente sem group_id para envio",
                client_name=route["client_name"],
                page_id=page_id,
                payload={"lead_index": idx},
            )
            continue

        try:
            message = _format_lead_message(
                body,
                route["template"],
                route["client_name"],
                route=route,
                page_id=page_id,
            )
            _emit_runtime_event(
                stage="MENSAGEM_FORMATADA",
                status="ok",
                detail="Mensagem de lead formatada",
                client_name=route["client_name"],
                page_id=page_id,
                group_id=group_id,
                payload={"lead_index": idx, "message_len": len(message)},
            )
            if dry:
                _wh_log(
                    f"LEAD_{idx} | OK_SIMULADO | canal={channel_label} | cod=DRY_RUN_SEM_ENVIO | "
                    f"cliente={route['client_name']} | page_id={page_id} | preview_len={len(message)}"
                )
                sent += 1
                _emit_runtime_event(
                    stage="DRY_RUN",
                    status="info",
                    detail="Envio simulado (DRY_RUN)",
                    client_name=route["client_name"],
                    page_id=page_id,
                    group_id=group_id,
                    payload={"lead_index": idx},
                )
                continue

            client = get_evolution_client()
            if not client.send_text_message(group_id, message):
                errors.append(f"lead_index_{idx}: send returned false")
                _wh_log(
                    f"LEAD_{idx} | ERRO_WHATSAPP | canal={channel_label} | cod=EVOLUTION_SEND_TEXT_FALHOU | "
                    f"cliente={route['client_name']} | page_id={page_id} | group_id={group_id} | "
                    f"evolution_instance={_evolution_instance_label()}",
                    level=logging.ERROR,
                )
                _emit_runtime_event(
                    stage="WHATSAPP_FALHA",
                    status="error",
                    detail="Evolution retornou falha ao enviar para grupo",
                    client_name=route["client_name"],
                    page_id=page_id,
                    group_id=group_id,
                    payload={"lead_index": idx},
                )
                continue

            _wh_log(
                f"LEAD_{idx} | OK_WHATSAPP | canal={channel_label} | cod=MENSAGEM_ENVIADA_EVOLUTION | "
                f"cliente={route['client_name']} | page_id={page_id} | group_id={group_id} | "
                f"evolution_instance={_evolution_instance_label()}"
            )
            sent += 1
            _emit_runtime_event(
                stage="WHATSAPP_ENVIADO_OK",
                status="ok",
                detail="Mensagem enviada ao grupo",
                client_name=route["client_name"],
                page_id=page_id,
                group_id=group_id,
                payload={"lead_index": idx},
            )

            int_gid = (route.get("internal_notify_group_id") or "").strip()
            if int_gid:
                base_fields = _base_message_fields(body, route=route)
                int_ctx = {
                    "client_name": route["client_name"],
                    "page_id": page_id,
                    "template_id": route["template"],
                    "nome": base_fields["nome"],
                    "email": base_fields["email"],
                    "whatsapp": base_fields["whatsapp"],
                    "telefone_digitos": base_fields["telefone_digitos"],
                    "form_name": base_fields["form_name"],
                    "page_path": base_fields.get("page_path", ""),
                    "utm_source": base_fields.get("utm_source", ""),
                    "utm_medium": base_fields.get("utm_medium", ""),
                    "utm_campaign": base_fields.get("utm_campaign", ""),
                    "utm_term": base_fields.get("utm_term", ""),
                    "utm_content": base_fields.get("utm_content", ""),
                    "traffic_source": base_fields.get("traffic_source", "unknown"),
                    "traffic_origin_url": base_fields.get("traffic_origin_url", ""),
                    "origem_anuncio": str((route or {}).get("origem_anuncio", "")).strip(),
                    "cliente_origem": str((route or {}).get("cliente_origem", "")).strip(),
                    "respostas": base_fields["respostas"],
                    "respostas_filtradas": base_fields["respostas_filtradas"],
                    "respostas_raw": base_fields["respostas_raw"],
                    "respostas_omitidas": base_fields["respostas_omitidas"],
                    "respostas_count": base_fields["respostas_count"],
                    "respostas_raw_count": base_fields["respostas_raw_count"],
                    "respostas_omitidas_count": base_fields["respostas_omitidas_count"],
                    "received_at": base_fields["received_at"],
                    "chegada_em": base_fields["chegada_em"],
                }
                _inject_custom_variables_into_ctx(int_ctx, body, route)
                int_body = render_internal_lead_notify(route, int_ctx)
                if int_body.strip() and not client.send_text_message(int_gid, int_body):
                    logger.warning(
                        "Falha ao enviar copia interna da empresa | group=%s | cliente=%s",
                        int_gid,
                        route["client_name"],
                    )

            extra_phone = _digits_only(route.get("phone_number"))
            if route.get("template") == "pratical_life" and extra_phone:
                if client.send_text_message(extra_phone, message):
                    _wh_log(
                        f"LEAD_{idx} | OK_WHATSAPP_EXTRA | canal={channel_label} | cod=MENSAGEM_ENVIADA_TELEFONE | "
                        f"cliente={route['client_name']} | numero={extra_phone} | "
                        f"evolution_instance={_evolution_instance_label()}"
                    )
                else:
                    errors.append(f"lead_index_{idx}: phone send returned false")
                    _wh_log(
                        f"LEAD_{idx} | ERRO_WHATSAPP_EXTRA | canal={channel_label} | cod=EVOLUTION_SEND_TELEFONE_FALHOU | "
                        f"cliente={route['client_name']} | numero={extra_phone} | "
                        f"evolution_instance={_evolution_instance_label()}",
                        level=logging.ERROR,
                    )
                    _emit_runtime_event(
                        stage="WHATSAPP_TELEFONE_FALHA",
                        status="error",
                        detail="Falha no envio extra para telefone direto",
                        client_name=route["client_name"],
                        page_id=page_id,
                        payload={"lead_index": idx, "numero": extra_phone},
                    )
        except Exception as e:
            errors.append(f"lead_index_{idx}: {e!s}")
            _wh_log(
                f"LEAD_{idx} | ERRO_EXCECAO | canal={channel_label} | cod=EXCECAO_ENVIO_WHATSAPP | "
                f"cliente={route.get('client_name', '')} | page_id={page_id} | group_id={group_id} | "
                f"evolution_instance={_evolution_instance_label()} | err={e!s}",
                level=logging.ERROR,
            )
            logger.exception("Falha ao enviar lead %s", idx)
            _emit_runtime_event(
                stage="WHATSAPP_EXCECAO",
                status="error",
                detail=f"Exceção no envio: {e!s}",
                client_name=route.get("client_name", ""),
                page_id=page_id,
                group_id=group_id,
                payload={"lead_index": idx},
            )

    if errors:
        _wh_log(
            f"POST {endpoint_label} | ERRO_RESPOSTA | canal={channel_label} | cod=WEBHOOK_HTTP_500 | "
            f"sent={sent} | erros={len(errors)} | resumo_erros={_skipped_leads_summary(errors)}",
            level=logging.ERROR,
        )
        _emit_runtime_event(
            stage="CONCLUIDO_FALHA",
            status="error",
            detail=f"Webhook finalizado com erros ({len(errors)})",
            payload={"endpoint": endpoint_label, "sent": sent, "errors": errors, "skipped": skipped},
        )
        return jsonify({"ok": False, "sent": sent, "skipped": skipped, "errors": errors}), 500

    _wh_log(
        f"POST {endpoint_label} | CONCLUIDO_OK | canal={channel_label} | cod=WEBHOOK_LEADS_FINALIZADO | "
        f"sent={sent} | skipped={len(skipped)} | resumo_skipped={_skipped_leads_summary(skipped)}"
    )
    _emit_runtime_event(
        stage="CONCLUIDO_OK",
        status="ok",
        detail="Webhook finalizado sem erros",
        payload={"endpoint": endpoint_label, "sent": sent, "skipped": skipped},
    )
    return jsonify({"ok": True, "sent": sent, "skipped": skipped}), 200


def _normalize_cors_origin(origin: str) -> str:
    """Origin não deve ter path; remove barra final por tolerância a configs."""
    o = (origin or "").strip()
    while o.endswith("/"):
        o = o[:-1]
    return o


def _cors_origins_from_site_lead_routes() -> List[str]:
    """Origens guardadas no Pulseboard (Leads Site), só cadastros ativos."""
    try:
        from execution import persistence

        if persistence.db_enabled():
            persistence.ensure_db_ready()
        routes = persistence.list_site_lead_routes()
    except Exception as exc:
        logger.debug("CORS: não foi possível carregar cors_allowed_origins das rotas site: %s", exc)
        return []
    out: List[str] = []
    seen: set = set()
    for r in routes:
        if not r.get("enabled", True):
            continue
        for o in r.get("cors_allowed_origins") or []:
            n = _normalize_cors_origin(str(o))
            if not n:
                continue
            kl = n.lower()
            if kl in seen:
                continue
            seen.add(kl)
            out.append(n)
    return out


def _lead_webhook_cors_origins_config() -> Tuple[str, ...]:
    """
    Allowlist de origens (sem barra no final na lista). Vazio = sem CORS nos webhooks de lead.
    "*" só permitido se META_LEAD_WEBHOOK_CORS_CREDENTIALS não for true (cookies / fetch credentials).
    Unifica META_LEAD_WEBHOOK_CORS_ORIGINS com origens dos cadastros Leads Site ativos (Pulseboard).
    """
    raw = (os.getenv("META_LEAD_WEBHOOK_CORS_ORIGINS") or "").strip()
    db_origins = _cors_origins_from_site_lead_routes()
    if raw == "*":
        if _lead_webhook_cors_credentials_enabled():
            logger.warning(
                "META_LEAD_WEBHOOK_CORS_ORIGINS=* é incompatível com META_LEAD_WEBHOOK_CORS_CREDENTIALS=true; "
                "use uma lista explícita de origens."
            )
            return ()
        return ("*",)
    env_parts = [_normalize_cors_origin(o) for o in raw.split(",") if o.strip()] if raw else []
    merged: List[str] = []
    seen: set = set()
    for o in env_parts + db_origins:
        if not o:
            continue
        kl = o.lower()
        if kl in seen:
            continue
        seen.add(kl)
        merged.append(o)
    return tuple(merged)


def _lead_webhook_cors_credentials_enabled() -> bool:
    v = (os.getenv("META_LEAD_WEBHOOK_CORS_CREDENTIALS") or "").strip().lower()
    return v in ("1", "true", "yes")


def _lead_webhook_cors_allow_headers_value() -> str:
    raw = (os.getenv("META_LEAD_WEBHOOK_CORS_ALLOW_HEADERS") or "").strip()
    if raw:
        return raw
    return "Content-Type, Authorization, X-Webhook-Secret, X-Request-Id, Accept, Accept-Language"


def _lead_webhook_cors_allow_methods_value() -> str:
    raw = (os.getenv("META_LEAD_WEBHOOK_CORS_ALLOW_METHODS") or "").strip()
    if raw:
        return raw
    return "POST, OPTIONS"


def _lead_webhook_cors_max_age() -> str:
    return (os.getenv("META_LEAD_WEBHOOK_CORS_MAX_AGE") or "86400").strip() or "86400"


def _lead_webhook_cors_expose_headers_value() -> str:
    return (os.getenv("META_LEAD_WEBHOOK_CORS_EXPOSE_HEADERS") or "").strip()


def _lead_webhook_cors_allow_origin_value() -> Optional[str]:
    cfg = _lead_webhook_cors_origins_config()
    if not cfg:
        return None
    if cfg == ("*",):
        return "*"
    origin = _normalize_cors_origin(request.headers.get("Origin") or "")
    if origin and origin in cfg:
        return origin
    return None


def _apply_lead_webhook_cors_to_response(
    response: Response, allow_origin: str, *, preflight: bool = False
) -> None:
    response.headers["Access-Control-Allow-Origin"] = allow_origin
    if allow_origin != "*":
        response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Methods"] = _lead_webhook_cors_allow_methods_value()
    response.headers["Access-Control-Allow-Headers"] = _lead_webhook_cors_allow_headers_value()
    if preflight:
        response.headers["Access-Control-Max-Age"] = _lead_webhook_cors_max_age()
    if _lead_webhook_cors_credentials_enabled():
        response.headers["Access-Control-Allow-Credentials"] = "true"
    expose = _lead_webhook_cors_expose_headers_value()
    if expose:
        response.headers["Access-Control-Expose-Headers"] = expose


def _apply_lead_webhook_cors_headers(response: Response) -> Response:
    allow = _lead_webhook_cors_allow_origin_value()
    if not allow:
        return response
    _apply_lead_webhook_cors_to_response(response, allow)
    return response


@app.before_request
def _lead_webhook_cors_preflight() -> Optional[Any]:
    if request.method != "OPTIONS":
        return None
    path = request.path or ""
    if path not in _LEAD_WEBHOOK_PATHS:
        return None
    cfg = _lead_webhook_cors_origins_config()
    if not cfg:
        return None
    allow = _lead_webhook_cors_allow_origin_value()
    if not allow:
        return Response(status=403)
    resp = Response(status=204)
    _apply_lead_webhook_cors_to_response(resp, allow, preflight=True)
    return resp


@app.after_request
def _lead_webhook_cors_after(response: Response) -> Response:
    path = request.path or ""
    if path not in _LEAD_WEBHOOK_PATHS:
        return response
    return _apply_lead_webhook_cors_headers(response)


@app.route("/meta-new-lead", methods=["POST"])
def meta_new_lead():
    response, status = _handle_meta_new_lead(
        "/meta-new-lead",
        route_mode="meta_only",
        channel_label="leads_meta",
    )
    return response, status


@app.route("/google-new-lead", methods=["POST"])
def google_new_lead():
    """Leads Google Ads: roteamento por google_customer_id → tabela google_clients."""
    response, status = _handle_meta_new_lead(
        "/google-new-lead",
        route_mode="google_only",
        channel_label="leads_google",
    )
    return response, status


@app.route("/site-new-lead", methods=["POST"])
def site_new_lead():
    """
    Endpoint dedicado para leads do site:
    - exige codi_id válido (28–36 dígitos)
    - ignora page_id para roteamento
    """
    response, status = _handle_meta_new_lead(
        "/site-new-lead",
        route_mode="site_only",
        channel_label="leads_site",
    )
    return response, status


@app.get("/health")
def meta_service_health() -> Any:
    """Health check HTTP (porta 8080) sem autenticacao — use no Easypanel em vez de POST /meta-new-lead."""
    return jsonify({"ok": True, "service": "meta_lead_webhook"}), 200


@app.before_request
def _dash_proxy_auth() -> Optional[Any]:
    path = request.path or ""
    if not path.startswith("/dash"):
        return None
    return dashboard_module.dashboard_auth_gate_response()


@app.get("/dash/login")
def dash_login_page() -> str:
    next_url = request.args.get("next") or "/dash/"
    return render_template(
        "login.html",
        **dashboard_module.dashboard_login_page_context(
            next_url=next_url,
            error=None,
            form_action="/dash/login",
        ),
    )


@app.post("/dash/login")
def dash_login_post() -> Any:
    next_url = (request.form.get("next") or "/dash/").strip() or "/dash/"
    if not dashboard_module.dashboard_auth_configured():
        return redirect(next_url)
    email = (request.form.get("email") or "").strip()
    pwd = request.form.get("password") or ""
    if dashboard_module.verify_dashboard_credentials(email, pwd):
        session["dashboard_ok"] = True
        return redirect(next_url)
    err = "E-mail ou senha incorretos." if dashboard_module.dashboard_require_email_login() else "Senha incorreta."
    return render_template(
        "login.html",
        **dashboard_module.dashboard_login_page_context(
            next_url=next_url,
            error=err,
            form_action="/dash/login",
        ),
    )


@app.get("/dash/logout")
def dash_logout() -> Any:
    session.clear()
    return redirect("/dash/login")


@app.get("/dash/api/health")
def dash_api_health() -> Any:
    return dashboard_module.api_health()


@app.get("/dash")
@app.get("/dash/")
def dash_home():
    return render_template("dashboard.html", dashboard_base="/dash")


@app.get("/dash/api/clients")
def dash_api_clients():
    return jsonify(dashboard_module._build_clients_response())


@app.post("/dash/api/clients")
def dash_api_add_client():
    return dashboard_module.api_add_client()


@app.put("/dash/api/clients/<int:client_id>")
def dash_api_update_client(client_id: int):
    return dashboard_module.api_update_client(client_id)


@app.get("/dash/api/google-clients")
def dash_api_google_clients():
    return dashboard_module.api_google_clients()


@app.post("/dash/api/google-clients")
def dash_api_add_google_client():
    return dashboard_module.api_add_google_client()


@app.put("/dash/api/google-clients/<int:client_id>")
def dash_api_update_google_client(client_id: int):
    return dashboard_module.api_update_google_client(client_id)


@app.get("/dash/api/site-lead-routes")
def dash_api_site_lead_routes() -> Any:
    return dashboard_module.api_site_lead_routes()


@app.post("/dash/api/site-lead-routes")
def dash_api_add_site_lead_route() -> Any:
    return dashboard_module.api_add_site_lead_route()


@app.put("/dash/api/site-lead-routes/<int:route_id>")
def dash_api_update_site_lead_route(route_id: int) -> Any:
    return dashboard_module.api_update_site_lead_route(route_id)


@app.delete("/dash/api/site-lead-routes/<int:route_id>")
def dash_api_delete_site_lead_route(route_id: int) -> Any:
    return dashboard_module.api_delete_site_lead_route(route_id)


@app.get("/dash/api/message-templates")
def dash_api_message_templates():
    return dashboard_module.api_message_templates()


@app.put("/dash/api/message-templates/<channel>/<template_id>")
def dash_api_upsert_message_template(channel: str, template_id: str):
    return dashboard_module.api_upsert_message_template(channel, template_id)


@app.put("/dash/api/message-filters/<channel>")
def dash_api_upsert_message_filters(channel: str):
    return dashboard_module.api_upsert_message_filters(channel)


@app.post("/dash/api/message-templates/preview")
def dash_api_message_template_preview():
    return dashboard_module.api_message_template_preview()


@app.post("/dash/api/message-templates/custom-variable-preview")
def dash_api_custom_variable_transformation_preview():
    return dashboard_module.api_custom_variable_transformation_preview()


@app.put("/dash/api/message-templates/variable-resolution/<channel>")
def dash_api_upsert_variable_resolution(channel: str):
    return dashboard_module.api_upsert_variable_resolution(channel)


@app.put("/dash/api/message-templates/custom-variables/<channel>")
def dash_api_upsert_custom_variables(channel: str):
    return dashboard_module.api_upsert_custom_variables(channel)


@app.post("/dash/api/harness/simulate-webhook")
def dash_api_harness_simulate_webhook():
    return dashboard_module.api_harness_simulate_webhook()


@app.get("/dash/api/events/recent")
def dash_api_events_recent():
    return dashboard_module.api_events_recent()


@app.get("/dash/api/events/stream")
def dash_api_events_stream() -> Response:
    return dashboard_module.api_events_stream()


@app.post("/evolution-webhook")
def meta_evolution_catalog_webhook() -> Any:
    return dashboard_module.evolution_catalog_webhook_view()


@app.get("/dash/api/catalog-groups")
def dash_api_catalog_groups_list():
    return dashboard_module.api_catalog_groups_list()


@app.patch("/dash/api/catalog-groups")
def dash_api_catalog_groups_patch():
    return dashboard_module.api_catalog_groups_patch()


@app.delete("/dash/api/catalog-groups")
def dash_api_catalog_groups_delete():
    return dashboard_module.api_catalog_groups_delete()


@app.post("/dash/api/catalog-groups/refresh")
def dash_api_catalog_groups_refresh():
    return dashboard_module.api_catalog_groups_refresh()


@app.route("/dash/api/catalog-groups/webhook-listener", methods=["GET", "POST"])
def dash_api_catalog_webhook_listener():
    return dashboard_module.api_catalog_webhook_listener()


@app.get("/dash/api/meta-catalog/ad-accounts")
def dash_api_meta_catalog_ad_accounts() -> Any:
    return jsonify(dashboard_module.meta_catalog_ad_accounts_payload())


@app.get("/dash/api/meta-catalog/pages")
def dash_api_meta_catalog_pages() -> Any:
    return jsonify(dashboard_module.meta_catalog_pages_payload())


def main() -> None:
    _load_env()
    port = int(os.getenv("WEBHOOK_PORT", "8080"))
    _wh_log(
        "SERVICO_INICIADO | "
        f"escutando 0.0.0.0:{port} | GET /health | POST /meta-new-lead | POST /google-new-lead | POST /site-new-lead | "
        f"dashboard /dash | catalogo POST /evolution-webhook"
    )
    serve_flask_app(app, port=port)


if __name__ == "__main__":
    main()

