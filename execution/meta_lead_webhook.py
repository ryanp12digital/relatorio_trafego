"""
Webhook HTTP: leads do Make/Meta -> mensagem formatada no grupo WhatsApp.

Rotas:
- POST /meta-new-lead      (rota padrao multi-cliente)
- POST /lorena-new-lead    (alias legado para compatibilidade)
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import sys
import requests
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, redirect, render_template, request, session

# Raiz do projeto no path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.evolution_client import get_evolution_client
from execution import dashboard_app as dashboard_module
from execution.flask_server import serve_flask_app
from execution.live_events import publish_event
from execution.message_templates import get_filter_rules, get_template_content, render_internal_lead_notify, render_template_text
from execution.project_paths import clients_json_path, google_clients_json_path

log_dir = os.path.join(os.path.dirname(__file__), "..", ".tmp")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "execution.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
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
# Ordem: legado Meta PT primeiro; depois chaves comuns em formulários instantâneos / Make.
_LEAD_NAME_FIELD_KEYS = ("nome_completo", "nome", "full_name", "name")
_LEAD_PHONE_FIELD_KEYS = ("telefone", "phone_number", "phone", "mobile", "celular")
_LEAD_BODY_SIGNAL_KEYS = frozenset(
    {"email", "nome_completo", "telefone", "nome", "full_name", "name", "phone_number", "phone", "mobile", "celular"}
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


def _emoji_for_log(message: str) -> str:
    text = message.upper()
    if "SERVICO_INICIADO" in text:
        return "🚀"
    if "NEGADO_AUTH" in text:
        return "🔒"
    if "RECEBIDO" in text:
        return "📥"
    if "PAYLOAD_OK" in text:
        return "📦"
    if "DRY_RUN" in text:
        return "🧪"
    if "WHATSAPP_ENVIADO_OK" in text:
        return "📤"
    if "CONCLUIDO_OK" in text:
        return "✅"
    if "ERRO" in text or "FALHA" in text or "EXCECAO" in text:
        return "❌"
    return "ℹ️"


def _wh_log(message: str, level: int = logging.INFO) -> None:
    logger.log(level, "%s %s %s", LOG_PREFIX, _emoji_for_log(message), message)


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
    # Prioriza variavel nova; fallback para variavel legada.
    fallback = (os.getenv("META_LEAD_FALLBACK_WHATSAPP") or "").strip()
    if fallback:
        return fallback
    return (os.getenv("LORENA_FALLBACK_WHATSAPP") or "").strip()


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
    if any(k in data for k in _LEAD_BODY_SIGNAL_KEYS):
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
        group_id = str(c.get("lead_group_id") or c.get("group_id") or "").strip()
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
    group_id = str(c.get("lead_group_id") or c.get("group_id") or "").strip()
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
    return {
        "client_name": str(c.get("client_name", "")).strip() or "Cliente",
        "group_id": str(c.get("group_id") or "").strip(),
        "phone_number": str(c.get("lead_phone_number", "")).strip(),
        "template": "default",
        "exclude_exact": [],
        "exclude_contains": [],
        "exclude_regex": [],
        "internal_notify_group_id": str(c.get("internal_notify_group_id", "")).strip(),
        "internal_lead_template": "",
        "internal_weekly_template": str(c.get("internal_weekly_template", "")).strip(),
        "internal_notify_message": str(c.get("internal_notify_message", "")).strip(),
    }


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


def _is_valid_site_codi_id(value: str) -> bool:
    return bool(re.fullmatch(r"\d{32}", (value or "").strip()))


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
    target_type = str(target.get("target_type", "meta")).strip().lower()
    target_name = str(target.get("target_client_name", "")).strip()
    if target_type == "google":
        for c in _load_google_clients():
            if c.get("enabled", True) is False:
                continue
            if str(c.get("client_name", "")).strip() == target_name:
                route = _route_from_google_client(c)
                route["route_origin"] = "site_codi_id"
                route["site_codi_id"] = cid
                route["route_target_type"] = "google"
                return route
        return None
    for c in _load_clients():
        if c.get("enabled", True) is False:
            continue
        if str(c.get("client_name", "")).strip() == target_name:
            route = _route_from_meta_client(c)
            route["route_origin"] = "site_codi_id"
            route["site_codi_id"] = cid
            route["route_target_type"] = "meta"
            return route
    return None


def _resolve_route_with_context(page_id: str, codi_id: str) -> Tuple[Optional[Dict[str, Any]], str, str, str]:
    """
    Organização explícita de contexto de roteamento:
    - native_ads: usa page_id (Meta/Google nativo)
    - site: usa codi_id (leads de site)
    """
    if page_id:
        return _resolve_lead_route(page_id), "native_ads", "PAGE_ID_SEM_CLIENTE_NA_PULSEBOARD", _configured_meta_pages_hint()
    if codi_id:
        if not _is_valid_site_codi_id(codi_id):
            return (
                None,
                "site",
                "CODI_ID_INVALID_FORMAT",
                f"codi_id_recebido={codi_id} (esperado: 32 dígitos numéricos)",
            )
        route = _resolve_site_lead_route(codi_id)
        if route:
            return route, "site", "", ""
        return None, "site", "CODI_ID_ROUTE_NOT_FOUND", f"codi_id_recebido={codi_id}"
    return None, "unknown", "ROUTING_KEY_MISSING", "Envie page_id (Meta/Ads) ou codi_id (Lead Site)"


def _resolve_legacy_lorena_route() -> Optional[Dict[str, Any]]:
    """
    Compatibilidade do endpoint legado:
    se payload antigo nao trouxer page_id, cai no cliente Lorena.
    """
    clients = _load_clients()
    for c in clients:
        client_name = str(c.get("client_name", "")).strip()
        template = str(c.get("lead_template", "")).strip()
        if client_name != "Lorena Carvalho" and template != "lorena":
            continue
        group_id = str(c.get("lead_group_id") or c.get("group_id") or "").strip()
        return {
            "client_name": client_name or "Lorena Carvalho",
            "group_id": group_id,
            "phone_number": "",
            "template": template or "lorena",
            "exclude_exact": [],
            "exclude_contains": [],
            "exclude_regex": [],
            "internal_notify_group_id": str(c.get("internal_notify_group_id", "")).strip(),
            "internal_lead_template": str(c.get("internal_lead_template", "")).strip(),
            "internal_weekly_template": str(c.get("internal_weekly_template", "")).strip(),
            "internal_notify_message": str(c.get("internal_notify_message", "")).strip(),
        }
    return None


def _allow_default_no_page_legacy_fallback() -> bool:
    """
    Compatibilidade: no endpoint padrão, payloads antigos sem page_id
    podem cair no roteamento legado da Lorena.

    Para desativar explicitamente:
      META_LEAD_ALLOW_DEFAULT_NO_PAGE_FALLBACK=0|false|no
    """
    raw = (os.getenv("META_LEAD_ALLOW_DEFAULT_NO_PAGE_FALLBACK") or "false").strip().lower()
    return raw not in {"0", "false", "no"}


def _check_webhook_secret() -> Optional[Tuple[Any, int]]:
    secret = (os.getenv("META_LEAD_WEBHOOK_SECRET") or os.getenv("LORENA_LEAD_WEBHOOK_SECRET") or "").strip()
    if not secret:
        return None
    hdr = (request.headers.get("X-Webhook-Secret") or "").strip()
    auth = request.headers.get("Authorization") or ""
    bearer = ""
    if auth.lower().startswith("bearer "):
        bearer = auth[7:].strip()
    if hdr == secret or bearer == secret:
        return None
    return jsonify({"ok": False, "error": "unauthorized"}), 401


def _base_message_fields(body: Dict[str, Any], route: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)
    global_rules = get_filter_rules("meta_lead")
    client_rules = {
        "exclude_exact": [str(v).strip().lower() for v in ((route or {}).get("exclude_exact") or []) if str(v).strip()],
        "exclude_contains": [str(v).strip().lower() for v in ((route or {}).get("exclude_contains") or []) if str(v).strip()],
        "exclude_regex": [str(v).strip() for v in ((route or {}).get("exclude_regex") or []) if str(v).strip()],
    }

    nome = _first_field_from_data_and_mappable(_LEAD_NAME_FIELD_KEYS, data, mappable)
    email = _format_field_value(data.get("email")) or _mappable_lookup(mappable, "email")
    telefone_raw = _first_field_from_data_and_mappable(_LEAD_PHONE_FIELD_KEYS, data, mappable)
    wa_link = _format_whatsapp_line(telefone_raw)
    telefone_digitos = _digits_only(telefone_raw)

    respostas_bundle = _build_respostas_bundle(
        mappable,
        global_rules=global_rules,
        client_rules=client_rules,
    )

    received_ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return {
        "nome": nome or "(nao informado)",
        "email": email or "(nao informado)",
        "whatsapp": wa_link,
        "telefone_digitos": telefone_digitos or "(nao informado)",
        "form_name": _extract_form_name(body),
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


def _extract_form_name(body: Dict[str, Any]) -> str:
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
    "lorena": _format_default_lead_message,
    "pratical_life": _format_pratical_life_lead_message,
}


def _format_lead_message(
    body: Dict[str, Any],
    template_id: str,
    client_name: str,
    route: Optional[Dict[str, Any]] = None,
    page_id: str = "",
) -> str:
    custom_content = get_template_content("meta_lead", template_id)
    if custom_content:
        base = _base_message_fields(body, route=route)
        rendered = render_template_text(
            custom_content,
            {
                "client_name": client_name,
                "page_id": page_id,
                "template_id": template_id,
                "nome": base["nome"],
                "email": base["email"],
                "whatsapp": base["whatsapp"],
                "telefone_digitos": base["telefone_digitos"],
                "form_name": base["form_name"],
                "respostas": base["respostas"],
                "respostas_filtradas": base["respostas_filtradas"],
                "respostas_raw": base["respostas_raw"],
                "respostas_omitidas": base["respostas_omitidas"],
                "respostas_count": base["respostas_count"],
                "respostas_raw_count": base["respostas_raw_count"],
                "respostas_omitidas_count": base["respostas_omitidas_count"],
                "received_at": base["received_at"],
                "chegada_em": base["chegada_em"],
            },
        )
        return _truncate_message(rendered)
    formatter = TEMPLATE_FORMATTERS.get(template_id, TEMPLATE_FORMATTERS["default"])
    return formatter(body, client_name, route)


def _handle_meta_new_lead(endpoint_label: str, allow_legacy_lorena_fallback: bool = False) -> Tuple[Any, int]:
    denied = _check_webhook_secret()
    if denied:
        _wh_log(
            f"POST {endpoint_label} | ERRO_AUTH | canal=leads_meta | cod=WEBHOOK_SECRET_META_NEGADO | "
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
        f"POST {endpoint_label} | RECEBIDO | canal=leads_meta | "
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
            f"POST {endpoint_label} | ERRO_JSON | canal=leads_meta | cod=PAYLOAD_NAO_E_JSON | "
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
        _wh_log(
            f"POST {endpoint_label} | ERRO_LEAD | canal=leads_meta | cod=PAYLOAD_SEM_LEAD_RECONHECIDO | "
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

    _wh_log(f"POST {endpoint_label} | PAYLOAD_OK | canal=leads_meta | cod=LEAD_NORMALIZADO | leads={len(events)}")
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
        native_form_id = _extract_native_form_id_from_body(body)
        route, route_context, route_error_code, route_error_hint = _resolve_route_with_context(page_id, codi_id)
        route_error_detail = (
            "Lead ignorado por page_id não mapeado"
            if route_error_code == "PAGE_ID_SEM_CLIENTE_NA_PULSEBOARD"
            else (
                "Lead de site ignorado por codi_id fora do padrão"
                if route_error_code == "CODI_ID_INVALID_FORMAT"
                else (
                    "Lead de site ignorado por codi_id sem rota cadastrada"
                    if route_error_code == "CODI_ID_ROUTE_NOT_FOUND"
                    else "Lead ignorado sem page_id e sem codi_id"
                )
            )
        )
        if route and route_context == "site":
            _wh_log(
                f"LEAD_{idx} | ROTA_CODI_ID_OK | canal=leads_site | cod=CODI_ID_ROUTE_MATCH | "
                f"codi_id={codi_id} | cliente={route.get('client_name','')}"
            )
        should_try_legacy_no_page = (
            not page_id
            and not codi_id
            and (
                allow_legacy_lorena_fallback
                or (
                    endpoint_label == "/meta-new-lead"
                    and _allow_default_no_page_legacy_fallback()
                )
            )
        )
        if not route and should_try_legacy_no_page:
            route = _resolve_legacy_lorena_route()
            if route:
                _wh_log(
                    f"LEAD_{idx} | AVISO_ROTA | canal=leads_meta | cod=FALLBACK_LEGADO_LORENA | "
                    f"endpoint={endpoint_label} | cliente={route['client_name']}"
                )
                _emit_runtime_event(
                    stage="FALLBACK_LEGADO_APLICADO",
                    status="warning",
                    detail="Roteamento sem page_id via fallback legado",
                    client_name=route["client_name"],
                    page_id=page_id,
                    group_id=route["group_id"],
                    payload={"lead_index": idx, "endpoint": endpoint_label},
                )
        if not route:
            skipped.append(
                f"lead_index_{idx}: rota_nao_mapeada (page_id={page_id or 'vazio'}; codi_id={codi_id or 'vazio'})"
            )
            _wh_log(
                f"LEAD_{idx} | ERRO_ROTA_CLIENTE | canal=leads_meta | cod={route_error_code} | "
                f"page_id_recebido={page_id or 'vazio'} | codi_id_recebido={codi_id or 'vazio'} | "
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
                "native_form_id": native_form_id,
                "route_origin": route.get("route_origin", "meta_page_id"),
                "route_target_type": route.get("route_target_type", "meta"),
                "route_context": route_context,
            },
        )
        if not group_id:
            skipped.append(f"lead_index_{idx}: group_id_ausente ({route['client_name']})")
            _wh_log(
                f"LEAD_{idx} | ERRO_CONFIG_CLIENTE | canal=leads_meta | cod=LEAD_GROUP_ID_VAZIO | "
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
                    f"LEAD_{idx} | OK_SIMULADO | canal=leads_meta | cod=DRY_RUN_SEM_ENVIO | "
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
                    f"LEAD_{idx} | ERRO_WHATSAPP | canal=leads_meta | cod=EVOLUTION_SEND_TEXT_FALHOU | "
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
                f"LEAD_{idx} | OK_WHATSAPP | canal=leads_meta | cod=MENSAGEM_ENVIADA_EVOLUTION | "
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
                        f"LEAD_{idx} | OK_WHATSAPP_EXTRA | canal=leads_meta | cod=MENSAGEM_ENVIADA_TELEFONE | "
                        f"cliente={route['client_name']} | numero={extra_phone} | "
                        f"evolution_instance={_evolution_instance_label()}"
                    )
                else:
                    errors.append(f"lead_index_{idx}: phone send returned false")
                    _wh_log(
                        f"LEAD_{idx} | ERRO_WHATSAPP_EXTRA | canal=leads_meta | cod=EVOLUTION_SEND_TELEFONE_FALHOU | "
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
                f"LEAD_{idx} | ERRO_EXCECAO | canal=leads_meta | cod=EXCECAO_ENVIO_WHATSAPP | "
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
            f"POST {endpoint_label} | ERRO_RESPOSTA | canal=leads_meta | cod=WEBHOOK_HTTP_500 | "
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
        f"POST {endpoint_label} | CONCLUIDO_OK | canal=leads_meta | cod=WEBHOOK_LEADS_FINALIZADO | "
        f"sent={sent} | skipped={len(skipped)} | resumo_skipped={_skipped_leads_summary(skipped)}"
    )
    _emit_runtime_event(
        stage="CONCLUIDO_OK",
        status="ok",
        detail="Webhook finalizado sem erros",
        payload={"endpoint": endpoint_label, "sent": sent, "skipped": skipped},
    )
    return jsonify({"ok": True, "sent": sent, "skipped": skipped}), 200


@app.route("/meta-new-lead", methods=["POST"])
def meta_new_lead():
    response, status = _handle_meta_new_lead("/meta-new-lead")
    return response, status


@app.route("/lorena-new-lead", methods=["POST"])
def lorena_new_lead_legacy():
    response, status = _handle_meta_new_lead(
        "/lorena-new-lead",
        allow_legacy_lorena_fallback=True,
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
        f"escutando 0.0.0.0:{port} | GET /health | POST /meta-new-lead | dashboard /dash | "
        f"catalogo grupos POST /evolution-webhook"
    )
    serve_flask_app(app, port=port)


if __name__ == "__main__":
    main()

