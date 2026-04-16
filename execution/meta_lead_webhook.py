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
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from flask import Flask, jsonify, request

# Raiz do projeto no path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.evolution_client import get_evolution_client

log_dir = os.path.join(os.path.dirname(__file__), "..", ".tmp")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "execution.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

LOG_PREFIX = "[P12_META_LEAD_WEBHOOK]"
_EXCLUDE_RESPOSTAS = frozenset({"nome_completo", "email", "telefone", "page_id", "pageId"})
_WHATSAPP_MSG_MAX = 4000


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


def _format_whatsapp_line(raw_phone: Optional[str]) -> str:
    digits = _digits_only(raw_phone)
    if digits:
        return f"https://wa.me/{digits}"
    fallback = _fallback_whatsapp_text()
    if fallback:
        return fallback
    return "(nao informado)"


def _mappable_lookup(mappable: List[Dict[str, Any]], name: str) -> str:
    for row in mappable:
        if not isinstance(row, dict):
            continue
        if str(row.get("name", "")).strip() == name:
            v = row.get("value")
            return _format_field_value(v)
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
    data = d.get("data")
    if not isinstance(data, dict):
        return False
    if d.get("leadgenId") is not None:
        return True
    if isinstance(d.get("mappable_field_data"), list):
        return True
    if "email" in data or "nome_completo" in data or "telefone" in data:
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

    # Envelope no formato {"body": {...}, ...}
    if "body" in item:
        inner = _coerce_inner_body(item.get("body"))
        if not inner:
            return None
        page_id = _first_non_empty(
            item.get("page_id"),
            item.get("pageId"),
            _extract_page_id_from_dict(inner),
        )
        if _is_meta_lead_body(inner) or isinstance(inner.get("data"), dict):
            return {"body": inner, "page_id": page_id}
        return None

    # Lead direto
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


def _ensure_mappable(body: Dict[str, Any], data: Dict[str, Any]) -> List[Dict[str, Any]]:
    mappable = body.get("mappable_field_data")
    if isinstance(mappable, list) and len(mappable) > 0:
        return mappable
    return _mappable_from_data(data)


def _load_clients() -> List[Dict[str, Any]]:
    clients_path = os.path.join(os.path.dirname(__file__), "..", "clients.json")
    try:
        with open(clients_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [c for c in data if isinstance(c, dict)]
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Falha ao carregar clients.json para roteamento de leads: %s", e)
    return []


def _resolve_lead_route(page_id: str) -> Optional[Dict[str, str]]:
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
            "template": str(c.get("lead_template", "")).strip() or "default",
        }
    return None


def _resolve_legacy_lorena_route() -> Optional[Dict[str, str]]:
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
            "template": template or "lorena",
        }
    return None


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


def _base_message_fields(body: Dict[str, Any]) -> Dict[str, str]:
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)

    nome = _format_field_value(data.get("nome_completo")) or _mappable_lookup(mappable, "nome_completo")
    email = _format_field_value(data.get("email")) or _mappable_lookup(mappable, "email")
    telefone_raw = data.get("telefone")
    if telefone_raw is None or telefone_raw == "":
        telefone_raw = _mappable_lookup(mappable, "telefone")
    wa_link = _format_whatsapp_line(telefone_raw)

    respostas = _build_respostas_text(mappable)
    if not respostas:
        respostas = "(nenhuma resposta adicional)"

    return {
        "nome": nome or "(nao informado)",
        "email": email or "(nao informado)",
        "whatsapp": wa_link,
        "respostas": respostas,
    }


def _truncate_message(msg: str) -> str:
    if len(msg) <= _WHATSAPP_MSG_MAX:
        return msg
    cut = _WHATSAPP_MSG_MAX - 20
    logger.warning("Mensagem de lead truncada para %s caracteres", _WHATSAPP_MSG_MAX)
    return msg[:cut] + "\n...(truncado)"


def _format_default_lead_message(body: Dict[str, Any], client_name: str) -> str:
    base = _base_message_fields(body)
    msg = (
        f"Novo lead - {client_name}\n"
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


def _format_pratical_life_lead_message(body: Dict[str, Any], client_name: str) -> str:
    base = _base_message_fields(body)
    msg = (
        f"Novo lead recebido - {client_name}\n"
        f"Contato:\n"
        f"- Nome: {base['nome']}\n"
        f"- WhatsApp: {base['whatsapp']}\n"
        f"- E-mail: {base['email']}\n"
        f"\n"
        f"Formulario:\n"
        f"{base['respostas']}"
    )
    return _truncate_message(msg)


TEMPLATE_FORMATTERS: Dict[str, Callable[[Dict[str, Any], str], str]] = {
    "default": _format_default_lead_message,
    "lorena": _format_default_lead_message,
    "pratical_life": _format_pratical_life_lead_message,
}


def _format_lead_message(body: Dict[str, Any], template_id: str, client_name: str) -> str:
    formatter = TEMPLATE_FORMATTERS.get(template_id, TEMPLATE_FORMATTERS["default"])
    return formatter(body, client_name)


def _handle_meta_new_lead(endpoint_label: str, allow_legacy_lorena_fallback: bool = False) -> Tuple[Any, int]:
    denied = _check_webhook_secret()
    if denied:
        _wh_log(
            f"POST {endpoint_label} | NEGADO_AUTH | "
            f"ip={_client_ip()} | content_length={request.content_length}",
            level=logging.WARNING,
        )
        return denied

    _wh_log(
        f"POST {endpoint_label} | RECEBIDO | "
        f"ip={_client_ip()} | content_type={request.content_type!r} | content_length={request.content_length}"
    )

    raw, parse_err = parse_incoming_payload()
    if raw is None:
        _wh_log(
            f"POST {endpoint_label} | ERRO_JSON | "
            f"ip={_client_ip()} | motivo={parse_err}",
            level=logging.WARNING,
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
        _wh_log(f"POST {endpoint_label} | ERRO_PAYLOAD | nenhum lead extraido", level=logging.WARNING)
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

    _wh_log(f"POST {endpoint_label} | PAYLOAD_OK | leads={len(events)}")

    dry = os.getenv("DRY_RUN", "false").lower() == "true"
    sent = 0
    errors: List[str] = []
    skipped: List[str] = []

    for idx, event in enumerate(events):
        body = event["body"]
        page_id = str(event.get("page_id", "")).strip()
        route = _resolve_lead_route(page_id)
        if not route and allow_legacy_lorena_fallback and not page_id:
            route = _resolve_legacy_lorena_route()
        if not route:
            skipped.append(f"lead_index_{idx}: page_id_nao_mapeado ({page_id or 'vazio'})")
            _wh_log(
                f"LEAD_{idx} | IGNORADO_ROUTE | page_id_nao_mapeado={page_id or 'vazio'}",
                level=logging.WARNING,
            )
            continue

        group_id = route["group_id"]
        if not group_id:
            skipped.append(f"lead_index_{idx}: group_id_ausente ({route['client_name']})")
            _wh_log(
                f"LEAD_{idx} | IGNORADO_CONFIG | group_id_ausente | cliente={route['client_name']}",
                level=logging.WARNING,
            )
            continue

        try:
            message = _format_lead_message(body, route["template"], route["client_name"])
            if dry:
                _wh_log(
                    f"LEAD_{idx} | DRY_RUN | cliente={route['client_name']} | page_id={page_id} | preview_len={len(message)}"
                )
                sent += 1
                continue

            client = get_evolution_client()
            if client.send_text_message(group_id, message):
                _wh_log(
                    f"LEAD_{idx} | WHATSAPP_ENVIADO_OK | cliente={route['client_name']} | page_id={page_id}"
                )
                sent += 1
            else:
                errors.append(f"lead_index_{idx}: send returned false")
                _wh_log(
                    f"LEAD_{idx} | WHATSAPP_FALHA | cliente={route['client_name']}",
                    level=logging.ERROR,
                )
        except Exception as e:
            errors.append(f"lead_index_{idx}: {e!s}")
            _wh_log(f"LEAD_{idx} | WHATSAPP_EXCECAO | {e!s}", level=logging.ERROR)
            logger.exception("Falha ao enviar lead %s", idx)

    if errors:
        _wh_log(
            f"POST {endpoint_label} | RESPOSTA_500 | sent={sent} | erros={len(errors)}",
            level=logging.ERROR,
        )
        return jsonify({"ok": False, "sent": sent, "skipped": skipped, "errors": errors}), 500

    _wh_log(f"POST {endpoint_label} | CONCLUIDO_OK | sent={sent} | skipped={len(skipped)}")
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


def main() -> None:
    _load_env()
    port = int(os.getenv("WEBHOOK_PORT", "8080"))
    _wh_log(
        "SERVICO_INICIADO | "
        f"escutando 0.0.0.0:{port} | rotas POST /meta-new-lead e /lorena-new-lead (legado)"
    )
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()

