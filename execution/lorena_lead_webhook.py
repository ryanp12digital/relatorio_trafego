"""
Webhook HTTP: leads do Make (envelope Meta) -> mensagem formatada no grupo WhatsApp (Lorena).

Rota: POST /lorena-new-lead
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

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

# Prefixo único para filtrar nos logs do Easypanel / arquivo: ex. buscar "P12_LORENA_WEBHOOK"
LOG_PREFIX = "[P12_LORENA_WEBHOOK]"

_EXCLUDE_RESPOSTAS = frozenset({"nome_completo", "email", "telefone"})
_WHATSAPP_MSG_MAX = 4000


def _wh_log(message: str, level: int = logging.INFO) -> None:
    """Log com prefixo fixo para grep no painel (ex.: filtro P12_LORENA_WEBHOOK)."""
    logger.log(level, "%s %s", LOG_PREFIX, message)


def _client_ip() -> str:
    """IP do cliente (considera proxy do Easypanel)."""
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    xri = (request.headers.get("X-Real-IP") or "").strip()
    if xri:
        return xri
    return request.remote_addr or "?"


def _leadgen_ids_for_log(bodies: List[Dict[str, Any]]) -> str:
    ids: List[str] = []
    for b in bodies:
        lid = b.get("leadgenId")
        if lid is not None and str(lid).strip():
            ids.append(str(lid).strip())
    return ",".join(ids) if ids else "n/d"


def _load_env() -> None:
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)


def _digits_only(phone: Optional[str]) -> str:
    if not phone:
        return ""
    return re.sub(r"\D", "", str(phone))


def _format_whatsapp_line(raw_phone: Optional[str]) -> str:
    """
    Sempre o número do lead: link wa.me só quando houver dígitos no payload.
    Texto dummy da Meta (sem dígitos) não deve ser trocado por número fixo de teste.

    Opcional: LORENA_FALLBACK_WHATSAPP — só usado se definido e o lead não tiver telefone válido.
    """
    digits = _digits_only(raw_phone)
    if digits:
        return f"https://wa.me/{digits}"
    fallback = (os.getenv("LORENA_FALLBACK_WHATSAPP") or "").strip()
    if fallback:
        return fallback
    return "(não informado)"


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
    """Uma linha por campo: `*nome_campo:* valor` (pergunta em negrito)."""
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
    """Make às vezes envia JSON duplo (string que contém JSON)."""
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
    """Objeto de lead Meta (com ou sem envelope externo)."""
    if not isinstance(d.get("data"), dict):
        return False
    data = d["data"]
    if d.get("leadgenId") is not None:
        return True
    if isinstance(d.get("mappable_field_data"), list):
        return True
    if "email" in data or "nome_completo" in data or "telefone" in data:
        return True
    return False


def _coerce_inner_body(val: Any) -> Optional[Dict[str, Any]]:
    """Campo `body` do envelope pode vir dict ou string JSON."""
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        unwrapped = _unwrap_json_strings(val)
        if isinstance(unwrapped, dict):
            return unwrapped
    return None


def _extract_lead_from_envelope_item(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    if "body" in item:
        inner = _coerce_inner_body(item.get("body"))
        if inner and _is_meta_lead_body(inner):
            return inner
        if isinstance(inner, dict) and isinstance(inner.get("data"), dict):
            return inner
    if _is_meta_lead_body(item):
        return item
    return None


def normalize_lead_bodies(raw: Any) -> List[Dict[str, Any]]:
    """
    Extrai lista de objetos lead (conteúdo de `body` no envelope Make).

    Aceita:
    - array de envelopes `[{ "body": { lead... } }, ...]`
    - objeto envelope único `{ "body": { lead... } }`
    - lead direto `{ "leadgenId", "data", "mappable_field_data" }`
    - `body` como string JSON
    - wrappers opcionais: chave `data` / `items` / `results` com array
    """
    if raw is None:
        return []

    raw = _unwrap_json_strings(raw)

    if isinstance(raw, dict):
        # Não usar a chave "data" aqui: no lead Meta `data` já é o objeto do formulário.
        for wrap_key in ("items", "results", "records", "bundles"):
            inner = raw.get(wrap_key)
            if isinstance(inner, list):
                return normalize_lead_bodies(inner)
            if isinstance(inner, dict):
                sub = normalize_lead_bodies(inner)
                if sub:
                    return sub

        one = _extract_lead_from_envelope_item(raw)
        if one:
            return [one]
        return []

    if isinstance(raw, list):
        out: List[Dict[str, Any]] = []
        for item in raw:
            lead = _extract_lead_from_envelope_item(item)
            if lead:
                out.append(lead)
        return out

    return []


def _mappable_from_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Se o Make não mandar mappable_field_data, monta a partir de `data`."""
    rows: List[Dict[str, Any]] = []
    for k, v in data.items():
        rows.append({"name": str(k), "value": v})
    return rows


def parse_incoming_payload() -> Tuple[Optional[Any], str]:
    """
    Lê JSON do POST com fallbacks (Make varia Content-Type e encoding).

    Returns:
        (parsed, "") em sucesso, ou (None, codigo_erro).
    """
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


def _ensure_mappable(body: Dict[str, Any], data: Dict[str, Any]) -> List[Dict[str, Any]]:
    mappable = body.get("mappable_field_data")
    if isinstance(mappable, list) and len(mappable) > 0:
        return mappable
    return _mappable_from_data(data)


def _resolve_group_id() -> Optional[str]:
    gid = (os.getenv("LORENA_LEAD_GROUP_ID") or "").strip()
    if gid:
        return gid
    clients_path = os.path.join(os.path.dirname(__file__), "..", "clients.json")
    try:
        with open(clients_path, "r", encoding="utf-8") as f:
            clients = json.load(f)
        if isinstance(clients, list):
            for c in clients:
                if not isinstance(c, dict):
                    continue
                if str(c.get("client_name", "")).strip() == "Lorena Carvalho":
                    g = str(c.get("group_id", "")).strip()
                    if g:
                        return g
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Falha ao ler clients.json para fallback de grupo: %s", e)
    return None


def _check_webhook_secret() -> Optional[Tuple[Any, int]]:
    secret = (os.getenv("LORENA_LEAD_WEBHOOK_SECRET") or "").strip()
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


def _format_lead_message(body: Dict[str, Any]) -> str:
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    mappable = _ensure_mappable(body, data)

    nome = _format_field_value(data.get("nome_completo")) or _mappable_lookup(
        mappable, "nome_completo"
    )
    email = _format_field_value(data.get("email")) or _mappable_lookup(mappable, "email")
    telefone_raw = data.get("telefone")
    if telefone_raw is None or telefone_raw == "":
        telefone_raw = _mappable_lookup(mappable, "telefone")
    wa_link = _format_whatsapp_line(telefone_raw)

    respostas = _build_respostas_text(mappable)
    if not respostas:
        respostas = "(nenhuma resposta adicional)"

    msg = (
        f"Nome do Lead: {nome or '(não informado)'}\n"
        f"WhatsApp do Lead: {wa_link}\n"
        f"E-mail do Lead: {email or '(não informado)'}\n"
        f"\n"
        f"==========\n"
        f"\n"
        f"Respostas do Lead:\n"
        f"{respostas}"
    )
    if len(msg) > _WHATSAPP_MSG_MAX:
        cut = _WHATSAPP_MSG_MAX - 20
        msg = msg[:cut] + "\n…(truncado)"
        logger.warning("Mensagem de lead truncada para %s caracteres", _WHATSAPP_MSG_MAX)
    return msg


@app.route("/lorena-new-lead", methods=["POST"])
def lorena_new_lead():
    denied = _check_webhook_secret()
    if denied:
        _wh_log(
            "POST /lorena-new-lead | NEGADO_AUTH | "
            f"ip={_client_ip()} | content_length={request.content_length}",
            level=logging.WARNING,
        )
        return denied

    _wh_log(
        "POST /lorena-new-lead | RECEBIDO | "
        f"ip={_client_ip()} | "
        f"content_type={request.content_type!r} | "
        f"content_length={request.content_length}"
    )

    raw, parse_err = parse_incoming_payload()
    if raw is None:
        ct = request.content_type or ""
        has_form = bool(request.form)
        _wh_log(
            "POST /lorena-new-lead | ERRO_JSON | "
            f"ip={_client_ip()} | content_type={ct!r} | form_keys={list(request.form.keys()) if has_form else []} | "
            f"motivo={parse_err}",
            level=logging.WARNING,
        )
        return jsonify(
            {
                "ok": False,
                "error": parse_err,
                "hint": "Envie JSON (array ou objeto) com lead em body ou lead direto; "
                "string JSON dupla e form field body/payload/data aceitos.",
            }
        ), 400

    bodies = normalize_lead_bodies(raw)
    if not bodies:
        _wh_log(
            "POST /lorena-new-lead | ERRO_PAYLOAD | "
            f"ip={_client_ip()} | tipo_raiz={type(raw).__name__} | "
            "nao foi possivel extrair lead (body ou objeto Meta)",
            level=logging.WARNING,
        )
        return jsonify(
            {
                "ok": False,
                "error": "missing_body",
                "hint": "Esperado: [{...,'body':{leadgenId,data,mappable_field_data}}] ou "
                "{body:{...}} ou lead direto com data + leadgenId/mappable_field_data.",
            }
        ), 400

    _wh_log(
        "POST /lorena-new-lead | PAYLOAD_OK | "
        f"leads={len(bodies)} | leadgen_id={_leadgen_ids_for_log(bodies)}"
    )

    group_id = _resolve_group_id()
    if not group_id:
        _wh_log(
            "ERRO_CONFIG | LORENA_LEAD_GROUP_ID ausente e fallback clients.json falhou",
            level=logging.ERROR,
        )
        return jsonify({"ok": False, "error": "missing_group_id"}), 500

    dry = (os.getenv("DRY_RUN", "false").lower() == "true")
    sent = 0
    errors: List[str] = []

    for i, body in enumerate(bodies):
        try:
            message = _format_lead_message(body)
            if dry:
                _wh_log(
                    f"LEAD_{i} | DRY_RUN | nao enviado WhatsApp | preview_len={len(message)}"
                )
                sent += 1
                continue
            client = get_evolution_client()
            if client.send_text_message(group_id, message):
                _wh_log(
                    f"LEAD_{i} | WHATSAPP_ENVIADO_OK | "
                    f"leadgen_id={body.get('leadgenId', 'n/d')!s}"
                )
                sent += 1
            else:
                _wh_log(
                    f"LEAD_{i} | WHATSAPP_FALHA | Evolution retornou false",
                    level=logging.ERROR,
                )
                errors.append(f"lead_index_{i}: send returned false")
        except Exception as e:
            _wh_log(f"LEAD_{i} | WHATSAPP_EXCECAO | {e!s}", level=logging.ERROR)
            logger.exception("Falha ao enviar lead %s", i)
            errors.append(f"lead_index_{i}: {e!s}")

    if errors:
        _wh_log(
            f"POST /lorena-new-lead | RESPOSTA_500 | sent={sent} | erros={len(errors)}",
            level=logging.ERROR,
        )
        return (
            jsonify({"ok": False, "sent": sent, "errors": errors}),
            500,
        )
    _wh_log(f"POST /lorena-new-lead | CONCLUIDO_OK | sent={sent}")
    return jsonify({"ok": True, "sent": sent})


def main() -> None:
    _load_env()
    port = int(os.getenv("WEBHOOK_PORT", "8080"))
    _wh_log(f"SERVICO_INICIADO | escutando 0.0.0.0:{port} | rota POST /lorena-new-lead")
    # Reduz ruído do Werkzeug no mesmo stream; nossas linhas usam P12_LORENA_WEBHOOK
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
