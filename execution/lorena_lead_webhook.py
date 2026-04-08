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
    """Uma linha por campo: `nome_campo: valor` (sem negrito)."""
    lines: List[str] = []
    for row in mappable:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if not name or name in _EXCLUDE_RESPOSTAS:
            continue
        val = _format_field_value(row.get("value"))
        lines.append(f"{name}: {val}")
    return "\n".join(lines)


def normalize_lead_bodies(raw: Any) -> List[Dict[str, Any]]:
    """Extrai lista de objetos `body` do payload Make (array envelope ou objeto único)."""
    if raw is None:
        return []
    if isinstance(raw, list):
        out: List[Dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict) and "body" in item and isinstance(item["body"], dict):
                out.append(item["body"])
        return out
    if isinstance(raw, dict):
        body = raw.get("body")
        if isinstance(body, dict):
            return [body]
    return []


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
    mappable = body.get("mappable_field_data")
    if not isinstance(mappable, list):
        mappable = []

    nome = _format_field_value(data.get("nome_completo")) or _mappable_lookup(
        mappable, "nome_completo"
    )
    email = _format_field_value(data.get("email")) or _mappable_lookup(mappable, "email")
    telefone_raw = data.get("telefone")
    if telefone_raw is None or telefone_raw == "":
        telefone_raw = _mappable_lookup(mappable, "telefone")
    digits = _digits_only(telefone_raw)
    wa_link = f"https://wa.me/{digits}" if digits else "(não informado)"

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

    raw = request.get_json(silent=True)
    if raw is None:
        _wh_log(
            "POST /lorena-new-lead | ERRO_JSON | "
            f"ip={_client_ip()} | corpo nao e JSON valido",
            level=logging.WARNING,
        )
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    bodies = normalize_lead_bodies(raw)
    if not bodies:
        _wh_log(
            "POST /lorena-new-lead | ERRO_PAYLOAD | "
            f"ip={_client_ip()} | sem objeto body no envelope (Make)",
            level=logging.WARNING,
        )
        return jsonify({"ok": False, "error": "missing_body", "hint": "expected array of {body: {...}} or {body: {...}}"}), 400

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
