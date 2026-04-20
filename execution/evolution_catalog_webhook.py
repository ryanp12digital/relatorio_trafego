"""
Webhook Evolution: catalogar grupos (@g.us) para a Pulseboard.

POST /evolution-webhook — sem sessão Flask; valida EVOLUTION_CATALOG_WEBHOOK_SECRET.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

LOG_PREFIX_EVOLUTION = "[P12_EVOLUTION_CATALOG]"


def _evolution_instance_tag() -> str:
    inst = (os.getenv("EVOLUTION_INSTANCE") or "").strip()
    return f"evolution_instance={inst}" if inst else "evolution_instance=(defina EVOLUTION_INSTANCE)"


def _evo_log(message: str, level: int = logging.INFO) -> None:
    logger.log(level, "%s %s | %s", LOG_PREFIX_EVOLUTION, _evolution_instance_tag(), message)


def _payload_shape_evolution(raw: Any, max_keys: int = 16) -> str:
    if raw is None:
        return "shape=tipo_null"
    raw = _unwrap_json_strings(raw)
    if isinstance(raw, list):
        return f"shape=lista[n={len(raw)}]"
    if isinstance(raw, dict):
        keys = ",".join(sorted(str(k) for k in list(raw.keys())[:max_keys]))
        more = "+" if len(raw) > max_keys else ""
        return f"shape=dict keys=[{keys}{more}]"
    return f"shape=tipo_{type(raw).__name__}"


def log_evolution_catalog_warning(cod: str, detail: str = "") -> None:
    """Chamado pela dashboard em falhas de parse antes de process_evolution_catalog_payload."""
    tail = f" | {detail}" if detail else ""
    _evo_log(f"cod={cod}{tail}", level=logging.WARNING)


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


def normalize_evolution_events(raw: Any) -> List[Dict[str, Any]]:
    """Aceita envelope n8n [{body:{event,...}}] ou object directo."""
    raw = _unwrap_json_strings(raw)
    if raw is None:
        return []
    if isinstance(raw, dict):
        if raw.get("event"):
            return [raw]
        body = raw.get("body")
        if isinstance(body, dict) and body.get("event"):
            return [body]
        return []
    if isinstance(raw, list):
        out: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            body = item.get("body")
            if isinstance(body, dict) and body.get("event"):
                out.append(body)
            elif item.get("event"):
                out.append(item)
        return out
    return []


def _is_group_jid(jid: str) -> bool:
    s = (jid or "").strip().lower()
    return bool(s) and s.endswith("@g.us")


def _preview_from_message(msg: Any, max_len: int = 120) -> str:
    if not isinstance(msg, dict):
        return ""
    for key in ("conversation", "extendedTextMessage", "imageMessage", "videoMessage"):
        block = msg.get(key)
        if isinstance(block, dict):
            t = block.get("text") or block.get("caption")
            if isinstance(t, str) and t.strip():
                t = t.strip().replace("\n", " ")
                return t[:max_len] if len(t) > max_len else t
        if isinstance(block, str) and block.strip():
            s = block.strip().replace("\n", " ")
            return s[:max_len] if len(s) > max_len else s
    return ""


def extract_group_jid_from_event(event_body: Dict[str, Any]) -> Optional[str]:
    """Extrai JID de grupo de messages.* ou groups.*."""
    event = str(event_body.get("event") or "").strip()
    data = event_body.get("data")
    if not isinstance(data, dict):
        data = {}

    # messages.upsert / MESSAGES_UPSERT
    if "message" in event.lower() or event in ("messages.upsert", "MESSAGES_UPSERT", "messages.update"):
        key = data.get("key")
        if isinstance(key, dict):
            rj = str(key.get("remoteJid") or "").strip()
            if _is_group_jid(rj):
                return rj
        # alguns payloads aninham message em data.message
        msg = data.get("message")
        if isinstance(msg, dict):
            key2 = msg.get("key")
            if isinstance(key2, dict):
                rj = str(key2.get("remoteJid") or "").strip()
                if _is_group_jid(rj):
                    return rj

    # groups.upsert / GROUP_UPDATE etc.
    for key in ("id", "groupId", "remoteJid", "jid"):
        v = data.get(key)
        if isinstance(v, str) and _is_group_jid(v):
            return v.strip()
    # lista de grupos
    if isinstance(data.get("chats"), list):
        for ch in data["chats"]:
            if isinstance(ch, dict):
                jid = str(ch.get("id") or ch.get("remoteJid") or "").strip()
                if _is_group_jid(jid):
                    return jid
    return None


def extract_activity_meta(event_body: Dict[str, Any]) -> Tuple[str, str, str]:
    event = str(event_body.get("event") or "").strip()
    data = event_body.get("data") if isinstance(event_body.get("data"), dict) else {}
    push = str(data.get("pushName") or data.get("push_name") or "").strip()[:200]
    msg = data.get("message")
    preview = _preview_from_message(msg) if isinstance(msg, dict) else ""
    if not preview and isinstance(data.get("message"), str):
        preview = (data.get("message") or "")[:120]
    return event, push, preview


def verify_evolution_catalog_webhook_secret(
    header_secret: str,
    auth_bearer: str,
) -> bool:
    expected = (os.getenv("EVOLUTION_CATALOG_WEBHOOK_SECRET") or "").strip()
    if not expected:
        allow = (os.getenv("EVOLUTION_CATALOG_ALLOW_INSECURE") or "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        if allow:
            _evo_log(
                "cod=CATALOGO_SEM_SECRET_DEV | "
                "EVOLUTION_CATALOG_ALLOW_INSECURE ativo - nao use em producao",
                level=logging.WARNING,
            )
            return True
        _evo_log(
            "cod=SECRET_CATALOGO_NAO_CONFIGURADO | defina EVOLUTION_CATALOG_WEBHOOK_SECRET na Evolution e no .env",
            level=logging.WARNING,
        )
        return False
    hs = (header_secret or "").strip()
    ab = (auth_bearer or "").strip()
    if hs and hs == expected:
        return True
    if ab and ab == expected:
        return True
    _evo_log("cod=WEBHOOK_SECRET_CATALOGO_INVALIDO | cabecalho X-Webhook-Secret ou Bearer incorreto", level=logging.WARNING)
    return False


def _enrich_group_subject_async(group_jid: str) -> None:
    def run() -> None:
        try:
            from execution import persistence
            from execution.evolution_client import get_evolution_client

            if not persistence.catalog_group_should_process(group_jid):
                return
            client = get_evolution_client()
            info = client.fetch_group_info(group_jid)
            if not info:
                return
            sub = (
                str(info.get("subject") or "")
                or str(info.get("name") or "")
                or str(info.get("groupName") or "")
            ).strip()
            if sub:
                persistence.update_catalog_group_subject(group_jid, sub)
        except Exception as e:
            _evo_log(f"cod=ENRIQUECIMENTO_SUBJECT_ERRO | group_jid={group_jid} | err={e!s}", level=logging.WARNING)

    threading.Thread(target=run, name=f"evo-catalog-{group_jid[:20]}", daemon=True).start()


def process_evolution_catalog_payload(
    raw: Any,
    *,
    header_secret: str = "",
    auth_bearer: str = "",
) -> Tuple[Dict[str, Any], int]:
    """
    Processa JSON bruto do webhook. Retorna (dict resposta, status_http).
    """
    if not verify_evolution_catalog_webhook_secret(header_secret, auth_bearer):
        return {"ok": False, "error": "unauthorized"}, 401

    events = normalize_evolution_events(raw)
    if not events:
        _evo_log(
            f"cod=EVENTOS_EVOLUTION_VAZIOS | canal=catalogo_grupos | {_payload_shape_evolution(raw)} | "
            f"dica=JSON com event+data ou lista n8n com body.event",
            level=logging.INFO,
        )
        return {"ok": True, "processed": 0, "skipped": "no_events"}, 200

    from execution import persistence

    processed = 0
    skipped_no_jid = 0
    first_event_name = ""
    for ev in events:
        gj = extract_group_jid_from_event(ev)
        if not gj:
            skipped_no_jid += 1
            if not first_event_name:
                first_event_name = str(ev.get("event") or "")[:80]
            continue
        et, push, preview = extract_activity_meta(ev)
        if persistence.upsert_catalog_group_activity(
            gj,
            event_type=et,
            push_name=push,
            preview=preview,
        ):
            processed += 1
            _enrich_group_subject_async(gj)

    if skipped_no_jid:
        _evo_log(
            f"cod=EVENTOS_SEM_JID_GRUPO | canal=catalogo_grupos | quantidade={skipped_no_jid} | "
            f"exemplo_event={first_event_name!r} | dica=remoteJid terminado em @g.us",
            level=logging.INFO,
        )
    _evo_log(
        f"cod=OK_CATALOGO_GRUPOS | canal=catalogo_grupos | eventos={len(events)} | grupos_actualizados={processed}",
        level=logging.INFO,
    )
    return {"ok": True, "processed": processed, "events": len(events)}, 200
