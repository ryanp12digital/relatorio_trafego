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
            logger.warning(
                "Webhook catálogo Evolution sem EVOLUTION_CATALOG_WEBHOOK_SECRET (EVOLUTION_CATALOG_ALLOW_INSECURE)"
            )
            return True
        logger.warning("Webhook catálogo Evolution rejeitado: defina EVOLUTION_CATALOG_WEBHOOK_SECRET")
        return False
    hs = (header_secret or "").strip()
    ab = (auth_bearer or "").strip()
    if hs and hs == expected:
        return True
    if ab and ab == expected:
        return True
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
            logger.warning("Enriquecimento subject grupo %s: %s", group_jid, e)

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
        return {"ok": True, "processed": 0, "skipped": "no_events"}, 200

    from execution import persistence

    processed = 0
    for ev in events:
        gj = extract_group_jid_from_event(ev)
        if not gj:
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

    return {"ok": True, "processed": processed, "events": len(events)}, 200
