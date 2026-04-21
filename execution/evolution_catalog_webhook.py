"""
Webhook Evolution: catalogar grupos (@g.us) para a Pulseboard.

POST /evolution-webhook — sem sessão Flask; valida EVOLUTION_CATALOG_WEBHOOK_SECRET
(cabeçalhos X-Webhook-Secret / Bearer ou query ?catalog_secret= / ?secret=).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

LOG_PREFIX_EVOLUTION = "[P12_EVOLUTION_CATALOG]"

# Obrigatório para ligar monitoramento de um grupo novo no catálogo (match sem maiúsculas/acentos).
_CATALOG_ACTIVATION_PHRASE = "ativar grupo"


def _emit_catalog_flow(
    agent: str,
    stage: str,
    status: str,
    detail: str,
    *,
    group_id: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    """Observabilidade na Pulseboard (source=catalog_* → aba Grupos WhatsApp)."""
    from execution.live_events import publish_event

    publish_event(
        source=f"catalog_{agent}",
        stage=stage,
        status=status,
        detail=detail,
        client_name="",
        group_id=(group_id or "").strip(),
        payload=payload or {},
    )


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


def expand_evolution_catalog_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    A Evolution/Baileys manda muitas vezes messages.upsert com data.messages[] (várias mensagens).
    Sem isto, key/message no nível data podem estar vazios e o JID do grupo não é detectado.
    """
    out: List[Dict[str, Any]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        data = ev.get("data")
        if not isinstance(data, dict):
            out.append(ev)
            continue
        msgs = data.get("messages")
        if not isinstance(msgs, list) or not msgs:
            out.append(ev)
            continue
        appended = False
        for m in msgs:
            if not isinstance(m, dict):
                continue
            chunk = dict(ev)
            chunk["data"] = {
                "key": m.get("key") if isinstance(m.get("key"), dict) else None,
                "message": m.get("message"),
                "pushName": m.get("pushName") or data.get("pushName") or data.get("push_name"),
            }
            out.append(chunk)
            appended = True
        if not appended:
            out.append(ev)
    return out


def _catalog_activation_phrase() -> str:
    return _CATALOG_ACTIVATION_PHRASE


def _normalize_phrase_match(s: str) -> str:
    s = unicodedata.normalize("NFD", (s or "").lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _collect_message_text_strings(obj: Any, acc: List[str], depth: int) -> None:
    if depth > 10 or len(acc) > 80:
        return
    if isinstance(obj, str):
        t = obj.strip()
        if len(t) >= 1:
            acc.append(t)
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("mentionedJid", "jpegThumbnail", "fileSha256", "mediaKey"):
                continue
            _collect_message_text_strings(v, acc, depth + 1)
    elif isinstance(obj, list):
        for it in obj[:40]:
            _collect_message_text_strings(it, acc, depth + 1)


def extract_plain_text_for_activation(event_body: Dict[str, Any]) -> str:
    """Texto agregado do payload (mensagem própria ou de terceiros) para bater a frase de activação."""
    data = event_body.get("data")
    if not isinstance(data, dict):
        return ""
    parts: List[str] = []
    msg = data.get("message")
    if msg is not None:
        _collect_message_text_strings(msg, parts, 0)
    for key in ("body", "text", "caption", "conversation"):
        v = data.get(key)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    return " ".join(parts)[:8000]


def event_body_contains_activation_phrase(event_body: Dict[str, Any]) -> bool:
    phrase = _normalize_phrase_match(_catalog_activation_phrase())
    blob = _normalize_phrase_match(extract_plain_text_for_activation(event_body))
    return bool(phrase) and phrase in blob


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
    """Extrai JID de grupo de messages.* ou groups.* (inclui mensagens enviadas pela própria instância / fromMe)."""
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
    query_secret: str = "",
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
            "cod=SECRET_CATALOGO_NAO_CONFIGURADO | defina EVOLUTION_CATALOG_WEBHOOK_SECRET no .env "
            "ou EVOLUTION_CATALOG_ALLOW_INSECURE=1 so em dev",
            level=logging.WARNING,
        )
        return False
    hs = (header_secret or "").strip()
    ab = (auth_bearer or "").strip()
    qs = (query_secret or "").strip()
    if hs and hs == expected:
        return True
    if ab and ab == expected:
        return True
    if qs and qs == expected:
        return True
    _evo_log(
        "cod=WEBHOOK_SECRET_CATALOGO_INVALIDO | X-Webhook-Secret, Bearer ou query catalog_secret/secret incorreto",
        level=logging.WARNING,
    )
    return False


def _enrich_group_subject_async(group_jid: str) -> None:
    def run() -> None:
        gj = (group_jid or "").strip()
        try:
            from execution import persistence
            from execution.evolution_client import get_evolution_client

            if not persistence.catalog_group_should_process(gj):
                _emit_catalog_flow(
                    "evolution_api",
                    "ENRICH_SKIP",
                    "info",
                    "Enriquecimento não executado (monitoramento desligado ou grupo ignorado)",
                    group_id=gj,
                )
                return
            _emit_catalog_flow(
                "evolution_api",
                "FETCH_GROUP_INFO",
                "info",
                "A pedir nome/subject à Evolution",
                group_id=gj,
            )
            client = get_evolution_client()
            info = client.fetch_group_info(gj)
            if not info:
                _emit_catalog_flow(
                    "evolution_api",
                    "FETCH_GROUP_INFO_VAZIO",
                    "warning",
                    "Evolution não devolveu dados do grupo",
                    group_id=gj,
                )
                return
            sub = (
                str(info.get("subject") or "")
                or str(info.get("name") or "")
                or str(info.get("groupName") or "")
            ).strip()
            if sub:
                persistence.update_catalog_group_subject(gj, sub)
                preview = sub[:120] + ("..." if len(sub) > 120 else "")
                _emit_catalog_flow(
                    "evolution_api",
                    "SUBJECT_ACTUALIZADO",
                    "ok",
                    f"Nome gravado: {preview}",
                    group_id=gj,
                    payload={"chars": len(sub)},
                )
            else:
                _emit_catalog_flow(
                    "evolution_api",
                    "SUBJECT_VAZIO",
                    "warning",
                    "Resposta da API sem subject/name",
                    group_id=gj,
                )
        except Exception as e:
            _evo_log(f"cod=ENRIQUECIMENTO_SUBJECT_ERRO | group_jid={gj} | err={e!s}", level=logging.WARNING)
            _emit_catalog_flow(
                "evolution_api",
                "ENRICH_ERRO",
                "error",
                str(e)[:500],
                group_id=gj,
            )

    threading.Thread(target=run, name=f"evo-catalog-{group_jid[:20]}", daemon=True).start()


def process_evolution_catalog_payload(
    raw: Any,
    *,
    header_secret: str = "",
    auth_bearer: str = "",
    query_secret: str = "",
) -> Tuple[Dict[str, Any], int]:
    """
    Processa JSON bruto do webhook. Retorna (dict resposta, status_http).
    """
    if not verify_evolution_catalog_webhook_secret(header_secret, auth_bearer, query_secret):
        _emit_catalog_flow(
            "auth",
            "SECRET_NEGADO",
            "error",
            "401 — secret do catálogo inválido ou EVOLUTION_CATALOG_WEBHOOK_SECRET não definido",
            payload={
                "hint": (
                    "Defina EVOLUTION_CATALOG_WEBHOOK_SECRET no .env e na Evolution: URL com "
                    "?catalog_secret=<valor> (ou ?secret=), ou cabeçalhos X-Webhook-Secret / Bearer. "
                    "Codifique caracteres especiais na query (percent-encoding). "
                    "Dev sem secret: EVOLUTION_CATALOG_ALLOW_INSECURE=1."
                ),
            },
        )
        return {
            "ok": False,
            "error": "unauthorized",
            "hint": (
                "Defina EVOLUTION_CATALOG_WEBHOOK_SECRET no servidor e envie o mesmo valor na Evolution: "
                "na URL do webhook use …/evolution-webhook?catalog_secret=<valor> (recomendado quando não há "
                "campos de cabeçalho), ou X-Webhook-Secret / Authorization: Bearer. "
                "Sem secret e sem EVOLUTION_CATALOG_ALLOW_INSECURE=1 o POST responde 401."
            ),
        }, 401

    _emit_catalog_flow("auth", "SECRET_OK", "ok", "Autenticação do webhook do catálogo aceite")

    from execution import persistence

    if not persistence.get_catalog_webhook_listening():
        _evo_log(
            "cod=LISTENER_PAUSADO | canal=catalogo_grupos | escuta desligada na Pulseboard — "
            "HTTP 200 sem processar (menos carga)",
            level=logging.INFO,
        )
        _emit_catalog_flow(
            "listener",
            "PAUSADO",
            "warning",
            "Pedido aceite mas ignorado: escuta do catálogo pausada na Pulseboard (200 sem gravar)",
        )
        return {"ok": True, "ignored": True, "reason": "listener_paused"}, 200

    events = normalize_evolution_events(raw)
    events = expand_evolution_catalog_events(events)
    if not events:
        _evo_log(
            f"cod=EVENTOS_EVOLUTION_VAZIOS | canal=catalogo_grupos | {_payload_shape_evolution(raw)} | "
            f"dica=JSON com event+data ou lista n8n com body.event",
            level=logging.INFO,
        )
        _emit_catalog_flow(
            "parser",
            "SEM_EVENTOS",
            "warning",
            f"Nenhum evento normalizado no JSON ({_payload_shape_evolution(raw)})",
        )
        return {"ok": True, "processed": 0, "skipped": "no_events"}, 200

    fe0 = str(events[0].get("event") or "")[:100]
    _emit_catalog_flow(
        "parser",
        "FILA",
        "info",
        f"{len(events)} evento(s) na fila do catálogo",
        payload={"primeiro_evento": fe0},
    )

    processed = 0
    skipped_no_jid = 0
    first_event_name = ""
    for ev in events:
        gj = extract_group_jid_from_event(ev)
        evname = str(ev.get("event") or "")[:120]
        if not gj:
            skipped_no_jid += 1
            if not first_event_name:
                first_event_name = str(ev.get("event") or "")[:80]
            _emit_catalog_flow(
                "extract",
                "JID_AUSENTE",
                "warning",
                "Evento sem JID de grupo (@g.us)",
                payload={"event": evname},
            )
            continue
        if event_body_contains_activation_phrase(ev):
            persistence.set_catalog_group_monitoring(gj, True)
            _emit_catalog_flow(
                "parser",
                "ACTIVACAO_PALAVRA_CHAVE",
                "ok",
                "Monitoramento ligado no grupo (frase obrigatória «Ativar grupo»)",
                group_id=gj,
            )
        et, push, preview = extract_activity_meta(ev)
        if persistence.upsert_catalog_group_activity(
            gj,
            event_type=et,
            push_name=push,
            preview=preview,
        ):
            processed += 1
            _emit_catalog_flow(
                "store",
                "ACTUALIZADO",
                "ok",
                f"Actividade gravada · {et or 'evento'}",
                group_id=gj,
                payload={"push_name": (push or "")[:80]},
            )
            _enrich_group_subject_async(gj)
            _emit_catalog_flow(
                "evolution_api",
                "ENRICH_AGENDADO",
                "info",
                "Thread de enriquecimento (nome) agendada",
                group_id=gj,
            )
        else:
            _emit_catalog_flow(
                "store",
                "SKIP_MONITORING",
                "warning",
                "Grupo não actualizado (monitoramento desligado ou JID inválido)",
                group_id=gj,
                payload={"event": et[:120] if et else ""},
            )

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
    _emit_catalog_flow(
        "parser",
        "LOTE_OK",
        "ok",
        f"Lote concluído: {processed} grupo(s) actualizado(s) de {len(events)} evento(s)",
        payload={"skipped_no_jid": skipped_no_jid},
    )
    return {"ok": True, "processed": processed, "events": len(events)}, 200
