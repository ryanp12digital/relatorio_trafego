"""
Notificações HTTP para webhook (ex.: n8n) em falhas da automação P12 Relatorios.
Campo origem nos payloads: p12_relatorios.
"""

from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_ERROR_WEBHOOK_URL = (
    "https://n8n-webhook.axmxa0.easypanel.host/webhook/erro-relatorio"
)


def _webhook_url() -> str:
    url = (os.getenv("ERROR_WEBHOOK_URL") or "").strip()
    return url if url else DEFAULT_ERROR_WEBHOOK_URL


def get_error_webhook_url() -> str:
    """URL efetiva do webhook (útil para testes e diagnóstico)."""
    return _webhook_url()


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _post_json(payload: Dict[str, Any]) -> Optional[int]:
    url = _webhook_url()
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        logger.info("Webhook de erro enviado com sucesso (HTTP %s)", r.status_code)
        return r.status_code
    except requests.RequestException as e:
        logger.warning("Falha ao enviar webhook de erro: %s", e)
        return None


def notify_meta_token_expirado(
    descricao: str,
    *,
    meta_error_code: Any = None,
    meta_error_subcode: Optional[int] = None,
    cliente: Optional[str] = None,
    fbtrace_id: Optional[str] = None,
) -> Optional[int]:
    """
    Payload para expiração/invalidação explícita do token (indícios na resposta Meta).
    Retorna o código HTTP em sucesso, ou None se falhou o POST.
    """
    payload: Dict[str, Any] = {
        "evento": "meta_token_expirado",
        "descricao": descricao,
        "timestamp": _now_iso_utc(),
        "origem": "p12_relatorios",
    }
    if meta_error_code is not None:
        payload["meta_error_code"] = meta_error_code
    if meta_error_subcode is not None:
        payload["meta_error_subcode"] = meta_error_subcode
    if cliente:
        payload["cliente"] = cliente
    if fbtrace_id:
        payload["fbtrace_id"] = fbtrace_id
    return _post_json(payload)


def notify_erro_automacao(
    descricao: str,
    *,
    tipo_excecao: Optional[str] = None,
    mensagem: Optional[str] = None,
    traceback_str: Optional[str] = None,
    cliente: Optional[str] = None,
) -> Optional[int]:
    """Payload para qualquer outra falha da automação. Retorna HTTP em sucesso ou None."""
    payload: Dict[str, Any] = {
        "evento": "erro_automacao",
        "descricao": descricao,
        "timestamp": _now_iso_utc(),
        "origem": "p12_relatorios",
    }
    if tipo_excecao:
        payload["tipo_excecao"] = tipo_excecao
    if mensagem:
        payload["mensagem"] = mensagem
    if traceback_str:
        payload["traceback"] = traceback_str[:8000]
    if cliente:
        payload["cliente"] = cliente
    return _post_json(payload)


def notify_exception_as_automation_error(
    exc: BaseException,
    descricao: str,
    *,
    cliente: Optional[str] = None,
) -> Optional[int]:
    """Conveniência: erro genérico com tipo e traceback."""
    return notify_erro_automacao(
        descricao,
        tipo_excecao=type(exc).__name__,
        mensagem=str(exc),
        traceback_str="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        cliente=cliente,
    )
