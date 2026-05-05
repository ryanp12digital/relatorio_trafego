"""Comparação de segredos em tempo constante (webhooks, tokens em cabeçalho)."""

from __future__ import annotations

import hmac


def constant_time_str_equal(a: str, b: str) -> bool:
    """True só se as strings forem idênticas; evita == em segredos (timing)."""
    if not a or not b:
        return False
    if len(a) != len(b):
        return False
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))
