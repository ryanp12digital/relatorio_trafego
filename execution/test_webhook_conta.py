"""
Busca métricas reais na Meta para uma conta de anúncios e envia um POST ao webhook.

Uso (raiz do repo):
  python execution/test_webhook_conta.py
  python execution/test_webhook_conta.py 535390208581579
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.data_processor import DataProcessor, format_currency, format_number
from execution.meta_client import get_meta_client
from execution.webhook_notify import get_error_webhook_url


def _normalize_act_id(raw: str) -> str:
    s = raw.strip().replace(" ", "")
    if s.startswith("act_"):
        return s
    return f"act_{s}"


def _period_dates_sp() -> tuple[str, str, str, str]:
    # America/Sao_Paulo (UTC-3, sem DST) — evita depender de tzdata/pytz no Windows
    sp = timezone(timedelta(hours=-3))
    now = datetime.now(sp)
    period_a_date = (now - timedelta(days=1)).date()
    period_a_start = period_a_date.strftime("%Y-%m-%d")
    period_a_end = period_a_start
    period_b_date = (now - timedelta(days=2)).date()
    period_b_start = period_b_date.strftime("%Y-%m-%d")
    period_b_end = period_b_start
    return period_a_start, period_a_end, period_b_start, period_b_end


def _champion_webhook(ch: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not ch:
        return None
    m = ch.get("metrics") or {}
    return {
        "ad_name": ch.get("ad_name"),
        "adset_name": ch.get("adset_name"),
        "campaign_name": ch.get("campaign_name"),
        "conversions": ch.get("conversions"),
        "spend": m.get("spend"),
        "cpa": m.get("cpa"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Teste: Meta + webhook com dados da conta.")
    parser.add_argument(
        "ad_account_id",
        nargs="?",
        default="535390208581579",
        help="ID numérico ou act_XXX (default: 535390208581579)",
    )
    args = parser.parse_args()

    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)

    ad_account_id = _normalize_act_id(args.ad_account_id)
    url = get_error_webhook_url()
    print(f"Conta: {ad_account_id}")
    print(f"Webhook: {url}")

    period_a_start, period_a_end, period_b_start, period_b_end = _period_dates_sp()
    print(
        f"Periodo A: {period_a_start} | Periodo B (comparativo): {period_b_start}"
    )

    meta_client = get_meta_client(ad_account_id)
    processor = DataProcessor()

    period_a_insights = meta_client.get_account_insights(period_a_start, period_a_end)
    period_a_ads = meta_client.get_ads_with_insights(period_a_start, period_a_end)
    period_b_insights = meta_client.get_account_insights(period_b_start, period_b_end)

    results = processor.process_periods(
        period_a_insights,
        period_b_insights,
        period_a_ads,
    )
    pa = results["period_a"]
    pb = results["period_b"]

    descricao = (
        f"Teste de envio com dados reais da conta {ad_account_id}. "
        f"Referencia {period_a_start}: investimento {format_currency(pa['spend'])}, "
        f"impressoes {format_number(pa['impressions'])}, cliques {format_number(pa['clicks'])}, "
        f"conversoes {format_number(pa['conversions'])}, CPA {format_currency(pa['cpa'])}. "
        f"Comparativo dia {period_b_start} no payload."
    )

    payload: Dict[str, Any] = {
        "evento": "teste_dados_conta",
        "descricao": descricao,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "origem": "p12_relatorios",
        "ad_account_id": ad_account_id,
        "periodo_a": period_a_start,
        "periodo_b": period_b_start,
        "metricas_periodo_a": {
            "spend": pa["spend"],
            "impressions": pa["impressions"],
            "clicks": pa["clicks"],
            "cpc": pa["cpc"],
            "cpm": pa["cpm"],
            "conversions": pa["conversions"],
            "cpa": pa["cpa"],
        },
        "metricas_periodo_b": {
            "spend": pb["spend"],
            "impressions": pb["impressions"],
            "clicks": pb["clicks"],
            "cpc": pb["cpc"],
            "cpm": pb["cpm"],
            "conversions": pb["conversions"],
            "cpa": pb["cpa"],
        },
        "deltas": results["deltas"],
        "top_criativo": _champion_webhook(results.get("champion")),
    }

    r = requests.post(url, json=payload, timeout=30)
    print(f"HTTP {r.status_code}")
    if r.status_code >= 400:
        print(r.text[:500])


if __name__ == "__main__":
    main()
