"""
Relatorio Google Ads -> WhatsApp (multi-client) usando google_clients.json.

Observacao:
- Descobre automaticamente as conversoes primarias da conta.
- Coleta metricas reais via Google Ads API (GAQL + OAuth refresh token).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.data_processor import format_currency, format_number
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


def _date_iso_to_br(iso_date: str) -> str:
    try:
        d = datetime.strptime(iso_date, "%Y-%m-%d").date()
        return d.strftime("%d/%m/%Y")
    except ValueError:
        return iso_date


def _format_percent(value: float) -> str:
    return f"{value:.2f}%".replace(".", ",")


def _normalize_customer_id(customer_id: str) -> str:
    digits = "".join(ch for ch in (customer_id or "") if ch.isdigit())
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return customer_id.strip()


def _period_dates_last_7_days() -> Tuple[str, str]:
    now = datetime.now(timezone.utc)
    yesterday = (now - timedelta(days=1)).date()
    start = yesterday - timedelta(days=6)
    return start.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")


def _load_google_clients() -> List[Dict[str, Any]]:
    clients_path = os.path.join(os.path.dirname(__file__), "..", "google_clients.json")
    with open(clients_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("google_clients.json deve conter uma lista")
    return [item for item in data if isinstance(item, dict)]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_digits(value: str) -> str:
    return "".join(ch for ch in (value or "") if ch.isdigit())


def _format_quantity(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return format_number(int(round(value)))
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


class GoogleAdsAPIError(Exception):
    """Erro de integração com Google Ads API."""


class GoogleAdsAPIClient:
    def __init__(self) -> None:
        self.client_id = (os.getenv("GOOGLE_ADS_CLIENT_ID") or "").strip()
        self.client_secret = (os.getenv("GOOGLE_ADS_CLIENT_SECRET") or "").strip()
        self.refresh_token = (os.getenv("GOOGLE_ADS_REFRESH_TOKEN") or "").strip()
        self.developer_token = (os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip()
        self.login_customer_id = _safe_digits((os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or "").strip())
        self.api_version = (os.getenv("GOOGLE_ADS_API_VERSION") or "v20").strip()
        self.base_url = f"https://googleads.googleapis.com/{self.api_version}"

        missing = []
        if not self.client_id:
            missing.append("GOOGLE_ADS_CLIENT_ID")
        if not self.client_secret:
            missing.append("GOOGLE_ADS_CLIENT_SECRET")
        if not self.refresh_token:
            missing.append("GOOGLE_ADS_REFRESH_TOKEN")
        if not self.developer_token:
            missing.append("GOOGLE_ADS_DEVELOPER_TOKEN")
        if missing:
            raise GoogleAdsAPIError(f"Variaveis ausentes para Google Ads API: {', '.join(missing)}")

        self._access_token: Optional[str] = None

    def _fetch_access_token(self) -> str:
        token_url = "https://oauth2.googleapis.com/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        try:
            response = requests.post(token_url, data=payload, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise GoogleAdsAPIError(f"Falha ao renovar access token Google: {exc}") from exc

        data = response.json()
        token = str(data.get("access_token", "")).strip()
        if not token:
            raise GoogleAdsAPIError("Resposta OAuth sem access_token.")
        self._access_token = token
        return token

    def _headers(self) -> Dict[str, str]:
        token = self._access_token or self._fetch_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "developer-token": self.developer_token,
            "Content-Type": "application/json",
        }
        if self.login_customer_id:
            headers["login-customer-id"] = self.login_customer_id
        return headers

    def search_stream(self, customer_id: str, query: str) -> List[Dict[str, Any]]:
        cid = _safe_digits(customer_id)
        if not cid:
            raise GoogleAdsAPIError(f"customer_id invalido: {customer_id!r}")

        endpoint = f"{self.base_url}/customers/{cid}/googleAds:searchStream"
        payload = {"query": query}
        headers = self._headers()

        def _do_request(hdrs: Dict[str, str]) -> requests.Response:
            return requests.post(endpoint, headers=hdrs, json=payload, timeout=60)

        try:
            response = _do_request(headers)
        except requests.RequestException as exc:
            raise GoogleAdsAPIError(f"Erro de rede em searchStream: {exc}") from exc

        if response.status_code == 401:
            # refresh token expirado/desautorizado: tenta renovar uma vez
            headers = self._headers()
            response = _do_request(headers)

        if response.status_code >= 400:
            body = response.text[:500]
            raise GoogleAdsAPIError(
                f"Google Ads searchStream falhou ({response.status_code}): {body}"
            )

        try:
            chunks = response.json()
        except ValueError as exc:
            raise GoogleAdsAPIError(f"Resposta invalida da Google Ads API: {exc}") from exc

        if not isinstance(chunks, list):
            raise GoogleAdsAPIError("Resposta searchStream fora do formato esperado (lista).")

        rows: List[Dict[str, Any]] = []
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            results = chunk.get("results")
            if not isinstance(results, list):
                continue
            rows.extend(r for r in results if isinstance(r, dict))
        return rows


def _primary_conversions_from_api(
    api_client: GoogleAdsAPIClient,
    customer_id: str,
) -> Dict[str, str]:
    query = """
        SELECT
          conversion_action.resource_name,
          conversion_action.name,
          conversion_action.status,
          conversion_action.primary_for_goal
        FROM conversion_action
        WHERE conversion_action.status != 'REMOVED'
          AND conversion_action.primary_for_goal = TRUE
    """
    rows = api_client.search_stream(customer_id, query)
    primary: Dict[str, str] = {}
    for row in rows:
        conv = row.get("conversionAction") or {}
        if not isinstance(conv, dict):
            continue
        resource_name = str(conv.get("resourceName", "")).strip()
        name = str(conv.get("name", "")).strip()
        if resource_name and name:
            primary[resource_name] = name
    return primary


def _base_metrics_from_api(
    api_client: GoogleAdsAPIClient,
    customer_id: str,
    period_start: str,
    period_end: str,
) -> Dict[str, Any]:
    query = f"""
        SELECT
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.conversions,
          metrics.cost_per_conversion,
          metrics.cost_micros
        FROM customer
        WHERE segments.date BETWEEN '{period_start}' AND '{period_end}'
    """
    rows = api_client.search_stream(customer_id, query)
    if not rows:
        return {
            "impressions": 0,
            "clicks": 0,
            "ctr": 0.0,
            "total_conversions": 0.0,
            "cost_per_conversion": 0.0,
            "total_spend": 0.0,
        }

    metrics = rows[0].get("metrics") or {}
    if not isinstance(metrics, dict):
        metrics = {}

    cost_micros = _safe_float(metrics.get("costMicros"))
    total_spend = cost_micros / 1_000_000.0
    total_conversions = _safe_float(metrics.get("conversions"))
    ctr_raw = _safe_float(metrics.get("ctr"))
    ctr_percent = ctr_raw * 100.0 if 0.0 <= ctr_raw <= 1.0 else ctr_raw
    cost_per_conversion = (total_spend / total_conversions) if total_conversions > 0 else 0.0
    return {
        "impressions": _safe_int(metrics.get("impressions")),
        "clicks": _safe_int(metrics.get("clicks")),
        "ctr": ctr_percent,
        "total_conversions": total_conversions,
        "cost_per_conversion": cost_per_conversion,
        "total_spend": total_spend,
    }


def _campaign_metrics_from_api(
    api_client: GoogleAdsAPIClient,
    customer_id: str,
    period_start: str,
    period_end: str,
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.conversions,
          metrics.cost_micros
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND segments.date BETWEEN '{period_start}' AND '{period_end}'
    """
    rows = api_client.search_stream(customer_id, query)
    campaigns: List[Dict[str, Any]] = []
    for row in rows:
        campaign = row.get("campaign") or {}
        metrics = row.get("metrics") or {}
        if not isinstance(campaign, dict) or not isinstance(metrics, dict):
            continue
        cost_micros = _safe_float(metrics.get("costMicros"))
        spend = cost_micros / 1_000_000.0
        conversions = _safe_float(metrics.get("conversions"))
        ctr_raw = _safe_float(metrics.get("ctr"))
        ctr_percent = ctr_raw * 100.0 if 0.0 <= ctr_raw <= 1.0 else ctr_raw
        entry = {
            "campaign_id": str(campaign.get("id", "")).strip(),
            "campaign_name": str(campaign.get("name", "Campanha sem nome")).strip() or "Campanha sem nome",
            "impressions": _safe_int(metrics.get("impressions")),
            "clicks": _safe_int(metrics.get("clicks")),
            "ctr": ctr_percent,
            "total_conversions": conversions,
            "total_spend": spend,
            "cost_per_conversion": (spend / conversions) if conversions > 0 else 0.0,
        }
        if (
            entry["impressions"] > 0
            or entry["clicks"] > 0
            or entry["total_conversions"] > 0
            or entry["total_spend"] > 0
        ):
            campaigns.append(entry)

    campaigns.sort(key=lambda item: (_safe_float(item["total_spend"]), _safe_float(item["total_conversions"])), reverse=True)
    return campaigns


def _primary_conversion_metrics_from_api(
    api_client: GoogleAdsAPIClient,
    customer_id: str,
    period_start: str,
    period_end: str,
) -> Dict[str, float]:
    query = f"""
        SELECT
          segments.conversion_action,
          metrics.conversions
        FROM customer
        WHERE segments.date BETWEEN '{period_start}' AND '{period_end}'
    """
    rows = api_client.search_stream(customer_id, query)
    by_resource: Dict[str, float] = {}
    for row in rows:
        segment = row.get("segments") or {}
        metrics = row.get("metrics") or {}
        if not isinstance(segment, dict) or not isinstance(metrics, dict):
            continue
        resource_name = str(segment.get("conversionAction", "")).strip()
        if not resource_name:
            continue
        by_resource[resource_name] = by_resource.get(resource_name, 0.0) + _safe_float(
            metrics.get("conversions")
        )
    return by_resource


def _build_default_metrics(primary_conversions: List[str]) -> Dict[str, Any]:
    conversions = {name: 0 for name in primary_conversions}
    return {
        "impressions": 0,
        "clicks": 0,
        "ctr": 0.0,
        "primary_conversions": conversions,
        "total_conversions": 0,
        "cost_per_conversion": 0.0,
        "total_spend": 0.0,
    }


def _collect_google_metrics(
    api_client: GoogleAdsAPIClient,
    customer_id: str,
    fallback_primary_conversions: List[str],
    period_start: str,
    period_end: str,
) -> Dict[str, Any]:
    primary_by_resource = _primary_conversions_from_api(api_client, customer_id)
    if not primary_by_resource and fallback_primary_conversions:
        logger.warning(
            "Conta %s sem conversoes primarias retornadas na API; usando fallback do google_clients.json.",
            customer_id,
        )
        base = _build_default_metrics(fallback_primary_conversions)
        campaigns = _campaign_metrics_from_api(
            api_client=api_client,
            customer_id=customer_id,
            period_start=period_start,
            period_end=period_end,
        )
        base.update(
            _base_metrics_from_api(
                api_client=api_client,
                customer_id=customer_id,
                period_start=period_start,
                period_end=period_end,
            )
        )
        base["campaigns"] = campaigns
        return base

    primary_metrics_by_resource = _primary_conversion_metrics_from_api(
        api_client=api_client,
        customer_id=customer_id,
        period_start=period_start,
        period_end=period_end,
    )
    base_metrics = _base_metrics_from_api(
        api_client=api_client,
        customer_id=customer_id,
        period_start=period_start,
        period_end=period_end,
    )

    primary_metrics_named: Dict[str, float] = {}
    for resource_name, conversion_name in primary_by_resource.items():
        primary_metrics_named[conversion_name] = primary_metrics_by_resource.get(resource_name, 0.0)

    if not primary_metrics_named and fallback_primary_conversions:
        primary_metrics_named = {name: 0.0 for name in fallback_primary_conversions}

    campaigns = _campaign_metrics_from_api(
        api_client=api_client,
        customer_id=customer_id,
        period_start=period_start,
        period_end=period_end,
    )

    return {
        "impressions": base_metrics["impressions"],
        "clicks": base_metrics["clicks"],
        "ctr": base_metrics["ctr"],
        "primary_conversions": primary_metrics_named,
        "total_conversions": base_metrics["total_conversions"],
        "cost_per_conversion": base_metrics["cost_per_conversion"],
        "total_spend": base_metrics["total_spend"],
        "campaigns": campaigns,
    }


def _build_google_report_message(
    client_name: str,
    customer_id: str,
    period_start: str,
    period_end: str,
    metrics: Dict[str, Any],
) -> str:
    conversion_lines = []
    for conv_name, conv_value in metrics.get("primary_conversions", {}).items():
        conversion_lines.append(f"- {conv_name}: {_format_quantity(_safe_float(conv_value))}")

    conversions_block = "\n".join(conversion_lines) if conversion_lines else "- (nenhuma conversao primaria configurada)"
    campaigns = metrics.get("campaigns") or []
    if not isinstance(campaigns, list):
        campaigns = []

    max_campaigns = _safe_int(os.getenv("GOOGLE_REPORT_MAX_CAMPAIGNS", "8"), 8)
    if max_campaigns <= 0:
        max_campaigns = 8
    shown_campaigns = campaigns[:max_campaigns]
    hidden_campaigns = max(0, len(campaigns) - len(shown_campaigns))

    campaign_lines: List[str] = []
    for idx, campaign in enumerate(shown_campaigns, start=1):
        if not isinstance(campaign, dict):
            continue
        campaign_lines.append(
            (
                f"\n{idx}) *{campaign.get('campaign_name', 'Campanha')}*\n"
                f"👁️ Impressoes: {format_number(_safe_int(campaign.get('impressions')))}\n"
                f"🖱️ Cliques: {format_number(_safe_int(campaign.get('clicks')))}\n"
                f"📈 CTR: {_format_percent(_safe_float(campaign.get('ctr')))}\n"
                f"🔢 Total de conversoes: {_format_quantity(_safe_float(campaign.get('total_conversions')))}\n"
                f"💸 Custo por conversao: {format_currency(_safe_float(campaign.get('cost_per_conversion')))}\n"
                f"💰 Investimento total: {format_currency(_safe_float(campaign.get('total_spend')))}"
            )
        )

    campaigns_block = (
        "".join(campaign_lines) if campaign_lines else "\n- Nenhuma campanha ativa com dados no periodo."
    )
    if hidden_campaigns > 0:
        campaigns_block += f"\n\n... e mais {hidden_campaigns} campanha(s) ativa(s)."

    return (
        f"*{client_name}*\n\n"
        f"📊 *Relatorio Google Ads*\n"
        f"🆔 *Conta:* {_normalize_customer_id(customer_id)}\n"
        f"📅 *Periodo (7 dias):* {_date_iso_to_br(period_start)} a {_date_iso_to_br(period_end)}\n\n"
        f"🎯 *Conversoes primarias:*\n"
        f"{conversions_block}\n\n"
        f"📌 *Campanhas ativas (metricas por campanha):*"
        f"{campaigns_block}"
    )


def run_google_reports(*, force_send_zero: bool = False, only_customer_id: Optional[str] = None) -> bool:
    clients = _load_google_clients()
    enabled = [c for c in clients if c.get("enabled", True)]
    if only_customer_id:
        digits = "".join(ch for ch in only_customer_id if ch.isdigit())
        enabled = [
            c
            for c in enabled
            if "".join(ch for ch in str(c.get("google_customer_id", "")) if ch.isdigit()) == digits
        ]

    if not enabled:
        logger.warning("Nenhum cliente Google habilitado para envio.")
        return False

    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    evolution = get_evolution_client()
    google_api = GoogleAdsAPIClient()
    period_start, period_end = _period_dates_last_7_days()

    success_count = 0
    for client in enabled:
        client_name = str(client.get("client_name", "Cliente Google")).strip()
        customer_id = str(client.get("google_customer_id", "")).strip()
        group_id = str(client.get("group_id", "")).strip()
        primary_conversions = client.get("primary_conversions") or []
        if not isinstance(primary_conversions, list):
            primary_conversions = []
        primary_conversions = [str(item).strip() for item in primary_conversions if str(item).strip()]

        if not customer_id or not group_id:
            logger.warning("Cliente %s ignorado por falta de google_customer_id/group_id.", client_name)
            continue

        try:
            metrics = _collect_google_metrics(
                api_client=google_api,
                customer_id=customer_id,
                fallback_primary_conversions=primary_conversions,
                period_start=period_start,
                period_end=period_end,
            )
        except GoogleAdsAPIError as exc:
            logger.error(
                "Falha ao consultar Google Ads para %s (%s): %s",
                client_name,
                _normalize_customer_id(customer_id),
                exc,
            )
            continue
        if (
            not force_send_zero
            and not any(
                _safe_float((c or {}).get("total_spend")) > 0
                or _safe_float((c or {}).get("total_conversions")) > 0
                for c in (metrics.get("campaigns") or [])
            )
        ):
            logger.info("Cliente %s sem atividade nas campanhas ativas. Pulando envio.", client_name)
            continue

        message = _build_google_report_message(client_name, customer_id, period_start, period_end, metrics)

        if dry_run:
            file_name = "".join(ch for ch in customer_id if ch.isdigit()) or "google"
            report_path = os.path.join(log_dir, f"google_report_{file_name}.md")
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(message + "\n")
            logger.info("DRY_RUN ativo: relatorio salvo em %s", report_path)
            success_count += 1
            continue

        if evolution.send_text_message(group_id, message):
            logger.info("Relatorio Google enviado para %s (%s).", client_name, group_id)
            success_count += 1
        else:
            logger.error("Falha ao enviar relatorio Google para %s.", client_name)

    return success_count > 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Envia relatorio Google Ads para grupos WhatsApp.")
    parser.add_argument("--force-send-zero", action="store_true", help="Envia mesmo com spend/conversoes zerados.")
    parser.add_argument("--customer-id", type=str, default="", help="Envia apenas para um customer_id especifico.")
    args = parser.parse_args()

    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)

    success = run_google_reports(
        force_send_zero=args.force_send_zero,
        only_customer_id=args.customer_id or None,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
