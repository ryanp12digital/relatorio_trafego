"""
Orquestrador principal do P12 Relatorios (Meta Ads via WhatsApp).

Este script coordena a coleta de dados, processamento, formatação e envio
do relatório semanal de performance do Meta Ads (últimos 7 dias vs semana anterior).
"""

import os
import sys
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.meta_client import get_meta_client, list_business_ad_accounts, MetaAPIAuthError
from execution.evolution_client import get_evolution_client
from execution.data_processor import DataProcessor, format_currency, format_number
from execution.webhook_notify import (
    notify_erro_automacao,
    notify_exception_as_automation_error,
    notify_meta_token_expirado,
)
from execution.project_paths import clients_json_path

# Configuração de logging
log_dir = os.path.join(os.path.dirname(__file__), '..', '.tmp')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'execution.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class P12RelatoriosReporter:
    """
    Classe principal para geração e envio de relatórios P12 Relatorios.

    Mensagens ao cliente: texto curto, direto, com data e métricas (negrito estilo WhatsApp).
    """
    
    def __init__(self):
        """Inicializa o reporter P12 Relatorios."""
        self._webhook_token_expiry_sent = False
        self._webhook_meta_auth_other_sent = False
        try:
            self.access_token = os.getenv('META_ACCESS_TOKEN')
            if not self.access_token:
                raise ValueError("META_ACCESS_TOKEN não configurada no .env")
            
            self.business_id = (os.getenv('META_BUSINESS_ID') or '').strip()
            if not self.business_id:
                logger.warning(
                    "META_BUSINESS_ID ausente no .env: generate_and_send_report() multi-client "
                    "nao funcionara; generate_and_send_report_for_client() continua disponivel."
                )
            
            self.evolution_client = get_evolution_client()
            self.data_processor = DataProcessor()
            self.dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
                
        except Exception as e:
            logger.error(f"Erro ao inicializar P12RelatoriosReporter: {str(e)}")
            raise
    
    def get_period_dates(self, account_timezone_name: Optional[str] = None) -> tuple[str, str, str, str]:
        """
        Período A: últimos 7 dias completos terminando em ontem (janela móvel, 7 dias).
        Período B: os 7 dias imediatamente anteriores ao período A.

        Usa o timezone da conta Meta quando informado; senão DEFAULT_REPORT_TIMEZONE ou America/Sao_Paulo.

        Returns:
            (period_a_start, period_a_end, period_b_start, period_b_end) em YYYY-MM-DD
        """
        resolved_timezone = account_timezone_name or os.getenv("DEFAULT_REPORT_TIMEZONE", "America/Sao_Paulo")
        try:
            tz = ZoneInfo(resolved_timezone)
            now = datetime.now(tz)
            logger.info(
                f"Data/hora atual ({resolved_timezone}): {now.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        except ZoneInfoNotFoundError:
            tz_fallback = timezone(timedelta(hours=-3))
            now = datetime.now(tz_fallback)
            logger.warning(
                f"Timezone '{resolved_timezone}' não encontrada. Usando fallback UTC-3."
            )
            logger.info(f"Data/hora atual (fallback UTC-3): {now.strftime('%Y-%m-%d %H:%M:%S')}")

        yesterday = (now - timedelta(days=1)).date()
        period_a_end = yesterday
        period_a_start = yesterday - timedelta(days=6)

        period_b_end = period_a_start - timedelta(days=1)
        period_b_start = period_b_end - timedelta(days=6)

        ps_a, pe_a = period_a_start.strftime("%Y-%m-%d"), period_a_end.strftime("%Y-%m-%d")
        ps_b, pe_b = period_b_start.strftime("%Y-%m-%d"), period_b_end.strftime("%Y-%m-%d")

        logger.info(f"Período A (7 dias até ontem): {ps_a} a {pe_a}")
        logger.info(f"Período B (7 dias comparativo): {ps_b} a {pe_b}")

        return ps_a, pe_a, ps_b, pe_b

    @staticmethod
    def is_scheduled_weekly_report_day() -> bool:
        """
        Relatório enviado só às segundas-feiras (timezone DEFAULT_REPORT_TIMEZONE).
        Defina FORCE_WEEKLY_REPORT=1 para ignorar (testes manuais).
        """
        if os.getenv("FORCE_WEEKLY_REPORT", "").lower() in ("1", "true", "yes"):
            return True
        tz_name = os.getenv("DEFAULT_REPORT_TIMEZONE", "America/Sao_Paulo")
        try:
            tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            tz = timezone(timedelta(hours=-3))
        now = datetime.now(tz)
        return now.weekday() == 0
    
    def _notify_meta_auth_webhook(self, e: MetaAPIAuthError, cliente: Optional[str] = None) -> None:
        """Envia webhook de token expirado ou erro de auth Meta (no máximo um de cada tipo por execução)."""
        if e.is_token_expiry_event:
            if self._webhook_token_expiry_sent:
                return
            self._webhook_token_expiry_sent = True
            descricao = (
                "A Meta Marketing API indicou expiração ou invalidação da sessão do access token. "
                "Renovar META_ACCESS_TOKEN (Meta Business / Graph API Explorer). "
                f"Detalhe: código {e.error_code}, subcódigo {e.error_subcode}: {e}"
            )
            notify_meta_token_expirado(
                descricao,
                meta_error_code=e.error_code,
                meta_error_subcode=e.error_subcode,
                cliente=cliente,
                fbtrace_id=e.fbtrace_id,
            )
        else:
            if self._webhook_meta_auth_other_sent:
                return
            self._webhook_meta_auth_other_sent = True
            descricao = (
                "Falha de autenticação na Meta (token ou permissões) sem indício explícito de expiração de sessão. "
                f"Cliente: {cliente or 'N/A'}. Detalhe: {e}"
            )
            notify_erro_automacao(
                descricao,
                tipo_excecao="MetaAPIAuthError",
                mensagem=str(e),
                cliente=cliente,
            )
    
    def _detect_conversion_types(self, insights: List[Dict[str, Any]], ads: List[Dict[str, Any]]) -> List[str]:
        """
        Detecta quais tipos de conversão existem nos dados.
        
        Args:
            insights: Lista de insights do período
            ads: Lista de anúncios com insights do período
            
        Returns:
            Lista com 'Lead', 'WhatsApp' ou ambos
        """
        conversion_types = set()
        
        # Verifica nos insights
        for insight in insights:
            actions = insight.get('actions', [])
            for action in actions:
                action_type = action.get('action_type', '').lower()
                value = int(action.get('value', 0))
                if value > 0:  # Só conta se tiver conversões
                    if 'lead' in action_type:
                        conversion_types.add('Lead')
                    elif 'whatsapp' in action_type or 'whats_app' in action_type:
                        conversion_types.add('WhatsApp')
        
        # Verifica nos ads (caso insights não tenham)
        for ad in ads:
            ad_insights = ad.get('insights', {}).get('data', [])
            for insight in ad_insights:
                actions = insight.get('actions', [])
                for action in actions:
                    action_type = action.get('action_type', '').lower()
                    value = int(action.get('value', 0))
                    if value > 0:  # Só conta se tiver conversões
                        if 'lead' in action_type:
                            conversion_types.add('Lead')
                        elif 'whatsapp' in action_type or 'whats_app' in action_type:
                            conversion_types.add('WhatsApp')
        
        return sorted(list(conversion_types))  # Retorna ordenado
    
    @staticmethod
    def _date_iso_to_br(iso_date: str) -> str:
        """Converte YYYY-MM-DD para DD/MM/AAAA."""
        try:
            d = datetime.strptime(iso_date, "%Y-%m-%d").date()
            return d.strftime("%d/%m/%Y")
        except (ValueError, TypeError):
            return iso_date

    @staticmethod
    def _conversion_label_from_types(conversion_types: Optional[List[str]]) -> str:
        """Rótulo da linha Conversão; sem tipo detectado usa 0 (pedido do cliente)."""
        if not conversion_types:
            return "0"
        if len(conversion_types) == 2:
            return "Lead + WhatsApp"
        return conversion_types[0]

    def _format_metric_lines(self, metrics: Dict[str, Any], conversion_label: str) -> str:
        """Linhas comuns de métricas (valores ausentes tratados como zero)."""
        spend = float(metrics.get("spend", 0) or 0)
        impressions = int(metrics.get("impressions", 0) or 0)
        clicks = int(metrics.get("clicks", 0) or 0)
        cpc = float(metrics.get("cpc", 0) or 0)
        conversions = int(metrics.get("conversions", 0) or 0)
        cpa = float(metrics.get("cpa", 0) or 0)
        return (
            f"💰 *Investimento:* {format_currency(spend)}\n"
            f"👁️ *Impressões:* {format_number(impressions)}\n"
            f"🖱️ *Cliques:* {format_number(clicks)}\n"
            f"💸 *CPC:* {format_currency(cpc)}\n"
            f"📌 *Conversão:* {conversion_label}\n"
            f"🎯 *Resultados:* {format_number(conversions)}\n"
            f"📉 *CPA:* {format_currency(cpa)}\n"
        )
    
    def format_absolute_report(
        self,
        metrics: Dict[str, float],
        period_start: str,
        period_end: str,
        account_name: str = "",
        conversion_types: Optional[List[str]] = None,
    ) -> str:
        """
        Relatório semanal (período A): bloco principal com intervalo de 7 dias.
        """
        if account_name:
            report = f"*{account_name}*\n\n"
        else:
            report = "*Relatório Meta Ads*\n\n"

        report += (
            f"📊 *Relatório semanal*\n"
            f"📅 *Período (7 dias):* {self._date_iso_to_br(period_start)} a {self._date_iso_to_br(period_end)}\n"
        )
        conversion_label = self._conversion_label_from_types(conversion_types)
        report += self._format_metric_lines(metrics, conversion_label)

        return report

    def format_comparative_report(
        self,
        metrics_b: Dict[str, float],
        period_b_start: str,
        period_b_end: str,
        account_name: str = "",
        conversion_types_b: Optional[List[str]] = None,
    ) -> str:
        """
        Métricas do período B (semana anterior, 7 dias).
        """
        if account_name:
            report = f"*Comparativo — {account_name}*\n\n"
        else:
            report = "*Comparativo*\n\n"

        report += (
            f"📅 *Semana anterior (7 dias):* "
            f"{self._date_iso_to_br(period_b_start)} a {self._date_iso_to_br(period_b_end)}\n"
        )
        label_b = self._conversion_label_from_types(conversion_types_b)
        report += self._format_metric_lines(metrics_b, label_b)

        return report
    
    def generate_and_send_report_for_client(
        self,
        client_name: str,
        ad_account_id: str,
        group_id: str,
        *,
        send_if_zero_spend: bool = False,
    ) -> bool:
        """
        Gera e envia o relatório (7 dias + comparativo semana anterior) em uma única mensagem WhatsApp.
        
        Args:
            client_name: Nome do cliente
            ad_account_id: ID da conta de anúncios (formato: act_XXXXXXXX)
            group_id: ID do grupo WhatsApp para envio
            send_if_zero_spend: Se True, envia mesmo com spend ~0 (ex.: envio manual)
            
        Returns:
            True se o relatório foi gerado/enviado com sucesso, False caso contrário
        """
        try:
            logger.info(f"Gerando relatório para cliente: {client_name} ({ad_account_id})")
            
            # Cria cliente para esta conta específica
            meta_client = get_meta_client(ad_account_id)
            
            # Calcula períodos
            account_timezone_name = meta_client.get_account_timezone_name()
            period_a_start, period_a_end, period_b_start, period_b_end = self.get_period_dates(
                account_timezone_name
            )
            
            # Coleta dados da Meta API
            logger.info(f"Coletando dados do período atual para {client_name}...")
            period_a_insights = meta_client.get_account_insights(period_a_start, period_a_end)
            period_a_ads = meta_client.get_ads_with_insights(period_a_start, period_a_end)
            
            logger.info(f"Coletando dados do período comparativo para {client_name}...")
            period_b_insights = meta_client.get_account_insights(period_b_start, period_b_end)
            
            # Processa dados (mesmo que não haja spend, gera relatório com valores zero)
            logger.info(f"Processando dados para {client_name}...")
            results = self.data_processor.process_periods(
                period_a_insights,
                period_b_insights,
                period_a_ads
            )
            
            # Sem spend no período: no cron não envia; envio manual pode forçar
            if results["period_a"]["spend"] <= 0.01 and not send_if_zero_spend:
                logger.info(
                    f"Cliente {client_name} sem investimento no período. Mensagem não será enviada."
                )
                return True
            
            # Detecta tipos de conversão (A: insights + ads; B: só insights de conta)
            conversion_types = self._detect_conversion_types(period_a_insights, period_a_ads)
            conversion_types_b = self._detect_conversion_types(period_b_insights, [])
            
            # Formata mensagens
            message_1 = self.format_absolute_report(
                results["period_a"],
                period_a_start,
                period_a_end,
                client_name,
                conversion_types,
            )
            message_2 = self.format_comparative_report(
                results["period_b"],
                period_b_start,
                period_b_end,
                client_name,
                conversion_types_b,
            )

            full_message = f"{message_1.rstrip()}\n\n{message_2.rstrip()}"
            
            # Modo DRY_RUN: salva em arquivo ao invés de enviar
            if self.dry_run:
                log_dir = os.path.join(os.path.dirname(__file__), '..', '.tmp')
                os.makedirs(log_dir, exist_ok=True)
                report_file = os.path.join(log_dir, f'report_{ad_account_id.replace("act_", "")}.md')
                
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write(f"# Relatório {client_name}\n\n")
                    f.write(full_message)
                    f.write("\n")
                
                logger.info(f"DRY_RUN: Relatório salvo em {report_file}")
                return True
            
            logger.info(f"Enviando relatório (única mensagem) via WhatsApp para {client_name}...")
            if self.evolution_client.send_text_message(group_id, full_message):
                logger.info(f"Relatório enviado com sucesso para {client_name}")
                return True
            logger.error(f"Falha ao enviar relatório para {client_name}")
            return False
                
        except MetaAPIAuthError as e:
            logger.error(f"Erro de autenticação Meta para {client_name}: {str(e)}")
            self._notify_meta_auth_webhook(e, cliente=client_name)
            return False
        except ValueError as e:
            logger.error(f"Erro de validação para {client_name}: {str(e)}")
            notify_exception_as_automation_error(
                e,
                f"Erro de validação ao gerar relatório para o cliente {client_name}.",
                cliente=client_name,
            )
            return False
        except Exception as e:
            logger.error(f"Erro ao gerar relatório para {client_name}: {str(e)}", exc_info=True)
            notify_exception_as_automation_error(
                e,
                f"Erro ao gerar ou enviar relatório para o cliente {client_name}.",
                cliente=client_name,
            )
            return False
    
    def load_clients_config(self) -> List[Dict[str, Any]]:
        """
        Carrega configuração de clientes do Postgres (DATABASE_URL) ou data/clients.json.
        """
        try:
            from execution.persistence import db_enabled, ensure_db_ready, list_meta_clients

            if db_enabled():
                ensure_db_ready()
                rows = list_meta_clients()
                return [{k: v for k, v in r.items() if k != "id"} for r in rows]
        except Exception:
            pass

        clients_path = clients_json_path()

        if not os.path.exists(clients_path):
            raise FileNotFoundError(f"Arquivo clients.json não encontrado em {clients_path}")

        try:
            with open(clients_path, "r", encoding="utf-8") as f:
                clients = json.load(f)

            if not isinstance(clients, list):
                raise ValueError("clients.json deve conter uma lista de clientes")

            return clients
        except json.JSONDecodeError as e:
            raise ValueError(f"Erro ao parsear clients.json: {str(e)}")
    
    def generate_and_send_report(self) -> bool:
        """
        Gera e envia relatórios para todos os clientes habilitados (Postgres ou data/clients.json).
        
        Returns:
            True se pelo menos um relatório foi enviado com sucesso, False caso contrário
        """
        try:
            logger.info("Iniciando geração de relatórios P12 Relatorios (multi-client, modo semanal)")
            if not self.is_scheduled_weekly_report_day():
                logger.info(
                    "Relatório semanal: hoje não é segunda-feira (DEFAULT_REPORT_TIMEZONE). "
                    "Nenhum envio. Use FORCE_WEEKLY_REPORT=1 para forçar."
                )
                return True
            if not self.business_id:
                logger.error("META_BUSINESS_ID é obrigatória para generate_and_send_report().")
                notify_erro_automacao(
                    "Fluxo multi-client abortado: META_BUSINESS_ID nao configurada no .env.",
                    tipo_excecao="ConfigurationError",
                    mensagem="Defina META_BUSINESS_ID para listar contas do Business.",
                )
                return False
            logger.info(f"Business ID: {self.business_id}")
            
            if self.dry_run:
                logger.info("Modo DRY_RUN ativado - relatórios serão salvos em .tmp/ ao invés de enviados")
            
            # Carrega configuração de clientes
            clients_config = self.load_clients_config()
            logger.info(f"Carregados {len(clients_config)} cliente(s) da configuracao")
            
            # Busca todas as contas de anúncios do Business
            logger.info("Buscando contas de anúncios do Business...")
            max_retries = int(os.getenv('MAX_RETRIES', '3'))
            try:
                business_accounts = list_business_ad_accounts(
                    self.access_token, self.business_id, max_retries
                )
            except MetaAPIAuthError as e:
                logger.error(f"Erro de autenticação Meta ao listar contas do Business: {e}")
                self._notify_meta_auth_webhook(e, cliente=None)
                return False
            
            if not business_accounts:
                logger.warning("Nenhuma conta de anúncios encontrada no Business")
                return False
            
            # Cria mapeamento de IDs de contas para validação
            business_account_ids = set()
            for account in business_accounts:
                account_id = account.get('id') or account.get('account_id')
                if account_id:
                    # Normaliza para formato act_XXXXXXXX
                    if not account_id.startswith('act_'):
                        if account_id.isdigit():
                            account_id = f"act_{account_id}"
                        else:
                            continue
                    business_account_ids.add(account_id)
            
            logger.info(f"Encontradas {len(business_account_ids)} conta(s) de anúncios no Business")
            
            # Filtra apenas clientes habilitados
            enabled_clients = [c for c in clients_config if c.get('enabled', True)]
            logger.info(f"Processando {len(enabled_clients)} cliente(s) habilitado(s)")
            
            # Gera relatório para cada cliente habilitado
            success_count = 0
            failed_count = 0
            
            for idx, client in enumerate(enabled_clients):
                client_name = client.get('client_name', 'Sem nome')
                ad_account_id = client.get('ad_account_id', '')
                group_id = client.get('group_id', '')
                enabled = client.get('enabled', True)
                
                if not enabled:
                    logger.info(f"Cliente {client_name} está desabilitado. Pulando...")
                    continue
                
                if not ad_account_id:
                    logger.warning(f"Cliente {client_name} sem ad_account_id. Pulando...")
                    failed_count += 1
                    continue
                
                if not group_id:
                    logger.warning(f"Cliente {client_name} sem group_id. Pulando...")
                    failed_count += 1
                    continue
                
                # Normaliza formato do ad_account_id
                if not ad_account_id.startswith('act_'):
                    if ad_account_id.isdigit():
                        ad_account_id = f"act_{ad_account_id}"
                    else:
                        logger.warning(f"Formato de ad_account_id inválido para {client_name}: {ad_account_id}")
                        failed_count += 1
                        continue
                
                # Valida se a conta existe no Business
                if ad_account_id not in business_account_ids:
                    logger.warning(f"Conta {ad_account_id} do cliente {client_name} não encontrada no Business ou não acessível")
                    failed_count += 1
                    continue
                
                # Gera e envia relatório para este cliente
                try:
                    success = self.generate_and_send_report_for_client(
                        client_name,
                        ad_account_id,
                        group_id
                    )
                    
                    if success:
                        success_count += 1
                        # Aguarda um pouco entre clientes para evitar rate limiting
                        time.sleep(2)
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.error(f"Erro ao processar cliente {client_name}: {str(e)}", exc_info=True)
                    notify_exception_as_automation_error(
                        e,
                        f"Erro inesperado ao processar o cliente {client_name} no loop principal.",
                        cliente=client_name,
                    )
                    failed_count += 1
                    # Continua para o próximo cliente (falha isolada)
                    continue
            
            # Log de contas sem mapeamento
            mapped_account_ids = {client.get('ad_account_id', '').replace('act_', '') 
                                  for client in enabled_clients 
                                  if client.get('ad_account_id', '')}
            unmapped_accounts = [acc for acc in business_account_ids 
                                 if acc.replace('act_', '') not in mapped_account_ids]
            
            if unmapped_accounts:
                logger.info(f"Contas sem mapeamento no clients.json: {', '.join(unmapped_accounts)}")
            
            logger.info(f"Processamento concluído: {success_count} sucesso(s), {failed_count} falha(s)")
            
            # Retorna True se pelo menos um cliente foi processado com sucesso
            return success_count > 0
                
        except FileNotFoundError as e:
            logger.error(f"Erro ao carregar configuração: {str(e)}")
            notify_exception_as_automation_error(
                e,
                "Arquivo de configuração necessário não encontrado (data/clients.json ou path inválido).",
            )
            return False
        except Exception as e:
            logger.error(f"Erro ao gerar relatórios: {str(e)}", exc_info=True)
            notify_exception_as_automation_error(
                e,
                "Erro inesperado durante generate_and_send_report (após inicialização).",
            )
            return False


def main():
    """Função principal para execução via cron."""
    try:
        # Carrega variáveis de ambiente
        from dotenv import load_dotenv
        env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(env_path)
        
        reporter = P12RelatoriosReporter()
        success = reporter.generate_and_send_report()
        
        if success:
            logger.info("Execução concluída com sucesso")
            sys.exit(0)
        else:
            logger.error("Execução concluída com falhas")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Erro fatal na execução: {str(e)}", exc_info=True)
        notify_exception_as_automation_error(
            e,
            "Erro fatal na execução do main_scheduler (antes de concluir ou fora do fluxo normal).",
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

# Nome de classe legado (imports antigos)
NextNousReporter = P12RelatoriosReporter
