"""
Orquestrador principal do sistema Next Nous.

Este script coordena a coleta de dados, processamento, formatação e envio
do relatório diário de performance do Meta Ads.
"""

import os
import sys
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import pytz

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.meta_client import get_meta_client, list_business_ad_accounts
from execution.evolution_client import get_evolution_client
from execution.data_processor import DataProcessor, format_currency, format_number

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


class NextNousReporter:
    """
    Classe principal para geração e envio de relatórios Next Nous.
    
    Persona: Fusão entre eficiência analítica (Jarvis) e sobriedade (Alfred).
    Tom: Formal, minimalista, preciso e levemente irônico.
    Formatação: Apenas Markdown (negrito, listas). Sem emojis.
    """
    
    def __init__(self):
        """Inicializa o reporter Next Nous."""
        try:
            self.access_token = os.getenv('META_ACCESS_TOKEN')
            if not self.access_token:
                raise ValueError("META_ACCESS_TOKEN não configurada no .env")
            
            self.business_id = os.getenv('META_BUSINESS_ID')
            if not self.business_id:
                raise ValueError("META_BUSINESS_ID não configurada no .env")
            
            self.evolution_client = get_evolution_client()
            self.data_processor = DataProcessor()
            self.dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
                
        except Exception as e:
            logger.error(f"Erro ao inicializar NextNousReporter: {str(e)}")
            raise
    
    def get_period_dates(self) -> tuple[str, str, str, str]:
        """
        Calcula as datas dos períodos A e B (dia anterior completo e dia antes disso).
        
        Busca por dia completo ao invés de 24h, validando timezone de São Paulo
        para garantir que está buscando o dia correto.
        
        Returns:
            Tupla com (period_a_start, period_a_end, period_b_start, period_b_end)
            Todas no formato YYYY-MM-DD (mesmo dia para start e end = dia completo)
        """
        # Usa timezone de São Paulo para garantir data correta
        tz_sp = pytz.timezone('America/Sao_Paulo')
        now = datetime.now(tz_sp)
        
        # Valida data e hora atual
        logger.info(f"Data/hora atual (São Paulo): {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        # Período A: dia anterior completo
        period_a_date = (now - timedelta(days=1)).date()
        period_a_start = period_a_date.strftime('%Y-%m-%d')
        period_a_end = period_a_date.strftime('%Y-%m-%d')  # Mesmo dia = dia completo
        
        # Período B: dia anterior ao Período A (dia antes do dia anterior)
        period_b_date = (now - timedelta(days=2)).date()
        period_b_start = period_b_date.strftime('%Y-%m-%d')
        period_b_end = period_b_date.strftime('%Y-%m-%d')  # Mesmo dia = dia completo
        
        logger.info(f"Período A (dia anterior): {period_a_start}")
        logger.info(f"Período B (comparativo): {period_b_start}")
        
        return period_a_start, period_a_end, period_b_start, period_b_end
    
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
    
    def format_absolute_report(self, metrics: Dict[str, float], account_name: str = "", conversion_types: List[str] = None) -> str:
        """
        Formata relatório de performance absoluta - Protocolo de Vigilância (tom real/cortesão).
        
        Args:
            metrics: Métricas agregadas do período
            account_name: Nome do cliente (opcional)
            conversion_types: Lista de tipos de conversão encontrados ['Lead', 'WhatsApp'] (opcional)
            
        Returns:
            Mensagem formatada no estilo real/cortesão com humor
        """
        report = "👑 PROTOCOLO DE VIGILÂNCIA"
        if account_name:
            report += f": {account_name}"
        report += "\n\n"
        
        report += "Caros membros da corte, os escribas reais completaram o levantamento dos tesouros do dia anterior. Eis os resultados consolidados:\n\n"
        
        report += f"💰 Investimento: `{format_currency(metrics['spend'])}`\n"
        report += f"👁️ Alcance Visual: `{format_number(metrics['impressions'])}` impressões\n"
        report += f"🖱️ Engajamento: `{format_number(metrics['clicks'])}` cliques\n"
        report += f"💸 Custo p/ Clique: `{format_currency(metrics['cpc'])}`\n"
        
        # Formata tipos de conversão
        if conversion_types:
            if len(conversion_types) == 2:
                conversion_label = "(Lead + WhatsApp)"
            elif len(conversion_types) == 1:
                conversion_label = f"({conversion_types[0]})"
            else:
                conversion_label = "(Lead + WhatsApp)"  # Padrão
        else:
            conversion_label = "(Lead + WhatsApp)"  # Padrão
        
        report += f"🎯 Alvos Convertidos: `{format_number(metrics['conversions'])}` {conversion_label}\n"
        report += f"📉 Custo p/ Alvo (CPA): `{format_currency(metrics['cpa'])}`\n"
        
        report += "\n🖥️ Os livros foram revisados. A integridade dos registros está confirmada."
        
        return report
    
    def format_comparative_report(
        self,
        metrics_a: Dict[str, float],
        metrics_b: Dict[str, float],
        deltas: Dict[str, str],
        champion: Dict[str, Any] | None,
        account_name: str = ""
    ) -> str:
        """
        Formata relatório comparativo - Análise Tática.
        
        Args:
            metrics_a: Métricas do período atual
            metrics_b: Métricas do período comparativo
            deltas: Variações percentuais (strings formatadas)
            champion: Informações do criativo campeão
            account_name: Nome do cliente (opcional, não usado)
            
        Returns:
            Mensagem formatada no estilo Batman/Next Nous
        """
        def get_delta_emoji(delta_str: str) -> str:
            """Retorna emoji baseado no sinal da variação."""
            if delta_str == "Novo Volume" or delta_str.startswith("+"):
                return "📈"
            else:
                return "📉"
        
        report = "🖥️ ANÁLISE TÁTICA: RELATÓRIO DA CORTE\n\n"
        report += "Comparando com o dia anterior, nossos conselheiros identificaram os seguintes movimentos no reino:\n\n"
        
        # Investimento
        delta_spend = deltas['spend']
        report += f"📊 Investimento: `{delta_spend}` {get_delta_emoji(delta_spend)}\n"
        
        # Cliques
        delta_clicks = deltas['clicks']
        report += f"🖱️ Cliques: `{delta_clicks}` {get_delta_emoji(delta_clicks)}\n"
        
        # Conversões
        delta_conversions = deltas['conversions']
        report += f"🎯 Conversões: `{delta_conversions}` {get_delta_emoji(delta_conversions)}\n"
        
        # CPA (com lógica especial para redução de custo)
        delta_cpa = deltas['cpa']
        cpa_emoji = get_delta_emoji(delta_cpa)
        cpa_comment = ""
        # Para CPA, negativo (redução) é bom, positivo (aumento) é ruim
        if delta_cpa != "Novo Volume" and delta_cpa.startswith("-"):
            cpa_comment = " (Excelente economia, Vossa Excelência)"
        report += f"💸 CPA: `{delta_cpa}` {cpa_emoji}{cpa_comment}\n"
        
        # TOP CREATIVE
        report += "\n"
        if champion:
            report += f"🎯 DESTAQUE DA CORTE (TOP CREATIVE):\n"
            report += f"🏷️ Criativo: `{champion['ad_name']}`\n"
            report += f"📁 Conjunto: `{champion['adset_name']}`\n"
            report += f"📁 Campanha: `{champion['campaign_name']}`\n"
        else:
            report += "🎯 DESTAQUE DA CORTE (TOP CREATIVE): Nenhum anúncio com conversões identificado.\n"
        
        report += "\n👑 A vigilância continua. Próxima inspeção em 24 horas."
        
        return report
    
    def generate_and_send_report_for_client(
        self,
        client_name: str,
        ad_account_id: str,
        group_id: str
    ) -> bool:
        """
        Gera e envia o relatório completo para um cliente específico.
        
        Args:
            client_name: Nome do cliente
            ad_account_id: ID da conta de anúncios (formato: act_XXXXXXXX)
            group_id: ID do grupo WhatsApp para envio
            
        Returns:
            True se o relatório foi gerado/enviado com sucesso, False caso contrário
        """
        try:
            logger.info(f"Gerando relatório para cliente: {client_name} ({ad_account_id})")
            
            # Cria cliente para esta conta específica
            meta_client = get_meta_client(ad_account_id)
            
            # Calcula períodos
            period_a_start, period_a_end, period_b_start, period_b_end = self.get_period_dates()
            
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
            
            # Verifica se há investimento (evita enviar mensagens vazias)
            if results['period_a']['spend'] <= 0.01:  # Tolerância para arredondamento
                logger.info(f"Cliente {client_name} sem investimento no período. Mensagem não será enviada.")
                return True  # Não é erro, apenas não há dados para enviar
            
            # Detecta tipos de conversão encontrados
            conversion_types = self._detect_conversion_types(period_a_insights, period_a_ads)
            
            # Formata mensagens
            message_1 = self.format_absolute_report(results['period_a'], client_name, conversion_types)
            message_2 = self.format_comparative_report(
                results['period_a'],
                results['period_b'],
                results['deltas'],
                results['champion'],
                client_name
            )
            
            # Modo DRY_RUN: salva em arquivo ao invés de enviar
            if self.dry_run:
                log_dir = os.path.join(os.path.dirname(__file__), '..', '.tmp')
                os.makedirs(log_dir, exist_ok=True)
                report_file = os.path.join(log_dir, f'report_{ad_account_id.replace("act_", "")}.md')
                
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write(f"# Relatório {client_name}\n\n")
                    f.write("## Mensagem 1\n\n")
                    f.write(message_1)
                    f.write("\n\n## Mensagem 2\n\n")
                    f.write(message_2)
                
                logger.info(f"DRY_RUN: Relatório salvo em {report_file}")
                return True
            
            # Envia mensagens via WhatsApp
            logger.info(f"Enviando mensagens via WhatsApp para {client_name}...")
            success_1 = self.evolution_client.send_text_message(group_id, message_1)
            
            if success_1:
                # Aguarda um momento antes de enviar a segunda mensagem
                time.sleep(2)
                success_2 = self.evolution_client.send_text_message(group_id, message_2)
                
                if success_2:
                    logger.info(f"Relatório enviado com sucesso para {client_name}")
                    return True
                else:
                    logger.error(f"Falha ao enviar segunda mensagem do relatório para {client_name}")
                    return False
            else:
                logger.error(f"Falha ao enviar primeira mensagem do relatório para {client_name}")
                return False
                
        except ValueError as e:
            logger.error(f"Erro de autenticação para {client_name}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Erro ao gerar relatório para {client_name}: {str(e)}", exc_info=True)
            return False
    
    def load_clients_config(self) -> List[Dict[str, Any]]:
        """
        Carrega configuração de clientes do arquivo clients.json.
        
        Returns:
            Lista de clientes configurados
            
        Raises:
            FileNotFoundError: Se clients.json não existir
            ValueError: Se formato do JSON estiver inválido
        """
        clients_path = os.path.join(os.path.dirname(__file__), '..', 'clients.json')
        
        if not os.path.exists(clients_path):
            raise FileNotFoundError(f"Arquivo clients.json não encontrado em {clients_path}")
        
        try:
            with open(clients_path, 'r', encoding='utf-8') as f:
                clients = json.load(f)
            
            if not isinstance(clients, list):
                raise ValueError("clients.json deve conter uma lista de clientes")
            
            return clients
        except json.JSONDecodeError as e:
            raise ValueError(f"Erro ao parsear clients.json: {str(e)}")
    
    def generate_and_send_report(self) -> bool:
        """
        Gera e envia relatórios para todos os clientes habilitados em clients.json.
        
        Returns:
            True se pelo menos um relatório foi enviado com sucesso, False caso contrário
        """
        try:
            logger.info("Iniciando geração de relatórios Next Nous multi-client")
            logger.info(f"Business ID: {self.business_id}")
            
            if self.dry_run:
                logger.info("Modo DRY_RUN ativado - relatórios serão salvos em .tmp/ ao invés de enviados")
            
            # Carrega configuração de clientes
            clients_config = self.load_clients_config()
            logger.info(f"Carregados {len(clients_config)} cliente(s) do clients.json")
            
            # Busca todas as contas de anúncios do Business
            logger.info("Buscando contas de anúncios do Business...")
            max_retries = int(os.getenv('MAX_RETRIES', '3'))
            business_accounts = list_business_ad_accounts(self.access_token, self.business_id, max_retries)
            
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
            return False
        except Exception as e:
            logger.error(f"Erro ao gerar relatórios: {str(e)}", exc_info=True)
            return False


def main():
    """Função principal para execução via cron."""
    try:
        # Carrega variáveis de ambiente
        from dotenv import load_dotenv
        env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(env_path)
        
        reporter = NextNousReporter()
        success = reporter.generate_and_send_report()
        
        if success:
            logger.info("Execução concluída com sucesso")
            sys.exit(0)
        else:
            logger.error("Execução concluída com falhas")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Erro fatal na execução: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
