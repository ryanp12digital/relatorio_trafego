"""
Cliente para conexão com Meta Marketing API (Graph API).

Este módulo implementa funções determinísticas para buscar dados de campanhas,
anúncios e métricas da Meta Ads, com tratamento de erros e retries.
"""

import os
import requests
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode

# Configuração de logging
log_file = os.path.join(os.path.dirname(__file__), '..', '.tmp', 'execution.log')
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MetaAPIClient:
    """Cliente para interação com Meta Marketing API."""
    
    BASE_URL = "https://graph.facebook.com/v18.0"
    
    def __init__(self, access_token: str, ad_account_id: str, max_retries: int = 3):
        """
        Inicializa o cliente Meta API.
        
        Args:
            access_token: Token de acesso da Meta API
            ad_account_id: ID da conta de anúncios (formato: act_XXXXXXXX)
            max_retries: Número máximo de tentativas em caso de erro
        """
        self.access_token = access_token
        self.ad_account_id = ad_account_id
        self.max_retries = max_retries
        
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Realiza uma requisição à API da Meta com retry automático.
        
        Args:
            endpoint: Endpoint da API (sem a base URL)
            params: Parâmetros da requisição
            
        Returns:
            Resposta JSON da API
            
        Raises:
            requests.RequestException: Em caso de erro na requisição após todas as tentativas
            ValueError: Em caso de erro de autenticação
        """
        params['access_token'] = self.access_token
        url = f"{self.BASE_URL}/{endpoint}"
        
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # Verifica erros na resposta JSON
                if 'error' in data:
                    error = data['error']
                    error_code = error.get('code', 'UNKNOWN')
                    error_message = error.get('message', 'Unknown error')
                    
                    # Erro de autenticação (código 190 ou 200)
                    if error_code in [190, 200]:
                        logger.error(f"Erro de autenticação Meta API: {error_message}")
                        raise ValueError(f"Falha de autenticação Meta API: {error_message}")
                    
                    # Outros erros
                    logger.warning(f"Erro Meta API (tentativa {attempt}/{self.max_retries}): {error_message}")
                    if attempt < self.max_retries:
                        time.sleep(5)
                        continue
                    else:
                        raise requests.RequestException(f"Erro Meta API após {self.max_retries} tentativas: {error_message}")
                
                return data
                
            except requests.Timeout:
                logger.warning(f"Timeout na requisição Meta API (tentativa {attempt}/{self.max_retries})")
                if attempt < self.max_retries:
                    time.sleep(5)
                    continue
                else:
                    raise
                    
            except requests.RequestException as e:
                logger.warning(f"Erro na requisição Meta API (tentativa {attempt}/{self.max_retries}): {str(e)}")
                if attempt < self.max_retries:
                    time.sleep(5)
                    continue
                else:
                    raise
    
    def _paginate_request(self, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Realiza requisição paginada à API da Meta.
        
        Args:
            endpoint: Endpoint da API
            params: Parâmetros da requisição
            
        Returns:
            Lista de todos os resultados paginados
        """
        all_data = []
        params = params.copy()
        
        while True:
            data = self._make_request(endpoint, params)
            
            if 'data' in data:
                all_data.extend(data['data'])
            
            # Verifica se há próxima página
            paging = data.get('paging', {})
            next_url = paging.get('next')
            
            if not next_url:
                break
            
            # Extrai os parâmetros da URL da próxima página
            params = {}
            if '?' in next_url:
                query_string = next_url.split('?')[1]
                params = dict(param.split('=') for param in query_string.split('&') if '=' in param)
            
            time.sleep(1)  # Rate limiting
        
        return all_data
    
    def get_account_insights(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Busca insights no nível de conta de anúncios.
        
        Args:
            start_date: Data inicial no formato YYYY-MM-DD
            end_date: Data final no formato YYYY-MM-DD
            
        Returns:
            Lista de insights agregados por período
        """
        endpoint = f"{self.ad_account_id}/insights"
        params = {
            'level': 'account',
            'fields': 'spend,impressions,clicks,actions,cpc,cpm',
            'time_range': f"{{\"since\":\"{start_date}\",\"until\":\"{end_date}\"}}",
            'time_increment': 1
        }
        
        logger.info(f"Buscando insights de conta: {start_date} até {end_date}")
        return self._paginate_request(endpoint, params)
    
    def get_ads_with_insights(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Busca anúncios com seus insights detalhados.
        
        Inclui apenas anúncios de campanhas com spend > 0, independentemente do status.
        
        Args:
            start_date: Data inicial no formato YYYY-MM-DD
            end_date: Data final no formato YYYY-MM-DD
            
        Returns:
            Lista de anúncios com insights e informações hierárquicas
        """
        # Primeiro busca campanhas com spend > 0
        campaigns_endpoint = f"{self.ad_account_id}/campaigns"
        campaigns_params = {
            'fields': 'id,name,status,insights{spend}',
            'level': 'campaign',
            'limit': 1000
        }
        
        logger.info(f"Buscando campanhas com spend no período: {start_date} até {end_date}")
        campaigns = self._paginate_request(campaigns_endpoint, campaigns_params)
        
        # Filtra campanhas com spend > 0
        campaigns_with_spend = []
        for campaign in campaigns:
            insights = campaign.get('insights', {}).get('data', [])
            if insights:
                total_spend = sum(float(insight.get('spend', 0)) for insight in insights)
                if total_spend > 0:
                    campaigns_with_spend.append(campaign['id'])
        
        # Busca anúncios das campanhas selecionadas com insights
        ads_data = []
        for campaign_id in campaigns_with_spend:
            ads_endpoint = f"{campaign_id}/ads"
            ads_params = {
                'fields': 'id,name,adset{id,name,campaign{id,name}},insights{spend,impressions,clicks,actions,cpc,cpm}',
                'limit': 1000
            }
            
            ads = self._paginate_request(ads_endpoint, ads_params)
            
            # Adiciona informações de período aos insights
            for ad in ads:
                if 'insights' in ad and 'data' in ad['insights']:
                    for insight in ad['insights']['data']:
                        insight['start_date'] = start_date
                        insight['end_date'] = end_date
                ads_data.append(ad)
            
            time.sleep(1)  # Rate limiting
        
        logger.info(f"Encontrados {len(ads_data)} anúncios com spend > 0")
        return ads_data


def list_business_ad_accounts(access_token: str, business_id: str, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    Lista todas as contas de anúncios de um Business Manager.
    
    Busca tanto contas próprias (owned_ad_accounts) quanto contas de clientes/parceiros
    (client_ad_accounts) e combina os resultados, removendo duplicatas.
    
    Args:
        access_token: Token de acesso da Meta API
        business_id: ID do Business Manager (formato numérico)
        max_retries: Número máximo de tentativas em caso de erro
        
    Returns:
        Lista de contas de anúncios com informações básicas (id, name)
        Formato: [{"id": "act_XXXXXXXX", "name": "Nome da Conta"}, ...]
    """
    BASE_URL = "https://graph.facebook.com/v18.0"
    
    def _fetch_accounts_from_endpoint(endpoint_path: str) -> List[Dict[str, Any]]:
        """Busca contas de um endpoint específico com paginação."""
        accounts = []
        endpoint = f"{business_id}/{endpoint_path}"
        params = {
            'access_token': access_token,
            'fields': 'id,name,account_id,account_status',
            'limit': 1000
        }
        
        url = f"{BASE_URL}/{endpoint}"
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if 'error' in data:
                    error = data['error']
                    error_code = error.get('code', 'UNKNOWN')
                    error_message = error.get('message', 'Unknown error')
                    
                    if error_code in [190, 200]:
                        logger.error(f"Erro de autenticação ao buscar contas ({endpoint_path}): {error_message}")
                        raise ValueError(f"Falha de autenticação: {error_message}")
                    
                    # Se erro não for de autenticação, retorna lista vazia (endpoint pode não existir)
                    logger.warning(f"Erro ao buscar contas ({endpoint_path}): {error_message}")
                    return accounts
                
                # Processa paginação
                while True:
                    if 'data' in data:
                        accounts.extend(data['data'])
                    
                    paging = data.get('paging', {})
                    next_url = paging.get('next')
                    
                    if not next_url:
                        break
                    
                    params = {}
                    if '?' in next_url:
                        query_string = next_url.split('?')[1]
                        params = dict(param.split('=') for param in query_string.split('&') if '=' in param)
                        params['access_token'] = access_token
                    
                    response = requests.get(next_url, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    time.sleep(1)
                
                break
                
            except ValueError:
                # Erro de autenticação, propaga
                raise
            except requests.RequestException as e:
                if attempt < max_retries:
                    logger.warning(f"Erro ao buscar contas ({endpoint_path}) (tentativa {attempt}/{max_retries}): {str(e)}")
                    time.sleep(5)
                    continue
                else:
                    logger.warning(f"Falha ao buscar contas ({endpoint_path}): {str(e)}")
                    return accounts
        
        return accounts
    
    # Busca contas próprias (owned_ad_accounts)
    logger.info(f"Buscando contas próprias (owned_ad_accounts) do Business {business_id}...")
    owned_accounts = _fetch_accounts_from_endpoint('owned_ad_accounts')
    logger.info(f"Encontradas {len(owned_accounts)} conta(s) própria(s)")
    
    # Busca contas de clientes/parceiros (client_ad_accounts)
    logger.info(f"Buscando contas de clientes/parceiros (client_ad_accounts) do Business {business_id}...")
    client_accounts = _fetch_accounts_from_endpoint('client_ad_accounts')
    logger.info(f"Encontradas {len(client_accounts)} conta(s) de cliente(s)/parceiro(s)")
    
    # Combina ambas as listas e remove duplicatas por ID
    all_accounts = []
    seen_ids = set()
    
    for account in owned_accounts + client_accounts:
        account_id = account.get('id') or account.get('account_id')
        if account_id and account_id not in seen_ids:
            seen_ids.add(account_id)
            all_accounts.append(account)
    
    logger.info(f"Total de {len(all_accounts)} conta(s) de anúncios únicas no Business {business_id} ({len(owned_accounts)} próprias + {len(client_accounts)} clientes)")
    return all_accounts


def get_ad_accounts_from_portfolio(access_token: str, portfolio_id: str, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    Busca todas as contas de anúncios vinculadas a um portfolio.
    
    Args:
        access_token: Token de acesso da Meta API
        portfolio_id: ID do portfolio/business (formato numérico)
        max_retries: Número máximo de tentativas em caso de erro
        
    Returns:
        Lista de contas de anúncios com informações básicas
    """
    BASE_URL = "https://graph.facebook.com/v18.0"
    
    # Tenta primeiro como Business Manager
    endpoint = f"{portfolio_id}/owned_ad_accounts"
    params = {
        'access_token': access_token,
        'fields': 'id,name,account_id,account_status,currency',
        'limit': 1000
    }
    
    url = f"{BASE_URL}/{endpoint}"
    all_accounts = []
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'error' in data:
                error = data['error']
                error_code = error.get('code', 'UNKNOWN')
                error_message = error.get('message', 'Unknown error')
                
                if error_code in [190, 200]:
                    logger.error(f"Erro de autenticação ao buscar contas: {error_message}")
                    raise ValueError(f"Falha de autenticação: {error_message}")
                
                # Se não encontrar, tenta como Business ID direto
                if attempt < max_retries:
                    logger.warning(f"Tentando buscar contas como Business ID (tentativa {attempt})...")
                    endpoint = f"{portfolio_id}/adaccounts"
                    url = f"{BASE_URL}/{endpoint}"
                    time.sleep(2)
                    continue
                else:
                    raise requests.RequestException(f"Erro ao buscar contas: {error_message}")
            
            # Processa paginação
            while True:
                if 'data' in data:
                    all_accounts.extend(data['data'])
                
                paging = data.get('paging', {})
                next_url = paging.get('next')
                
                if not next_url:
                    break
                
                params = {}
                if '?' in next_url:
                    query_string = next_url.split('?')[1]
                    params = dict(param.split('=') for param in query_string.split('&') if '=' in param)
                    params['access_token'] = access_token
                
                response = requests.get(next_url, timeout=30)
                response.raise_for_status()
                data = response.json()
                time.sleep(1)
            
            break
            
        except requests.RequestException as e:
            if attempt < max_retries:
                logger.warning(f"Erro ao buscar contas (tentativa {attempt}/{max_retries}): {str(e)}")
                time.sleep(5)
                continue
            else:
                raise
    
    logger.info(f"Encontradas {len(all_accounts)} conta(s) de anúncios no portfolio {portfolio_id}")
    return all_accounts


def get_meta_client(ad_account_id: str) -> MetaAPIClient:
    """
    Factory function para criar instância do cliente Meta API usando variáveis de ambiente.
    
    Args:
        ad_account_id: ID da conta de anúncios (obrigatório, formato: act_XXXXXXXX)
    
    Returns:
        Instância configurada de MetaAPIClient
        
    Raises:
        ValueError: Se variáveis de ambiente não estiverem configuradas
    """
    access_token = os.getenv('META_ACCESS_TOKEN')
    max_retries = int(os.getenv('MAX_RETRIES', '3'))
    
    if not access_token:
        raise ValueError("META_ACCESS_TOKEN não configurada no .env")
    if not ad_account_id:
        raise ValueError("ad_account_id é obrigatório")
    
    return MetaAPIClient(access_token, ad_account_id, max_retries)
