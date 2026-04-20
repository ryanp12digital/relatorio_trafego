"""
Cliente para conexão com Evolution API (WhatsApp).

Este módulo implementa funções determinísticas para envio de mensagens
via WhatsApp através da Evolution API, com tratamento de erros e retries.
"""

import os
import re
import requests
import time
import logging
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

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

_ERR_HINT = re.compile(
    r"\b(fail|erro|error|invalid|not found|inexistente|not in group|not joined)\b", re.IGNORECASE
)


def _unwrap_response_payload(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    inner = data.get("data")
    if isinstance(inner, dict) and ("key" in inner or "message" in inner or "error" in inner):
        return inner
    return data


def _send_text_outcome(data: Any) -> Tuple[bool, str]:
    """
    Interpreta JSON da Evolution após sendText. Evita tratar 'message' genérico como sucesso
    (algumas versões devolvem 200 com corpo de erro em string).
    """
    d = _unwrap_response_payload(data)
    if not d:
        return False, "corpo vazio ou nao-JSON"
    if d.get("error") in (True, "true", 1, "1") or d.get("status") == "error":
        return False, f"api error: {d.get('message', d)}"
    for err_key in ("errors", "exception", "err"):
        if d.get(err_key):
            return False, f"campo {err_key}={d.get(err_key)}"
    m_str = d.get("message")
    if isinstance(m_str, str) and m_str and _ERR_HINT.search(m_str) and "extendedText" not in m_str.lower():
        if len(m_str) < 400 and not d.get("key"):
            return False, f"message suspeita: {m_str[:200]}"

    key = d.get("key")
    if isinstance(key, dict) and (key.get("id") or key.get("remoteJid")):
        rj = str(key.get("remoteJid") or "")
        mid = str(key.get("id") or "")
        brief = f"remoteJid={rj} id={mid[:24]}{'...' if len(mid) > 24 else ''}"
        return True, brief

    msg = d.get("message")
    if isinstance(msg, dict) and (msg.get("extendedTextMessage") or msg.get("conversation")):
        return True, "message.estruturado(whatsapp)"
    st = d.get("status")
    if st is not None and str(st).upper() in ("PENDING", "SERVER_ACK", "DELIVERED", "SENT", "READ"):
        return True, f"status={st}"

    rj_top = d.get("remoteJid")
    if rj_top:
        return True, f"remoteJid_raiz={rj_top!s}"

    return False, f"sem key.id/remoteJid (keys={list(d.keys())[:10]})"


class EvolutionAPIClient:
    """Cliente para interação com Evolution API."""
    
    def __init__(self, base_url: str, api_key: str, instance: str, max_retries: int = 3, retry_delay: int = 300):
        """
        Inicializa o cliente Evolution API.
        
        Args:
            base_url: URL base da Evolution API
            api_key: Chave de API da Evolution
            instance: Nome da instância do WhatsApp
            max_retries: Número máximo de tentativas (padrão: 3)
            retry_delay: Delay entre tentativas em segundos (padrão: 300 = 5 minutos)
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.instance = instance
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logger
        
        # Headers padrão
        self.headers = {
            'Content-Type': 'application/json',
            'apikey': api_key
        }
    
    def _check_connection(self) -> bool:
        """
        Verifica se a Evolution API está online e se a instância configurada está conectada.

        Returns:
            True se a instância estiver com connectionStatus 'open' (ou similar), False caso contrário
        """
        try:
            url = f"{self.base_url}/instance/fetchInstances"
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code != 200:
                logger.warning(f"Evolution API retornou status {response.status_code}")
                return False

            data = response.json()

            # /instance/fetchInstances retorna uma LISTA de instâncias
            if not isinstance(data, list):
                logger.warning(f"Formato inesperado em fetchInstances: {type(data)}")
                return False

            target = (self.instance or "").strip().lower()
            for inst in data:
                if not isinstance(inst, dict):
                    continue

                inst_name = str(inst.get("name", "")).strip().lower()
                if inst_name == target:
                    status = str(inst.get("connectionStatus", "")).strip().lower()
                    # No seu retorno: connectionStatus = "open"
                    is_connected = status in ["open", "connected", "online"]
                    if is_connected:
                        logger.info(f"Instância {self.instance} está conectada (connectionStatus: {status})")
                    else:
                        logger.warning(f"Instância {self.instance} encontrada mas não conectada (connectionStatus: {status})")
                    return is_connected

            logger.warning(f"Instância {self.instance} não encontrada na lista de instâncias (names disponíveis: {[i.get('name') for i in data if isinstance(i, dict)]})")
            return False

        except requests.RequestException as e:
            logger.warning(f"Erro ao verificar conexão Evolution API: {str(e)}")
            return False
    
    def send_text_message(self, group_id: str, message: str) -> bool:
        """
        Envia mensagem de texto para um grupo do WhatsApp.
        
        Implementa retry automático com 3 tentativas e intervalo de 5 minutos
        caso a Evolution API esteja offline.
        
        Args:
            group_id: ID do grupo WhatsApp (formato: group_id@g.us)
            message: Texto da mensagem a ser enviada
            
        Returns:
            True se a mensagem foi enviada com sucesso, False caso contrário
            
        Raises:
            requests.RequestException: Em caso de erro após todas as tentativas
        """
        endpoint = f"{self.base_url}/message/sendText/{self.instance}"
        
        payload = {
            "number": group_id,
            "text": message
        }
        
        for attempt in range(1, self.max_retries + 1):
            # Verifica conexão antes de tentar enviar
            if not self._check_connection():
                logger.warning(f"Evolution API offline (tentativa {attempt}/{self.max_retries})")
                if attempt < self.max_retries:
                    logger.info(f"Aguardando {self.retry_delay} segundos antes da próxima tentativa...")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    logger.error("Evolution API permanece offline após todas as tentativas")
                    raise requests.RequestException("Evolution API offline após 3 tentativas")
            
            try:
                logger.info(f"Enviando mensagem para grupo {group_id} (tentativa {attempt}/{self.max_retries})")
                response = requests.post(endpoint, json=payload, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                ok, detail = _send_text_outcome(data)
                if ok:
                    logger.info(
                        "Mensagem enviada com sucesso para grupo %s | %s",
                        group_id,
                        detail,
                    )
                    return True

                err_hint = data if isinstance(data, dict) else str(data)[:400]
                logger.warning(
                    "Resposta Evolution sem key/id de mensagem (grupo=%s): %s | corpo=%s",
                    group_id,
                    detail,
                    err_hint,
                )
                # Não usar RETRY_DELAY longo para corpo JSON estranho — falha rápida.
                return False
                        
            except requests.Timeout:
                logger.warning(f"Timeout ao enviar mensagem (tentativa {attempt}/{self.max_retries})")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise
                    
            except requests.RequestException as e:
                logger.warning(f"Erro ao enviar mensagem (tentativa {attempt}/{self.max_retries}): {str(e)}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise
        
        return False

    def fetch_group_info(self, group_jid: str) -> Optional[Dict[str, Any]]:
        """
        GET /group/findGroupInfos/{instance}?groupJid=... (Evolution v2).
        Retorna dict com id, subject, etc., ou None se 404/erro.
        """
        gj = (group_jid or "").strip()
        if not gj:
            return None
        url = f"{self.base_url}/group/findGroupInfos/{quote(self.instance, safe='')}"
        try:
            resp = requests.get(
                url,
                headers=self.headers,
                params={"groupJid": gj, "getParticipants": "false"},
                timeout=20,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning("findGroupInfos falhou para %s: %s", gj, e)
            return None
        except ValueError as e:
            logger.warning("findGroupInfos JSON inválido para %s: %s", gj, e)
            return None

        if not isinstance(data, dict):
            return None
        inner = data.get("data")
        if isinstance(inner, dict) and (inner.get("id") or inner.get("subject") is not None):
            return inner
        if data.get("id") or data.get("subject") is not None:
            return data
        return None


def get_evolution_client() -> EvolutionAPIClient:
    """
    Factory function para criar instância do cliente Evolution API usando variáveis de ambiente.
    
    Returns:
        Instância configurada de EvolutionAPIClient
        
    Raises:
        ValueError: Se variáveis de ambiente não estiverem configuradas
    """
    base_url = os.getenv('EVOLUTION_URL') or os.getenv('EVOLUTION_SERVER_URL')
    api_key = os.getenv('EVOLUTION_API_KEY')
    instance = os.getenv('EVOLUTION_INSTANCE')
    max_retries = int(os.getenv('MAX_RETRIES', '3'))
    retry_delay = int(os.getenv('RETRY_DELAY_SECONDS', '300'))
    
    if not base_url:
        raise ValueError("EVOLUTION_URL ou EVOLUTION_SERVER_URL não configurada no .env")
    if not api_key:
        raise ValueError("EVOLUTION_API_KEY não configurada no .env")
    if not instance:
        raise ValueError("EVOLUTION_INSTANCE não configurada no .env")
    
    return EvolutionAPIClient(base_url, api_key, instance, max_retries, retry_delay)