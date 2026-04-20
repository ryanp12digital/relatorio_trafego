# Diretiva: Busca de Dados na Meta Marketing API

## Objetivo
Definir procedimento padrão para buscar dados de campanhas e métricas da Meta Marketing API (Graph API) através do script `execution/meta_client.py`.

## Entrada
- **Período A (Atual)**: Últimas 24 horas fechadas (formato: YYYY-MM-DD)
- **Período B (Comparativo)**: 24 horas imediatamente anteriores ao Período A (formato: YYYY-MM-DD)

## Variáveis de Ambiente Necessárias
- `META_ACCESS_TOKEN`: Token de acesso da Meta API (long-lived token recomendado)
- `META_AD_ACCOUNT_ID`: ID da conta de anúncios (formato: `act_XXXXXXXX`)
- `MAX_RETRIES`: Número máximo de tentativas (padrão: 3)

## Script a Usar
`execution/meta_client.py`

## Funções Principais

### 1. `get_account_insights(start_date, end_date)`
Busca insights agregados no nível de conta de anúncios.

**Parâmetros:**
- `start_date`: Data inicial (YYYY-MM-DD)
- `end_date`: Data final (YYYY-MM-DD)

**Retorna:**
- Lista de insights com métricas: spend, impressions, clicks, actions, cpc, cpm

**Uso:**
```python
from execution.meta_client import get_meta_client

client = get_meta_client()
insights = client.get_account_insights("2024-01-15", "2024-01-16")
```

### 2. `get_ads_with_insights(start_date, end_date)`
Busca anúncios com insights detalhados, incluindo apenas campanhas com spend > 0.

**Filtros aplicados:**
- Apenas campanhas com spend > 0 no período
- Inclui campanhas ativas, pausadas ou deletadas (se tiveram spend)

**Retorna:**
- Lista de anúncios com:
  - Nome do criativo
  - Nome do conjunto de anúncios (adset)
  - Nome da campanha
  - Insights (spend, impressions, clicks, actions, cpc, cpm)

**Uso:**
```python
ads = client.get_ads_with_insights("2024-01-15", "2024-01-16")
```

## Saída Esperada
- JSON estruturado com dados hierárquicos (conta → campanha → adset → anúncio)
- Métricas agregadas por período
- Informações de conversões (actions) incluindo Lead e WhatsApp

## Limites da API e Rate Limiting

### Limites Conhecidos
- **Rate Limit**: Aproximadamente 200 requisições por hora por usuário
- **Páginas**: Resultados são paginados automaticamente (até 100 registros por página)
- **Timeout**: 30 segundos por requisição
- **Retries**: 3 tentativas com delay de 5 segundos entre tentativas

### Edge Cases
1. **Token Expirado** (Erro 190/200):
   - O script loga o erro e lança `ValueError`
   - Solução: Gerar novo long-lived token na Meta Business Suite

2. **Conta sem Spend**:
   - Retorna lista vazia (comportamento esperado)
   - Não é um erro, apenas não há dados

3. **Campanhas Deletadas**:
   - Apenas campanhas com spend > 0 são incluídas
   - Campanhas deletadas sem spend não aparecem

4. **Período sem Dados**:
   - API retorna array vazio `[]`
   - Processamento continua normalmente (métricas ficam zeradas)

### Paginação Automática
O script implementa paginação automática através de `_paginate_request()`:
- Detecta `paging.next` na resposta
- Extrai parâmetros da próxima página
- Continua até não haver mais páginas
- Rate limiting de 1 segundo entre páginas

## Tratamento de Erros

### Erros de Autenticação
- **Código 190**: Token inválido ou expirado
- **Código 200**: Permissões insuficientes
- **Ação**: Script loga erro e lança `ValueError` (falha fatal)

### Erros de Rede
- **Timeout**: 3 tentativas com 5 segundos de delay
- **Conexão**: 3 tentativas com 5 segundos de delay
- **Ação**: Se todas as tentativas falharem, lança `RequestException`

### Logs
Todos os erros são logados em `.tmp/execution.log` com timestamp e stack trace.

## Melhores Práticas

1. **Sempre usar long-lived tokens**: Reduz necessidade de renovação
2. **Validar variáveis de ambiente**: Antes de criar o cliente
3. **Monitorar logs**: Verificar `.tmp/execution.log` para problemas
4. **Respeitar rate limits**: O script já implementa delays automáticos
5. **Períodos fechados**: Sempre usar períodos completos de 24 horas

## Exemplo de Uso Completo

```python
from datetime import datetime, timedelta
from execution.meta_client import get_meta_client

# Calcula períodos (últimas 24h fechadas)
now = datetime.now()
period_a_end = (now - timedelta(days=1)).strftime('%Y-%m-%d')
period_a_start = (now - timedelta(days=2)).strftime('%Y-%m-%d')

# Cria cliente e busca dados
client = get_meta_client()
insights = client.get_account_insights(period_a_start, period_a_end)
ads = client.get_ads_with_insights(period_a_start, period_a_end)
```

## Atualizações
- **2024-01-XX**: Implementação inicial
- Mantenha este documento atualizado com novos limites de API ou edge cases descobertos durante execução
