# Diretiva: Protocolo de Envio de Relatório via WhatsApp

## Objetivo
Definir procedimento padrão para envio de relatórios formatados via WhatsApp através da Evolution API usando o script `execution/evolution_client.py`.

## Entrada
- **Mensagem formatada**: Texto do relatório no formato Markdown (sem emojis)
- **Group ID**: ID do grupo WhatsApp (formato: `group_id@g.us`)

## Variáveis de Ambiente Necessárias
- `EVOLUTION_URL`: URL base da Evolution API
- `EVOLUTION_API_KEY`: Chave de API da Evolution
- `EVOLUTION_INSTANCE`: Nome da instância do WhatsApp configurada
- `EVOLUTION_GROUP_ID`: ID do grupo WhatsApp para envio
- `MAX_RETRIES`: Número máximo de tentativas (padrão: 3)
- `RETRY_DELAY_SECONDS`: Delay entre tentativas em segundos (padrão: 300 = 5 minutos)

## Script a Usar
`execution/evolution_client.py`

## Funções Principais

### 1. `send_text_message(group_id, message)`
Envia mensagem de texto para um grupo do WhatsApp.

**Parâmetros:**
- `group_id`: ID do grupo (formato: `group_id@g.us`)
- `message`: Texto da mensagem a ser enviada

**Retorna:**
- `True` se enviado com sucesso
- `False` se falhou após todas as tentativas

**Uso:**
```python
from execution.evolution_client import get_evolution_client

client = get_evolution_client()
success = client.send_text_message("120363123456789012@g.us", "Mensagem de teste")
```

### 2. `_check_connection()`
Verifica se a Evolution API está online e a instância está conectada.

**Retorna:**
- `True` se API está online e instância está conectada
- `False` caso contrário

**Comportamento:**
- Chamada automaticamente antes de cada tentativa de envio
- Se API estiver offline, aguarda `RETRY_DELAY_SECONDS` antes de tentar novamente

## Protocolo de Envio

### Estrutura do Relatório
O relatório é enviado em **duas mensagens separadas**:

#### Mensagem 1: Relatório Absoluto
Contém métricas do período atual (últimas 24h):
- Investimento (Spend)
- Impressões
- Cliques
- CPC
- CPM
- Conversões
- CPA

#### Mensagem 2: Relatório Comparativo
Contém análise comparativa e criativo campeão:
- Variações percentuais (deltas)
- Informações do criativo campeão:
  - Nome da Campanha
  - Nome do Conjunto de Anúncios
  - Nome do Criativo
  - Conversões
  - Investimento
  - CPA

### Formatação da Mensagem

#### Regras de Formatação (Persona Next Nous)
- **Proibido**: Emojis
- **Permitido**: Markdown (negrito, listas, quebras de linha)
- **Tom**: Formal, minimalista, preciso e levemente irônico
- **Formatação**: Apenas `**negrito**` para títulos e métricas importantes

#### Exemplo de Formatação
```
**Relatório de Performance - Últimas 24h**

**Investimento:** R$ 1.234,56
**Impressões:** 12.345
**Cliques:** 123
**CPC:** R$ 10,04
**CPM:** R$ 100,00
**Conversões:** 5
**CPA:** R$ 246,91
```

### Delay Entre Mensagens
- Aguardar **2 segundos** entre o envio da primeira e segunda mensagem
- Evita sobrecarga e garante ordem de recebimento

## Tratamento de Erros

### Evolution API Offline
- **Comportamento**: Realiza 3 tentativas com intervalo de 5 minutos
- **Verificação**: Antes de cada tentativa, verifica status da conexão
- **Log**: Registra todas as tentativas em `.tmp/execution.log`
- **Resultado**: Se após 3 tentativas ainda estiver offline, lança `RequestException`

### Erro de Autenticação
- **Código**: 401 Unauthorized
- **Causa**: API Key inválida ou expirada
- **Ação**: Script loga erro e lança exceção

### Erro de Grupo
- **Código**: Grupo não encontrado ou sem permissão
- **Causa**: Group ID incorreto ou bot não está no grupo
- **Ação**: Script loga erro e retorna `False`

### Timeout
- **Timeout**: 30 segundos por requisição
- **Comportamento**: 3 tentativas com delay de 5 minutos entre tentativas
- **Ação**: Se todas as tentativas falharem, lança `RequestException`

### Logs
Todos os eventos são registrados em `.tmp/execution.log`:
- Tentativas de envio
- Status da conexão
- Erros e exceções
- Timestamps de todas as operações

## Limites e Rate Limiting

### Limites Conhecidos
- **Rate Limit**: Dependente da configuração da Evolution API
- **Timeout**: 30 segundos por requisição
- **Retries**: Máximo de 3 tentativas
- **Delay entre retries**: 5 minutos (300 segundos)

### Delay Entre Mensagens
- **Delay obrigatório**: 2 segundos entre primeira e segunda mensagem
- **Motivo**: Garantir ordem de recebimento e evitar sobrecarga

## Melhores Práticas

1. **Sempre verificar conexão**: Antes de enviar, verificar se API está online
2. **Logs detalhados**: Registrar todas as tentativas e resultados
3. **Formatação consistente**: Usar sempre Markdown sem emojis
4. **Delay entre mensagens**: Respeitar delay de 2 segundos
5. **Tratamento de erros**: Não silenciar erros, sempre logar e notificar

## Exemplo de Uso Completo

```python
from execution.evolution_client import get_evolution_client
from execution.main_scheduler import NextNousReporter

# Envio direto
client = get_evolution_client()
group_id = "120363123456789012@g.us"
message = "**Relatório de Teste**\n\nMensagem formatada aqui."

success = client.send_text_message(group_id, message)
if success:
    print("Mensagem enviada com sucesso")
else:
    print("Falha ao enviar mensagem")

# Envio completo via orquestrador
reporter = NextNousReporter()
success = reporter.generate_and_send_report()
```

## Estrutura de Mensagens (Template)

### Mensagem 1 - Absoluto
```
**Relatório de Performance - Últimas 24h**

**Investimento:** {spend}
**Impressões:** {impressions}
**Cliques:** {clicks}
**CPC:** {cpc}
**CPM:** {cpm}
**Conversões:** {conversions}
**CPA:** {cpa}
```

### Mensagem 2 - Comparativo
```
**Análise Comparativa - Período Atual vs Anterior**

**Variações:**
• Investimento: {delta_spend}
• Impressões: {delta_impressions}
• Cliques: {delta_clicks}
• CPC: {delta_cpc}
• CPM: {delta_cpm}
• Conversões: {delta_conversions}
• CPA: {delta_cpa}

**Criativo Campeão**
• Campanha: {campaign_name}
• Conjunto de Anúncios: {adset_name}
• Criativo: {ad_name}
• Conversões: {conversions}
• Investimento: {spend}
• CPA: {cpa}
```

## Atualizações
- **2024-01-XX**: Implementação inicial
- Mantenha este documento atualizado se descobrir novos limites de API ou ajustes no protocolo
