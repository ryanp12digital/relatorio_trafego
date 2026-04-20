# Diretiva: Cálculo de Métricas e Comparações

## Objetivo
Definir procedimento padrão para processar dados da Meta API, calcular métricas agregadas, identificar criativos campeões e calcular variações percentuais entre períodos através do script `execution/data_processor.py`.

## Entrada
- **Period A Insights**: Lista de insights do período atual (nível de conta)
- **Period B Insights**: Lista de insights do período comparativo (nível de conta)
- **Period A Ads**: Lista de anúncios do período atual (com insights detalhados)

## Script a Usar
`execution/data_processor.py`

## Funções Principais

### 1. `extract_conversions(actions)`
Extrai e soma conversões de Lead e WhatsApp.

**Lógica:**
- Percorre lista de ações (actions) do insight
- Identifica ações do tipo "lead" ou "whatsapp"/"whats_app"
- Soma os valores dessas ações
- Retorna total de conversões

**Uso:**
```python
from execution.data_processor import DataProcessor

processor = DataProcessor()
conversions = processor.extract_conversions(insight.get('actions', []))
```

### 2. `aggregate_metrics(insights)`
Agrega métricas de uma lista de insights.

**Métricas Calculadas:**
- **Spend**: Soma do investimento total
- **Impressions**: Soma de impressões totais
- **Clicks**: Soma de cliques totais
- **Conversions**: Soma de conversões (Lead + WhatsApp)
- **CPC**: Spend / Clicks (se clicks > 0, senão 0)
- **CPM**: (Spend / Impressions) * 1000 (se impressions > 0, senão 0)
- **CPA**: Spend / Conversions (se conversions > 0, senão 0)

**Uso:**
```python
metrics = processor.aggregate_metrics(insights_list)
```

### 3. `calculate_delta(value_a, value_b)`
Calcula variação percentual entre dois valores.

**Fórmula:**
```
Delta = ((Valor_A - Valor_B) / Valor_B) * 100
```

**Edge Cases:**
- Se `value_b == 0`: Retorna `"Novo Volume"` (sem comparação possível)
- Se delta positivo: Retorna `"+XX.XX%"`
- Se delta negativo: Retorna `"-XX.XX%"`

**Precisão:** 2 casas decimais

**Uso:**
```python
delta = processor.calculate_delta(1000.0, 800.0)  # Retorna "+25.00%"
delta = processor.calculate_delta(500.0, 0.0)     # Retorna "Novo Volume"
```

### 4. `find_champion_creative(ads_data)`
Identifica o criativo campeão (anúncio com maior volume de conversões).

**Critério:**
- Nível de granularidade: `ad` (anúncio individual)
- Métrica: Maior volume de conversões (Lead + WhatsApp) no Período A

**Dados Retornados:**
- Nome do Criativo (`ad_name`)
- Nome do Conjunto de Anúncios (`adset_name`)
- Nome da Campanha (`campaign_name`)
- Total de Conversões
- Métricas agregadas do criativo (spend, CPA, etc.)

**Uso:**
```python
champion = processor.find_champion_creative(ads_list)
if champion:
    print(f"Criativo: {champion['ad_name']}")
    print(f"Conversões: {champion['conversions']}")
```

### 5. `process_periods(period_a_insights, period_b_insights, period_a_ads)`
Processa dados de dois períodos e gera relatório comparativo completo.

**Retorna:**
```python
{
    'period_a': {
        'spend': float,
        'impressions': int,
        'clicks': int,
        'conversions': int,
        'cpc': float,
        'cpm': float,
        'cpa': float
    },
    'period_b': {
        # Mesmas métricas do período A
    },
    'deltas': {
        'spend': str,          # Ex: "+15.50%"
        'impressions': str,
        'clicks': str,
        'cpc': str,
        'cpm': str,
        'conversions': str,
        'cpa': str
    },
    'champion': {
        'ad_name': str,
        'adset_name': str,
        'campaign_name': str,
        'conversions': int,
        'metrics': {...}
    } ou None
}
```

**Uso:**
```python
results = processor.process_periods(
    period_a_insights,
    period_b_insights,
    period_a_ads
)
```

## Regras de Negócio

### 1. Tipos de Conversão
- **Lead**: Qualquer ação com `action_type` contendo "lead"
- **WhatsApp**: Qualquer ação com `action_type` contendo "whatsapp" ou "whats_app"
- **Total**: Soma de Lead + WhatsApp

### 2. Janelas de Tempo
- **Período A**: Últimas 24 horas fechadas
- **Período B**: 24 horas imediatamente anteriores ao Período A
- Ambos os períodos são completos (00:00:00 até 23:59:59)

### 3. Filtro de Inclusão
- Apenas campanhas com **spend > 0** no período são incluídas
- Status da campanha (Ativa, Pausada, Deletada) não importa, apenas o spend

### 4. Identificação de Campeão
- Comparação feita apenas no **Período A** (período atual)
- Critério: Maior volume de conversões (Lead + WhatsApp)
- Em caso de empate: Retorna o primeiro encontrado (pode ser aleatório)

## Edge Cases

### 1. Sem Conversões
- `find_champion_creative()` retorna `None`
- CPA fica como `0.0` se não houver conversões

### 2. Sem Cliques
- CPC fica como `0.0` (evita divisão por zero)

### 3. Sem Impressões
- CPM fica como `0.0` (evita divisão por zero)

### 4. Período Comparativo Zerado
- Delta retorna `"Novo Volume"` (indicando que não havia volume antes)

### 5. Lista Vazia de Insights
- `aggregate_metrics()` retorna todas as métricas zeradas

### 6. Lista Vazia de Anúncios
- `find_champion_creative()` retorna `None`

## Funções Auxiliares de Formatação

### `format_currency(value)`
Formata valor monetário em Real (BRL).

**Formato:** `R$ X.XXX,XX`

**Exemplo:**
```python
from execution.data_processor import format_currency
format_currency(1234.56)  # Retorna "R$ 1.234,56"
```

### `format_number(value)`
Formata número inteiro com separador de milhar.

**Formato:** `X.XXX`

**Exemplo:**
```python
from execution.data_processor import format_number
format_number(1234)  # Retorna "1.234"
```

## Exemplo de Uso Completo

```python
from execution.data_processor import DataProcessor
from execution.meta_client import get_meta_client

# Coleta dados
client = get_meta_client()
period_a_insights = client.get_account_insights("2024-01-15", "2024-01-16")
period_b_insights = client.get_account_insights("2024-01-14", "2024-01-15")
period_a_ads = client.get_ads_with_insights("2024-01-15", "2024-01-16")

# Processa dados
processor = DataProcessor()
results = processor.process_periods(
    period_a_insights,
    period_b_insights,
    period_a_ads
)

# Acessa resultados
print(f"Spend Período A: {results['period_a']['spend']}")
print(f"Delta Spend: {results['deltas']['spend']}")
if results['champion']:
    print(f"Criativo Campeão: {results['champion']['ad_name']}")
```

## Validações
- Sempre validar se `champion` é `None` antes de acessar seus dados
- Verificar se métricas não são `None` antes de formatar
- Tratar divisão por zero em CPC, CPM e CPA

## Atualizações
- **2024-01-XX**: Implementação inicial
- Mantenha este documento atualizado se descobrir novos edge cases ou ajustes na lógica de negócio
