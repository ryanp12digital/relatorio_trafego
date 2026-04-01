# ESPECIFICAÇÃO TÉCNICA: AGENTE NEXT NOUS (META ADS REPORT)

Este documento define as regras de implementação para o agente Next Nous. A implementação deve seguir rigorosamente a arquitetura de 3 camadas definida no arquivo AGENTS.md.

## 1. REGRAS DE NEGÓCIO (DATA LOGIC)

### 1.1 Escopo de Dados
- Fonte: Meta Marketing API (Graph API).
- Filtro de Inclusão: Todas as campanhas que apresentaram spend > 0 no período, independentemente do status (Ativa, Pausada ou Deletada).
- Tipos de Conversão: Soma de eventos de "Lead" e "WhatsApp" (mensagens iniciadas).

### 1.2 Janelas de Tempo (Timeframes)
- Período A (Atual): Últimas 24 horas fechadas.
- Período B (Comparativo): As 24 horas imediatamente anteriores ao Período A.
- Cálculo de Delta: ((Valor_A - Valor_B) / Valor_B) * 100. Caso Valor_B seja 0, retornar "Novo Volume".

### 1.3 Métricas Obrigatórias
1. Investimento (Spend)
2. Impressões
3. Cliques
4. CPC (Custo por Clique)
5. CPM (Custo por mil impressões)
6. Conversões (Total Lead + WhatsApp)
7. Custo por Conversão (CPA)

### 1.4 Identificação de Criativo Campeão
- Nível de granularidade: ad.
- Critério: Maior volume de conversões no Período A.
- Dados necessários: Nome do Criativo, Nome do Conjunto de Anúncios e Nome da Campanha.

## 2. PERSONA E COMUNICAÇÃO

- Nome: Next Nous.
- Perfil: Fusão entre a eficiência analítica do Jarvis (Iron Man) e a sobriedade/fidelidade do Alfred (Batman).
- Tom: Formal, minimalista, preciso e levemente irônico.
- Restrição: Proibido o uso de emojis. Utilizar apenas formatação Markdown (negrito, listas) para clareza.

## 3. ESTRUTURA DE ARQUIVOS (HIERARQUIA)

O projeto deve ser organizado da seguinte forma:

/
├── .env                        # Credenciais e IDs
├── directives/                 # Camada 1: SOPs (Markdown)
│   ├── meta_ads_fetch.md       # Como buscar dados na API
│   ├── metrics_calculation.md  # Lógica de cálculo e comparação
│   └── report_delivery.md      # Protocolo de envio WhatsApp
├── execution/                  # Camada 3: Scripts (Python)
│   ├── meta_client.py          # Conexão base com Meta API
│   ├── evolution_client.py     # Conexão base com Evolution API
│   ├── data_processor.py       # Cálculos e comparações
│   └── main_scheduler.py       # Script principal para o Cron
└── .tmp/                       # Arquivos temporários de processamento

## 4. PASSO A PASSO PARA EXECUÇÃO (WORKFLOW)

O Cursor deve seguir esta sequência de desenvolvimento:

### Passo 1: Configuração de Ambiente
1. Validar as variáveis no .env (Evolution URL, API Key, Instance, Group ID, Meta Token, Ad Account ID).
2. Criar a estrutura de pastas directives/, execution/ e .tmp/.

### Passo 2: Desenvolvimento da Camada de Execução (Scripts)
1. meta_client.py: Implementar função para buscar insights em nível de conta e nível de anúncio com tratamento de erros e retries.
2. data_processor.py: Criar funções que recebam os JSONs da Meta, realizem a soma das conversões (Lead + WA) e calculem as variações percentuais.
3. evolution_client.py: Implementar a função de envio de mensagem de texto para o ID-GRUPO especificado.
4. main_scheduler.py: Orquestrador que chama a coleta, processa os dados, formata o texto na persona Next Nous e dispara o envio.

### Passo 3: Criação das Diretivas (SOPs)
1. Escrever os arquivos .md na pasta directives/ detalhando como cada script deve ser operado e quais são os limites de API conhecidos.

### Passo 4: Formatação da Mensagem
1. Configurar o template de saída para enviar duas mensagens distintas:
   - Mensagem 1: Relatório de performance absoluta (24h).
   - Mensagem 2: Relatório de performance comparativa (%) e dados do Criativo Campeão.

### Passo 5: Agendamento
1. Gerar o comando crontab para execução diária às 10:00 AM.

## 5. TRATAMENTO DE ERROS (SELF-ANNEALING)

- Caso a API da Meta retorne erro de Token, o script deve logar o erro e notificar falha de autenticação.
- Caso a Evolution API esteja offline, o script deve realizar 3 tentativas com intervalo de 5 minutos antes de encerrar o processo.
- Logs de execução devem ser salvos em .tmp/execution.log.