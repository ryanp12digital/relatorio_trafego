# ESPECIFICAÇÃO TÉCNICA: P12 RELATORIOS (META ADS REPORT)

Este documento define as regras de implementação para o projeto **P12 Relatorios**. A implementação deve seguir rigorosamente a arquitetura de 3 camadas definida no arquivo AGENTS.md.

## 1. REGRAS DE NEGÓCIO (DATA LOGIC)

### 1.1 Escopo de Dados
- Fonte: Meta Marketing API (Graph API).
- Filtro de Inclusão: Todas as campanhas que apresentaram spend > 0 no período, independentemente do status (Ativa, Pausada ou Deletada).
- Tipos de Conversão: Soma de eventos de "Lead" e "WhatsApp" (mensagens iniciadas).

### 1.2 Janelas de Tempo (Timeframes)
- Período A (Atual): Dia anterior completo (fuso São Paulo / UTC-3 no agendador).
- Período B (Comparativo): Dia imediatamente anterior ao período A.
- Deltas percentuais podem ser calculados em `data_processor` para outros usos; a mensagem WhatsApp atual inclui métricas absolutas do dia A e do dia B lado a lado (bloco comparativo = valores do período B).

### 1.3 Métricas Obrigatórias
1. Investimento (Spend)
2. Impressões
3. Cliques
4. CPC (Custo por Clique)
5. CPM (Custo por mil impressões)
6. Conversões (Total Lead + WhatsApp)
7. Custo por Conversão (CPA)

### 1.4 Criativo campeão (processamento interno)
- Ainda calculado em `data_processor` quando necessário para evoluções; o template WhatsApp atual não inclui bloco "Top criativo".

## 2. COMUNICAÇÃO (WHATSAPP)

- Nome do projeto: **P12 Relatorios**.
- Formato: título em negrito (estilo WhatsApp), data(s), linhas `emoji *Rótulo:* valor`.
- Uma única mensagem por cliente: bloco do dia de referência + bloco comparativo (métricas absolutas do dia B).
- Campo `origem` em webhooks de erro: `p12_relatorios`.

## 3. ESTRUTURA DE ARQUIVOS (HIERARQUIA)

/
├── .env                        # Credenciais e IDs
├── data/                       # JSON: Meta, Google, templates de mensagem
│   ├── clients.json
│   ├── google_clients.json
│   └── message_templates.json
├── docs/                       # Documentação e Camada 1 (SOPs)
│   └── directives/
│       ├── meta_ads_fetch.md
│       ├── metrics_calculation.md
│       └── report_delivery.md
├── execution/                  # Camada 3: Scripts (Python)
│   ├── meta_client.py
│   ├── evolution_client.py
│   ├── data_processor.py
│   ├── webhook_notify.py
│   └── main_scheduler.py       # Orquestrador (classe P12RelatoriosReporter)
├── scripts/
│   └── cron_daily_report.sh    # Wrapper de log no Docker
└── .tmp/                       # Logs e artefatos regeneráveis

## 4. PASSO A PASSO PARA EXECUÇÃO (WORKFLOW)

### Passo 1: Configuração de Ambiente
1. Validar variáveis no `.env` (Evolution, Meta Token, Business ID para multi-client).
2. Preencher `data/clients.json` para cada cliente (ou usar Postgres + dashboard).

### Passo 2: Camada de Execução
1. meta_client.py: insights conta + anúncios com spend, retries e erros de token.
2. data_processor.py: agregação, conversões Lead+WhatsApp, métricas derivadas.
3. evolution_client.py: envio de texto para o `group_id`.
4. main_scheduler.py: orquestra coleta, formata relatório P12 Relatorios e envia.

### Passo 3: Diretivas (SOPs)
1. Manter `docs/directives/*.md` alinhados aos scripts e limites de API.

### Passo 4: Agendamento
1. Cron diário (ex.: 10:00) — ver Dockerfile / `docs/CRON_SETUP.md`.

## 5. TRATAMENTO DE ERROS (SELF-ANNEALING)

- Erro de token Meta: log + webhook (`meta_token_expirado` / `erro_automacao`) quando configurado.
- Evolution API offline: retentativas com intervalo configurável.
- Logs: `.tmp/execution.log`, `.tmp/cron.log` (container).
