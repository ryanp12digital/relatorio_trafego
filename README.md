# P12 Relatorios

Sistema automatizado de relatórios de performance do Meta Ads via WhatsApp.

Projeto baseado na arquitetura de 3 camadas para maximizar confiabilidade e separação de responsabilidades.

## Projeto: P12 Relatorios (Meta Ads)

Automação que:
- Coleta dados de performance do Meta Ads via Marketing API
- Calcula métricas e comparações entre períodos
- Envia relatórios objetivos (data + métricas) via WhatsApp (Evolution API)

**Identificação em webhooks / logs:** `p12_relatorios` / `P12 Relatorios`.

## Estrutura do Projeto

```
.tmp/                       # Arquivos intermediários (sempre regeneráveis)
execution/                  # Scripts Python determinísticos
│   ├── meta_client.py      # Conexão com Meta Marketing API
│   ├── evolution_client.py # Conexão com Evolution API (WhatsApp)
│   ├── data_processor.py   # Cálculos e comparações
│   └── main_scheduler.py   # Orquestrador principal
directives/                 # SOPs em Markdown
│   ├── meta_ads_fetch.md       # Como buscar dados na API
│   ├── metrics_calculation.md  # Lógica de cálculo e comparação
│   └── report_delivery.md      # Protocolo de envio WhatsApp
.env                        # Variáveis de ambiente e APIs (criar manualmente)
requirements.txt            # Dependências Python
CRON_SETUP.md              # Instruções de agendamento
```

## Arquitetura

### Camada 1: Diretiva (O que fazer)
- SOPs escritos em Markdown em `directives/`
- Definem objetivos, entradas, ferramentas/scripts, saídas e edge cases
- Instruções em linguagem natural

### Camada 2: Orquestração (Tomada de decisão)
- Leitura de diretivas
- Chamada de ferramentas de execução na ordem correta
- Tratamento de erros
- Atualização de diretivas com aprendizados

### Camada 3: Execução (Fazer o trabalho)
- Scripts determinísticos em Python em `execution/`
- Chamadas de API, processamento de dados, operações de arquivos
- Lógica determinística e testável

## Configuração

### 1. Instalação de Dependências
```bash
pip install -r requirements.txt
```

### 2. Configuração de Variáveis de Ambiente
1. Copie `ENV_TEMPLATE.txt` para `.env`
2. Preencha as variáveis obrigatórias:
   - **Evolution API**: URL, API Key, Instance
   - **Meta API**: Access Token, Business ID (2126303521185590)
   - **Opcionais**: MAX_RETRIES, RETRY_DELAY_SECONDS, DRY_RUN

### 3. Configuração de Clientes (Multi-Client)
Configure os clientes no arquivo `clients.json` na raiz do projeto:

```json
[
  {
    "client_name": "Cliente Exemplo",
    "ad_account_id": "act_1234567890",
    "group_id": "120363406487428645@g.us",
    "enabled": true
  }
]
```

**Regras:**
- Cada cliente deve ter `ad_account_id` (formato: `act_XXXXXXXX`) e `group_id` (formato: `group_id@g.us`)
- Se `enabled=false`, o cliente será pulado
- O sistema valida se a conta existe no Business antes de processar

**Descobrir IDs de contas:**
```bash
python execution/list_ad_accounts.py
```

### Onde adicionar novas contas e novos grupos

| O quê | Onde |
|--------|------|
| Nome exibido no relatório, conta Meta (`act_...`), grupo WhatsApp (`...@g.us`), ligar/desligar | **[`clients.json`](clients.json)** na raiz — acrescente um objeto JSON na lista ou altere `enabled` |
| Token Meta, Business ID, Evolution API, webhook de erros | **`.env`** (local) ou variáveis de ambiente no painel do servidor |
| Envio manual para um grupo do `.env` (teste) | `python execution/send_report_to_env_group.py` — ver docstring do script |

Cada entrada em `clients.json` é independente: uma linha de negócio = uma conta de anúncios + um `group_id` do WhatsApp.

### Deploy no servidor (Docker)

O repositório inclui um [`Dockerfile`](Dockerfile) que:

- Instala dependências, copia o projeto (incluindo **`clients.json`** versionado no git).
- Agenda **`execution/main_scheduler.py` diariamente às 10:00** (horário do container; `TZ` padrão São Paulo no Dockerfile).

No **Easypanel** (ou similar):

1. Defina variáveis de ambiente equivalentes ao `.env` (o [`entrypoint.sh`](entrypoint.sh) gera `/app/.env` a partir delas se não houver arquivo montado).
2. Garanta **`META_BUSINESS_ID`** e **`META_ACCESS_TOKEN`** — sem Business ID o fluxo multi-client do cron aborta.
3. Para **incluir cliente novo**: edite `clients.json`, faça commit/deploy de nova imagem **ou** monte um volume só em `/app/clients.json` para mudar sem rebuild.
4. Logs no container:
   - **`.tmp/cron.log`** — saída do `main_scheduler` e blocos `INICIO`/`FIM` com horário UTC e `exit_code` (gerado por `scripts/cron_daily_report.sh`).
   - **`.tmp/execution.log`** — logging do Python (handlers do app).
   - **`.tmp/crond.log`** — mensagens mínimas do daemon `crond` (BusyBox); o stdout do container fica limpo (sem `wakeup dt=60` a cada minuto).

### 4. Modo DRY_RUN
Para testar sem enviar WhatsApp, configure `DRY_RUN=true` no `.env`. Os relatórios serão salvos em `.tmp/report_<ad_account_id>.md`.

### 5. Execução Manual
```bash
# Execução normal (envia WhatsApp)
python execution/main_scheduler.py

# Listar contas do Business (validação)
python execution/list_ad_accounts.py

# Modo DRY_RUN (teste sem enviar)
DRY_RUN=true python execution/main_scheduler.py
```

### 6. Agendamento Automático
Consulte `CRON_SETUP.md` para instruções de agendamento diário às 10:00 AM.

## Modo Multi-Client

O sistema suporta múltiplos clientes em uma única execução:
- Descobre automaticamente todas as contas do Business Manager
- Processa apenas clientes habilitados em `clients.json`
- Envia relatórios para grupos WhatsApp diferentes (um por cliente)
- Falhas são isoladas por cliente (não trava o lote inteiro)

**Vantagens:**
- Um único cron para todos os clientes
- Configuração centralizada via `clients.json`
- Validação automática de contas do Business

## Princípios

- **Deliverables**: Vivem na nuvem (Google Sheets, Google Slides, etc.)
- **Intermediários**: Arquivos temporários em `.tmp/` (podem ser apagados)
- **Auto-aperfeiçoamento**: O sistema aprende com erros e atualiza diretivas
- **Confiabilidade**: Lógica determinística em scripts Python

Para mais detalhes, consulte `AGENTS.md`.
