# Checklist de Configuração - P12 Relatorios

## Pré-requisitos
- [ ] Python 3.8+ instalado
- [ ] Acesso à Meta Marketing API (token válido)
- [ ] Evolution API configurada e funcionando
- [ ] ID do grupo WhatsApp onde o relatório será enviado

## Configuração do Ambiente

### 1. Instalação de Dependências
- [ ] Executar `pip install -r requirements.txt`
- [ ] Verificar instalação: `pip list | grep requests`

### 2. Configuração do .env
- [ ] Copiar `ENV_TEMPLATE.txt` para `.env`
- [ ] Preencher `EVOLUTION_URL`
- [ ] Preencher `EVOLUTION_API_KEY`
- [ ] Preencher `EVOLUTION_INSTANCE`
- [ ] Preencher `EVOLUTION_GROUP_ID` (formato: `group_id@g.us`)
- [ ] Preencher `META_ACCESS_TOKEN` (long-lived token recomendado)
- [ ] Preencher `META_AD_ACCOUNT_ID` (formato: `act_XXXXXXXX`)
- [ ] Opcional: Ajustar `MAX_RETRIES` (padrão: 3)
- [ ] Opcional: Ajustar `RETRY_DELAY_SECONDS` (padrão: 300)

### 3. Validação das Credenciais

#### Meta API
- [ ] Token de acesso válido e com permissões corretas
- [ ] Ad Account ID correto (formato: `act_XXXXXXXX`)
- [ ] Verificar se a conta tem campanhas ativas

#### Evolution API
- [ ] API online e acessível
- [ ] Instância configurada e conectada
- [ ] Bot adicionado ao grupo WhatsApp
- [ ] Group ID correto (verificar formato)

### 4. Teste Manual
- [ ] Executar `python execution/main_scheduler.py` manualmente
- [ ] Verificar logs em `.tmp/execution.log`
- [ ] Confirmar recebimento de mensagens no grupo WhatsApp

### 5. Agendamento Automático
- [ ] Seguir instruções em `docs/CRON_SETUP.md`
- [ ] Configurar cron/task scheduler para execução diária às 10:00 AM
- [ ] Testar execução agendada
- [ ] Verificar logs após primeira execução automática

## Verificação Final

### Estrutura de Arquivos
- [ ] `execution/` com todos os scripts Python
- [ ] `data/` com `clients.json` (e opcionalmente `google_clients.json`, `message_templates.json`)
- [ ] `docs/directives/` com todas as diretivas Markdown
- [ ] `.tmp/` criado (para logs)
- [ ] `.env` configurado (não versionado)

### Funcionalidades
- [ ] Coleta de dados da Meta API funcionando
- [ ] Processamento de métricas funcionando
- [ ] Identificação de criativo campeão funcionando
- [ ] Envio de mensagens via WhatsApp funcionando
- [ ] Formatação de mensagens correta (Markdown, sem emojis)
- [ ] Tratamento de erros funcionando (logs em `.tmp/execution.log`)

## Troubleshooting

### Problemas Comuns

**Erro de autenticação Meta API**
- Verificar se o token está válido
- Verificar permissões do token
- Gerar novo long-lived token se necessário

**Evolution API offline**
- Verificar status da API
- Verificar conectividade da instância
- Verificar se o bot está no grupo

**Nenhuma mensagem recebida**
- Verificar logs em `.tmp/execution.log`
- Verificar se o Group ID está correto
- Verificar se o bot tem permissão para enviar mensagens

**Erro de importação Python**
- Verificar se todas as dependências estão instaladas
- Verificar se está executando do diretório correto
- Verificar variáveis de ambiente carregadas corretamente

## Suporte
- Consultar diretivas em `docs/directives/` para detalhes específicos
- Verificar logs em `.tmp/execution.log` para erros
- Consultar `docs/AGENTE_P12_RELATORIOS.md` para especificação técnica completa
