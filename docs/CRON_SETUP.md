# Configuração do Cron para Execução Semanal

## Objetivo
Configurar execução automática do P12 Relatorios **às segundas-feiras, 10:00** (relatório: últimos 7 dias vs semana anterior).

## Pré-requisitos
- Python 3.8+ instalado
- Ambiente virtual configurado (recomendado)
- Dependências instaladas (`pip install -r requirements.txt`)
- Arquivo `.env` configurado com todas as variáveis necessárias

## Comando Crontab

### Linux/macOS
```bash
# Editar crontab
crontab -e

# Adicionar linha (segundas às 10:00 — campo dia-da-semana = 1)
0 10 * * 1 cd /caminho/para/p12-relatorios && /usr/bin/python3 execution/main_scheduler.py >> .tmp/cron.log 2>&1
```

### Windows (Task Scheduler)
1. Abrir "Agendador de Tarefas" (Task Scheduler)
2. Criar nova tarefa básica
3. Configurar:
   - **Nome**: P12 Relatorios Weekly Report
   - **Gatilho**: Semanalmente, **segunda-feira**, às 10:00
   - **Ação**: Iniciar um programa
   - **Programa/script**: `python.exe` (caminho completo)
   - **Adicionar argumentos**: `execution/main_scheduler.py`
   - **Iniciar em**: `C:\caminho\para\p12-relatorios` (ajuste para a pasta do projeto)

### PowerShell Script para Windows
Criar arquivo `schedule_task.ps1`:
```powershell
$action = New-ScheduledTaskAction -Execute "python.exe" -Argument "execution/main_scheduler.py" -WorkingDirectory "C:\caminho\para\p12-relatorios"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At 10:00
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERNAME" -LogonType Interactive
Register-ScheduledTask -TaskName "P12RelatoriosWeeklyReport" -Action $action -Trigger $trigger -Principal $principal
```

## Verificação
Após configurar, verificar:
1. Logs em `.tmp/execution.log`
2. Logs do cron em `.tmp/cron.log` (se configurado)
3. Mensagens recebidas no grupo WhatsApp

## Troubleshooting
- Verificar caminhos absolutos no crontab
- Verificar permissões de execução do Python
- Verificar variáveis de ambiente (pode ser necessário carregar `.env` explicitamente)
- Verificar logs de erro em `.tmp/execution.log`
