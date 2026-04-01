# Configuração do Cron para Execução Diária

## Objetivo
Configurar execução automática do relatório Next Nous diariamente às 10:00 AM.

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

# Adicionar linha (executa diariamente às 10:00 AM)
0 10 * * * cd /caminho/para/Gestor-de-Trafego && /usr/bin/python3 execution/main_scheduler.py >> .tmp/cron.log 2>&1
```

### Windows (Task Scheduler)
1. Abrir "Agendador de Tarefas" (Task Scheduler)
2. Criar nova tarefa básica
3. Configurar:
   - **Nome**: Next Nous Daily Report
   - **Gatilho**: Diariamente às 10:00
   - **Ação**: Iniciar um programa
   - **Programa/script**: `python.exe` (caminho completo)
   - **Adicionar argumentos**: `execution/main_scheduler.py`
   - **Iniciar em**: `C:\Paginas Codadas IA\Nous\Nous Dynamics\Gestor-de-Trafego`

### PowerShell Script para Windows
Criar arquivo `schedule_task.ps1`:
```powershell
$action = New-ScheduledTaskAction -Execute "python.exe" -Argument "execution/main_scheduler.py" -WorkingDirectory "C:\Paginas Codadas IA\Nous\Nous Dynamics\Gestor-de-Trafego"
$trigger = New-ScheduledTaskTrigger -Daily -At 10:00
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERNAME" -LogonType Interactive
Register-ScheduledTask -TaskName "NextNousDailyReport" -Action $action -Trigger $trigger -Principal $principal
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
