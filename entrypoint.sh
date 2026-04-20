#!/bin/sh

# Entrypoint P12 Relatorios - sincroniza variáveis de ambiente para uso do cron
# Funciona tanto com .env montado quanto com variáveis de ambiente do Easypanel

# 1. Verifica se .env já existe (montado pelo Easypanel)
if [ -f /app/.env ]; then
    echo "P12 Relatorios: 📄 Usando arquivo .env montado"
else
    # 2. Cria .env a partir de variáveis de ambiente (se disponíveis)
    # Formato correto: KEY=value (sem prefixos ou exports)
    # Inclui variáveis do relatório (REPORT_*, DEFAULT_REPORT_TIMEZONE, FORCE_WEEKLY_REPORT) e Meta (META_* já cobre atribuição).
    printenv | grep -E '^(EVOLUTION_|META_|MAX_RETRIES|RETRY_DELAY_SECONDS|ERROR_WEBHOOK_|DRY_RUN|TZ|LORENA_|WEBHOOK_|REPORT_|DEFAULT_REPORT_TIMEZONE|FORCE_WEEKLY_REPORT|META_LEAD_|DASHBOARD_|ENABLE_DASHBOARD|GOOGLE_|DATABASE_URL|SUPABASE_DATABASE_URL|FLASK_SECRET_KEY|SUPABASE_)=' > /app/.env 2>/dev/null
    
    if [ -s /app/.env ]; then
        echo "P12 Relatorios: ✅ .env criado a partir de variáveis de ambiente"
    else
        echo "P12 Relatorios: ⚠️ Aviso - Nenhuma variável de ambiente encontrada"
        # Cria arquivo vazio para evitar erros no load_dotenv()
        touch /app/.env
    fi
fi

# 3. Garante que o diretório de logs e data/ existem
mkdir -p /app/.tmp /app/data
touch /app/.tmp/cron.log

# 4. Webhook Make -> WhatsApp (leads Meta multi-cliente). Porta: WEBHOOK_PORT (default 8080).
#    POST /meta-new-lead (padrao) e /lorena-new-lead (alias legado).
WEBHOOK_PORT="${WEBHOOK_PORT:-8080}"
echo "P12 Relatorios: 🚀 Iniciando webhook Meta Leads na porta ${WEBHOOK_PORT} (background)..."
export WEBHOOK_PORT
# tee: mesma saida no stdout do container (logs Easypanel) e no arquivo para arquivo/grep
cd /app && python /app/execution/meta_lead_webhook.py 2>&1 | tee -a /app/.tmp/webhook_meta_leads.log &
echo "P12 Relatorios: 📡 Webhook Meta Leads no stdout (filtro: P12_META_LEAD_WEBHOOK) e em /app/.tmp/webhook_meta_leads.log"

# 5. Dashboard viva (opcional): porta DASHBOARD_PORT (default 8091)
ENABLE_DASHBOARD="${ENABLE_DASHBOARD:-true}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8091}"
if [ "${ENABLE_DASHBOARD}" = "true" ] || [ "${ENABLE_DASHBOARD}" = "1" ] || [ "${ENABLE_DASHBOARD}" = "yes" ]; then
    echo "P12 Relatorios: 🎛️ Iniciando dashboard na porta ${DASHBOARD_PORT} (background)..."
    export DASHBOARD_PORT
    cd /app && python /app/execution/dashboard_app.py 2>&1 | tee -a /app/.tmp/dashboard.log &
    echo "P12 Relatorios: 🖥️ Dashboard no stdout e em /app/.tmp/dashboard.log"
else
    echo "P12 Relatorios: ⏭️ Dashboard desativada (ENABLE_DASHBOARD=${ENABLE_DASHBOARD})"
fi

# 6. Inicia o daemon do cron em foreground
# -l 8 = log minimo do busybox crond (evita "wakeup dt=60" e dump da crontab a cada minuto no stdout)
# Saida interna do crond vai para .tmp/crond.log; cada execucao do job detalha em .tmp/cron.log
echo "P12 Relatorios: ⏰ Iniciando agendamento (Cron segunda-feira 10:00 TZ do container)..."
echo "P12 Relatorios: 📝 Logs do job: /app/.tmp/cron.log | daemon crond: /app/.tmp/crond.log"
touch /app/.tmp/crond.log
exec /usr/sbin/crond -f -l 8 -L /app/.tmp/crond.log