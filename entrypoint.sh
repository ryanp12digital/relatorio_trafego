#!/bin/sh

# Entrypoint P12 Relatorios - sincroniza variáveis de ambiente para uso do cron
# Funciona tanto com .env montado quanto com variáveis de ambiente do Easypanel

# 1. Verifica se .env já existe (montado pelo Easypanel)
if [ -f /app/.env ]; then
    echo "P12 Relatorios: Usando arquivo .env montado"
else
    # 2. Cria .env a partir de variáveis de ambiente (se disponíveis)
    # Formato correto: KEY=value (sem prefixos ou exports)
    printenv | grep -E '^(EVOLUTION_|META_|MAX_RETRIES|RETRY_DELAY_SECONDS|ERROR_WEBHOOK_|DRY_RUN|TZ)=' > /app/.env 2>/dev/null
    
    if [ -s /app/.env ]; then
        echo "P12 Relatorios: .env criado a partir de variáveis de ambiente"
    else
        echo "P12 Relatorios: Aviso - Nenhuma variável de ambiente encontrada"
        # Cria arquivo vazio para evitar erros no load_dotenv()
        touch /app/.env
    fi
fi

# 3. Garante que o diretório de logs existe
mkdir -p /app/.tmp
touch /app/.tmp/cron.log

# 4. Inicia o daemon do cron em foreground
# -l 8 = log minimo do busybox crond (evita "wakeup dt=60" e dump da crontab a cada minuto no stdout)
# Saida interna do crond vai para .tmp/crond.log; cada execucao do job detalha em .tmp/cron.log
echo "P12 Relatorios: Iniciando agendamento (Cron diario 10:00 TZ do container)..."
echo "P12 Relatorios: Logs do job: /app/.tmp/cron.log | daemon crond: /app/.tmp/crond.log"
touch /app/.tmp/crond.log
exec /usr/sbin/crond -f -l 8 -L /app/.tmp/crond.log