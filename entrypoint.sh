#!/bin/sh

# Entrypoint do Next Nous - Sincroniza variáveis de ambiente para uso do cron
# Funciona tanto com .env montado quanto com variáveis de ambiente do Easypanel

# 1. Verifica se .env já existe (montado pelo Easypanel)
if [ -f /app/.env ]; then
    echo "Next Nous: Usando arquivo .env montado"
else
    # 2. Cria .env a partir de variáveis de ambiente (se disponíveis)
    # Formato correto: KEY=value (sem prefixos ou exports)
    printenv | grep -E '^(EVOLUTION_|META_|MAX_RETRIES|RETRY_DELAY_SECONDS)=' > /app/.env 2>/dev/null
    
    if [ -s /app/.env ]; then
        echo "Next Nous: .env criado a partir de variáveis de ambiente"
    else
        echo "Next Nous: Aviso - Nenhuma variável de ambiente encontrada"
        # Cria arquivo vazio para evitar erros no load_dotenv()
        touch /app/.env
    fi
fi

# 3. Garante que o diretório de logs existe
mkdir -p /app/.tmp
touch /app/.tmp/cron.log

# 4. Inicia o daemon do cron em foreground
echo "Next Nous: Iniciando agendamento (Cron)..."
exec /usr/sbin/crond -f -l 2 -L /dev/stdout