FROM python:3.11-alpine

WORKDIR /app

# Instala timezone data
RUN apk add --no-cache tzdata

# Porta do webhook Make -> leads Meta (mapear no Easypanel)
EXPOSE 8080

# Define timezone para São Paulo
ENV TZ=America/Sao_Paulo
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone

# Copia dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o projeto
COPY . .

# Cria pasta de logs com permissões corretas
RUN mkdir -p .tmp && chmod 755 .tmp

RUN chmod +x scripts/cron_daily_report.sh

# Cron semanal: segunda-feira 10:00 (fuso do container; TZ=America/Sao_Paulo).
# Relatorio: ultimos 7 dias vs semana anterior (ver main_scheduler.get_period_dates).
RUN echo 'SHELL=/bin/sh' > /etc/crontabs/root \
    && echo 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' >> /etc/crontabs/root \
    && echo '0 10 * * 1 /app/scripts/cron_daily_report.sh' >> /etc/crontabs/root

# Torna o entrypoint executável
# entrypoint.sh: sincroniza variáveis do Easypanel para .env (P12 Relatorios)
RUN chmod +x entrypoint.sh

# Define o entrypoint como comando inicial
# O entrypoint gerencia a criação do .env e inicia o cron em foreground
CMD ["./entrypoint.sh"]