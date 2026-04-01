FROM python:3.11-alpine

WORKDIR /app

# Instala timezone data
RUN apk add --no-cache tzdata

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

# Cron diario 10:00 (fuso do container; TZ=America/Sao_Paulo no Dockerfile).
# Job chama script que escreve blocos legiveis em .tmp/cron.log.
RUN echo 'SHELL=/bin/sh' > /etc/crontabs/root \
    && echo 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' >> /etc/crontabs/root \
    && echo '0 10 * * * /app/scripts/cron_daily_report.sh' >> /etc/crontabs/root

# Torna o entrypoint executável
# entrypoint.sh: sincroniza variáveis do Easypanel para .env (P12 Relatorios)
RUN chmod +x entrypoint.sh

# Define o entrypoint como comando inicial
# O entrypoint gerencia a criação do .env e inicia o cron em foreground
CMD ["./entrypoint.sh"]