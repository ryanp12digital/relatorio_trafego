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

# Configura o Cron para execução diária às 10:00 AM (horário de São Paulo)
RUN echo 'SHELL=/bin/sh' > /etc/crontabs/root \
    && echo 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' >> /etc/crontabs/root \
    && echo '0 10 * * * cd /app && python execution/main_scheduler.py >> /app/.tmp/cron.log 2>&1' >> /etc/crontabs/root

# Torna o entrypoint executável
# O entrypoint.sh sincroniza variáveis de ambiente do Easypanel para .env,
# garantindo que o cron tenha acesso às credenciais (Meta, Evolution API, etc.)
RUN chmod +x entrypoint.sh

# Define o entrypoint como comando inicial
# O entrypoint gerencia a criação do .env e inicia o cron em foreground
CMD ["./entrypoint.sh"]