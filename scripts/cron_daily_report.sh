#!/bin/sh
# Executado pelo crontab no container: registra inicio/fim e delega ao orquestrador.
set -eu

cd /app
LOG=/app/.tmp/cron.log
mkdir -p /app/.tmp

UTC_TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u '+%Y-%m-%d %H:%M:%S UTC')
LOCAL_TS=$(date '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date)

{
  echo ""
  echo "================================================================================"
  echo "[$UTC_TS] INICIO main_scheduler (UTC)"
  echo "[$LOCAL_TS] INICIO main_scheduler (fuso do container)"
  echo "--------------------------------------------------------------------------------"
} >> "$LOG"

set +e
python execution/main_scheduler.py >> "$LOG" 2>&1
EC=$?
set -e

UTC_END=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u '+%Y-%m-%d %H:%M:%S UTC')

{
  echo "--------------------------------------------------------------------------------"
  echo "[$UTC_END] FIM main_scheduler exit_code=$EC"
  echo "================================================================================"
} >> "$LOG"

# Uma linha no stdout do container (facil de achar nos logs do painel)
echo "[P12Relatorios] main_scheduler concluido exit=$EC em $UTC_END"

exit "$EC"
