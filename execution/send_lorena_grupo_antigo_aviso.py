"""
Envio único: mensagem de teste/aviso no grupo antigo da Lorena antes da troca de group_id.

Execute uma vez (com .env e Evolution OK), depois faça deploy com data/clients.json já no grupo novo.

  python execution/send_lorena_grupo_antigo_aviso.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from execution.evolution_client import get_evolution_client

# Grupo anterior (antes de 120363408539276524@g.us)
LORENA_GRUPO_ANTIGO = "120363419835081376@g.us"

MENSAGEM = (
    "[P12 Relatórios — teste de migração]\n\n"
    "A partir de agora, o relatório semanal Meta Ads e os avisos de lead novo da Lorena "
    "passam a ser enviados para o grupo novo.\n\n"
    "Esta mensagem é só um teste no grupo atual antes da troca. "
    "Se você leu isso aqui, o WhatsApp está OK."
)


def main() -> None:
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)
    if os.getenv("DRY_RUN", "false").lower() == "true":
        print("DRY_RUN=true: não enviando. Mensagem seria:\n", MENSAGEM)
        return
    client = get_evolution_client()
    if client.send_text_message(LORENA_GRUPO_ANTIGO, MENSAGEM):
        print(f"Enviado com sucesso para {LORENA_GRUPO_ANTIGO}")
    else:
        print("Falha ao enviar.")
        sys.exit(1)


if __name__ == "__main__":
    main()
