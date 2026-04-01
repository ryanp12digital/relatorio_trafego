"""
Gera o relatorio completo (absoluto + comparativo em uma unica mensagem) e envia para EVOLUTION_GROUP_ID.

Prioridade (sem argumentos):
1) Entrada em clients.json com o mesmo group_id do .env (conta + nome do cliente).
2) Se nao houver match: usa META_AD_ACCOUNT_ID e nome REPORT_CLIENT_NAME ou "Cliente".

Com argumentos:
  python execution/send_report_to_env_group.py 535390208581579 --nome "Nome no relatorio"

Requer META_ACCESS_TOKEN, Evolution configurada, EVOLUTION_GROUP_ID.
META_BUSINESS_ID e opcional para este script (nao usada no envio unico).
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.main_scheduler import P12RelatoriosReporter


def _normalize_act(raw: str) -> str:
    s = raw.strip().replace(" ", "")
    if s.startswith("act_"):
        return s
    return f"act_{s}"


def _resolve_target() -> tuple[str, str]:
    group_id = (os.getenv("EVOLUTION_GROUP_ID") or "").strip()
    if not group_id:
        raise SystemExit("Defina EVOLUTION_GROUP_ID no .env.")

    clients_path = os.path.join(
        os.path.dirname(__file__), "..", "clients.json"
    )
    if os.path.isfile(clients_path):
        with open(clients_path, encoding="utf-8") as f:
            clients = json.load(f)
        for c in clients:
            if not isinstance(c, dict):
                continue
            if (c.get("group_id") or "").strip() != group_id:
                continue
            name = (c.get("client_name") or "Cliente").strip()
            ad = _normalize_act(str(c.get("ad_account_id", "")))
            if ad == "act_":
                break
            return name, ad

    ad_raw = os.getenv("META_AD_ACCOUNT_ID") or ""
    if not ad_raw.strip():
        raise SystemExit(
            "Nenhum cliente no clients.json com este EVOLUTION_GROUP_ID "
            "e META_AD_ACCOUNT_ID vazio no .env."
        )
    name = (os.getenv("REPORT_CLIENT_NAME") or "Cliente").strip()
    return name, _normalize_act(ad_raw)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Envia relatorio Meta Ads para EVOLUTION_GROUP_ID."
    )
    parser.add_argument(
        "ad_account_id",
        nargs="?",
        default=None,
        help="ID numerico da conta (ex.: 535390208581579) ou act_XXX; opcional",
    )
    parser.add_argument(
        "--nome",
        "-n",
        default=None,
        help="Nome exibido no titulo do relatorio (obrigatorio se passar conta na linha de comando)",
    )
    args = parser.parse_args()

    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)

    if args.ad_account_id:
        if not (args.nome or "").strip():
            parser.error(
                'Informe o nome no relatorio: --nome "Nome do cliente"'
            )
        client_name = args.nome.strip()
        ad_account_id = _normalize_act(args.ad_account_id)
    else:
        client_name, ad_account_id = _resolve_target()

    group_id = os.getenv("EVOLUTION_GROUP_ID", "").strip()
    if not group_id:
        raise SystemExit("Defina EVOLUTION_GROUP_ID no .env.")


    print(f"Cliente: {client_name}")
    print(f"Conta: {ad_account_id}")
    print(f"Grupo: {group_id}")

    reporter = P12RelatoriosReporter()
    ok = reporter.generate_and_send_report_for_client(
        client_name,
        ad_account_id,
        group_id,
        send_if_zero_spend=True,
    )
    if not ok:
        sys.exit(1)
    print("Relatorio enviado (1 mensagem com os dois blocos).")


if __name__ == "__main__":
    main()
