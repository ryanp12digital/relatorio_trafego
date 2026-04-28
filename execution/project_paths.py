"""
Caminhos do repositório: dados em data/ na raiz do projeto (Docker WORKDIR /app).
"""

from __future__ import annotations

import os

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(_ROOT, "data")


def ensure_data_dir() -> None:
    """Garante que data/ existe (útil em deploy com volume vazio)."""
    os.makedirs(DATA_DIR, exist_ok=True)


def repo_root() -> str:
    return _ROOT


def data_dir() -> str:
    return DATA_DIR


def clients_json_path() -> str:
    return os.path.join(DATA_DIR, "clients.json")


def google_clients_json_path() -> str:
    return os.path.join(DATA_DIR, "google_clients.json")


def message_templates_json_path() -> str:
    return os.path.join(DATA_DIR, "message_templates.json")


def catalog_groups_json_path() -> str:
    return os.path.join(DATA_DIR, "catalog_groups.json")


def catalog_webhook_listener_json_path() -> str:
    """Liga/desliga processamento global do POST /evolution-webhook (partilhado entre processos)."""
    return os.path.join(DATA_DIR, "catalog_webhook_listener.json")


def site_lead_routes_json_path() -> str:
    """Regras de roteamento para leads de site (form_id -> cliente destino)."""
    return os.path.join(DATA_DIR, "site_lead_routes.json")
