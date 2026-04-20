"""
Persistência Postgres (Supabase) para clientes e documento de templates.

Ative com DATABASE_URL ou SUPABASE_DATABASE_URL (connection string do pooler).
Se não estiver definido, o projeto continua usando os JSON em data/ (clients, google_clients, message_templates).
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from copy import deepcopy
from typing import Any, Dict, List, Optional

from execution.project_paths import (
    clients_json_path,
    google_clients_json_path,
    message_templates_json_path,
)

logger = logging.getLogger(__name__)
_DB_BOOTSTRAPPED = False

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Json
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore
    Json = None  # type: ignore


def database_url() -> str:
    return (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL") or "").strip()


def db_enabled() -> bool:
    return bool(database_url()) and psycopg is not None and Json is not None


@contextmanager
def _connect():
    if not db_enabled():
        raise RuntimeError("database_not_configured")
    conn = psycopg.connect(database_url(), row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _norm_str_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str) and value.strip():
        return [x.strip() for x in value.split(",") if x.strip()]
    return []


def list_meta_clients() -> List[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, client_name, ad_account_id, group_id, meta_page_id, lead_group_id,
                       lead_phone_number, lead_template, lead_exclude_fields, lead_exclude_contains,
                       lead_exclude_regex, enabled
                FROM meta_clients
                ORDER BY sort_order ASC, id ASC
                """
            )
            rows = cur.fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "client_name": str(r["client_name"] or ""),
                "ad_account_id": str(r["ad_account_id"] or ""),
                "group_id": str(r["group_id"] or ""),
                "meta_page_id": str(r["meta_page_id"] or ""),
                "lead_group_id": str(r["lead_group_id"] or ""),
                "lead_phone_number": str(r["lead_phone_number"] or ""),
                "lead_template": str(r["lead_template"] or "default"),
                "lead_exclude_fields": _json_list(r.get("lead_exclude_fields")),
                "lead_exclude_contains": _json_list(r.get("lead_exclude_contains")),
                "lead_exclude_regex": _json_list(r.get("lead_exclude_regex")),
                "enabled": bool(r.get("enabled", True)),
            }
        )
    return out


def get_meta_client(client_id: int) -> Optional[Dict[str, Any]]:
    for c in list_meta_clients():
        if int(c["id"]) == int(client_id):
            return c
    return None


def insert_meta_client(data: Dict[str, Any]) -> int:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta_clients (
                  client_name, ad_account_id, group_id, meta_page_id, lead_group_id,
                  lead_phone_number, lead_template, lead_exclude_fields, lead_exclude_contains,
                  lead_exclude_regex, enabled
                ) VALUES (
                  %(client_name)s, %(ad_account_id)s, %(group_id)s, %(meta_page_id)s, %(lead_group_id)s,
                  %(lead_phone_number)s, %(lead_template)s, %(lead_exclude_fields)s,
                  %(lead_exclude_contains)s, %(lead_exclude_regex)s, %(enabled)s
                )
                RETURNING id
                """,
                {
                    "client_name": data["client_name"],
                    "ad_account_id": data["ad_account_id"],
                    "group_id": data.get("group_id", ""),
                    "meta_page_id": data.get("meta_page_id", ""),
                    "lead_group_id": data.get("lead_group_id", ""),
                    "lead_phone_number": data.get("lead_phone_number", ""),
                    "lead_template": data.get("lead_template", "default"),
                    "lead_exclude_fields": Json(data.get("lead_exclude_fields") or []),
                    "lead_exclude_contains": Json(data.get("lead_exclude_contains") or []),
                    "lead_exclude_regex": Json(data.get("lead_exclude_regex") or []),
                    "enabled": bool(data.get("enabled", True)),
                },
            )
            row = cur.fetchone()
            return int(row["id"])


def update_meta_client(client_id: int, data: Dict[str, Any]) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE meta_clients SET
                  client_name = %(client_name)s,
                  ad_account_id = %(ad_account_id)s,
                  group_id = %(group_id)s,
                  meta_page_id = %(meta_page_id)s,
                  lead_group_id = %(lead_group_id)s,
                  lead_phone_number = %(lead_phone_number)s,
                  lead_template = %(lead_template)s,
                  lead_exclude_fields = %(lead_exclude_fields)s,
                  lead_exclude_contains = %(lead_exclude_contains)s,
                  lead_exclude_regex = %(lead_exclude_regex)s,
                  enabled = %(enabled)s,
                  updated_at = now()
                WHERE id = %(id)s
                """,
                {
                    "id": client_id,
                    "client_name": data["client_name"],
                    "ad_account_id": data["ad_account_id"],
                    "group_id": data.get("group_id", ""),
                    "meta_page_id": data.get("meta_page_id", ""),
                    "lead_group_id": data.get("lead_group_id", ""),
                    "lead_phone_number": data.get("lead_phone_number", ""),
                    "lead_template": data.get("lead_template", "default"),
                    "lead_exclude_fields": Json(data.get("lead_exclude_fields") or []),
                    "lead_exclude_contains": Json(data.get("lead_exclude_contains") or []),
                    "lead_exclude_regex": Json(data.get("lead_exclude_regex") or []),
                    "enabled": bool(data.get("enabled", True)),
                },
            )


def list_google_clients() -> List[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, client_name, google_customer_id, group_id, notes, google_template,
                       primary_conversions, enabled
                FROM google_clients
                ORDER BY sort_order ASC, id ASC
                """
            )
            rows = cur.fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        pc = r.get("primary_conversions")
        if not isinstance(pc, list):
            pc = _json_list(pc)
        out.append(
            {
                "id": int(r["id"]),
                "client_name": str(r["client_name"] or ""),
                "google_customer_id": str(r["google_customer_id"] or ""),
                "group_id": str(r["group_id"] or ""),
                "notes": str(r["notes"] or ""),
                "google_template": str(r["google_template"] or "default"),
                "primary_conversions": [str(x).strip() for x in pc if str(x).strip()],
                "enabled": bool(r.get("enabled", True)),
            }
        )
    return out


def get_google_client(client_id: int) -> Optional[Dict[str, Any]]:
    for c in list_google_clients():
        if int(c["id"]) == int(client_id):
            return c
    return None


def insert_google_client(data: Dict[str, Any]) -> int:
    primary = data.get("primary_conversions") or []
    if not isinstance(primary, list):
        primary = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO google_clients (
                  client_name, google_customer_id, group_id, notes, google_template,
                  primary_conversions, enabled
                ) VALUES (
                  %(client_name)s, %(google_customer_id)s, %(group_id)s, %(notes)s, %(google_template)s,
                  %(primary_conversions)s, %(enabled)s
                )
                RETURNING id
                """,
                {
                    "client_name": data["client_name"],
                    "google_customer_id": data["google_customer_id"],
                    "group_id": data.get("group_id", ""),
                    "notes": data.get("notes", ""),
                    "google_template": data.get("google_template", "default"),
                    "primary_conversions": Json(primary),
                    "enabled": bool(data.get("enabled", True)),
                },
            )
            return int(cur.fetchone()["id"])


def update_google_client(client_id: int, data: Dict[str, Any]) -> None:
    primary = data.get("primary_conversions") or []
    if not isinstance(primary, list):
        primary = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE google_clients SET
                  client_name = %(client_name)s,
                  google_customer_id = %(google_customer_id)s,
                  group_id = %(group_id)s,
                  notes = %(notes)s,
                  google_template = %(google_template)s,
                  primary_conversions = %(primary_conversions)s,
                  enabled = %(enabled)s,
                  updated_at = now()
                WHERE id = %(id)s
                """,
                {
                    "id": client_id,
                    "client_name": data["client_name"],
                    "google_customer_id": data["google_customer_id"],
                    "group_id": data.get("group_id", ""),
                    "notes": data.get("notes", ""),
                    "google_template": data.get("google_template", "default"),
                    "primary_conversions": Json(primary),
                    "enabled": bool(data.get("enabled", True)),
                },
            )


def get_message_templates_body() -> Dict[str, Any]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT body FROM message_templates_doc WHERE id = 1")
            row = cur.fetchone()
    if not row:
        return {}
    body = row.get("body")
    return body if isinstance(body, dict) else {}


def save_message_templates_body(body: Dict[str, Any]) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO message_templates_doc (id, body, updated_at)
                VALUES (1, %s, now())
                ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, updated_at = now()
                """,
                (Json(body),),
            )


def save_template_channels(channels: Dict[str, Any]) -> None:
    """Atualiza só chaves de canais (meta_lead, google_report, …), preservando filters no documento."""
    body = get_message_templates_body()
    for k, v in channels.items():
        if k == "filters":
            continue
        if isinstance(v, dict):
            body[k] = deepcopy(v)
    save_message_templates_body(body)


def seed_from_json_files_if_empty() -> None:
    """Importa JSON locais na primeira vez (tabelas vazias)."""
    if not db_enabled():
        return
    clients_path = clients_json_path()
    google_path = google_clients_json_path()
    tpl_path = message_templates_json_path()

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::int AS c FROM meta_clients")
            n_meta = int(cur.fetchone()["c"])
            if n_meta == 0 and os.path.isfile(clients_path):
                with open(clients_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, list):
                    for row in raw:
                        if not isinstance(row, dict):
                            continue
                        cur.execute(
                            """
                            INSERT INTO meta_clients (
                              client_name, ad_account_id, group_id, meta_page_id, lead_group_id,
                              lead_phone_number, lead_template, lead_exclude_fields, lead_exclude_contains,
                              lead_exclude_regex, enabled
                            ) VALUES (
                              %(client_name)s, %(ad_account_id)s, %(group_id)s, %(meta_page_id)s, %(lead_group_id)s,
                              %(lead_phone_number)s, %(lead_template)s, %(ef)s, %(ec)s, %(er)s, %(en)s
                            )
                            """,
                            {
                                "client_name": str(row.get("client_name", "")).strip(),
                                "ad_account_id": str(row.get("ad_account_id", "")).strip(),
                                "group_id": str(row.get("group_id", "")).strip(),
                                "meta_page_id": str(row.get("meta_page_id", "")).strip(),
                                "lead_group_id": str(row.get("lead_group_id", "")).strip(),
                                "lead_phone_number": str(row.get("lead_phone_number", "")).strip(),
                                "lead_template": str(row.get("lead_template", "default")).strip() or "default",
                                "ef": Json(_norm_str_list(row.get("lead_exclude_fields"))),
                                "ec": Json(_norm_str_list(row.get("lead_exclude_contains"))),
                                "er": Json(_norm_str_list(row.get("lead_exclude_regex"))),
                                "en": bool(row.get("enabled", True)),
                            },
                        )
                    logger.info("Seed: importados %s cliente(s) Meta de clients.json", len(raw))

            cur.execute("SELECT COUNT(*)::int AS c FROM google_clients")
            n_google = int(cur.fetchone()["c"])
            if n_google == 0 and os.path.isfile(google_path):
                with open(google_path, "r", encoding="utf-8") as f:
                    graw = json.load(f)
                if isinstance(graw, list):
                    for row in graw:
                        if not isinstance(row, dict):
                            continue
                        primary = row.get("primary_conversions")
                        if not isinstance(primary, list):
                            primary = []
                        cur.execute(
                            """
                            INSERT INTO google_clients (
                              client_name, google_customer_id, group_id, notes, google_template,
                              primary_conversions, enabled
                            ) VALUES (
                              %(client_name)s, %(google_customer_id)s, %(group_id)s, %(notes)s, %(google_template)s,
                              %(pc)s, %(en)s
                            )
                            """,
                            {
                                "client_name": str(row.get("client_name", "")).strip(),
                                "google_customer_id": str(row.get("google_customer_id", "")).strip(),
                                "group_id": str(row.get("group_id", "")).strip(),
                                "notes": str(row.get("notes", "")).strip(),
                                "google_template": str(row.get("google_template", "default")).strip() or "default",
                                "pc": Json(primary),
                                "en": bool(row.get("enabled", True)),
                            },
                        )
                    logger.info("Seed: importados %s cliente(s) Google de google_clients.json", len(graw))

            cur.execute("SELECT body FROM message_templates_doc WHERE id = 1")
            doc_row = cur.fetchone()
            body = doc_row.get("body") if doc_row else {}
            empty_body = not body or (isinstance(body, dict) and len(body) == 0)
            if empty_body and os.path.isfile(tpl_path):
                with open(tpl_path, "r", encoding="utf-8") as f:
                    merged = json.load(f)
                if isinstance(merged, dict) and merged:
                    cur.execute(
                        """
                        INSERT INTO message_templates_doc (id, body, updated_at)
                        VALUES (1, %s, now())
                        ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, updated_at = now()
                        """,
                        (Json(merged),),
                    )
                    logger.info("Seed: message_templates.json copiado para message_templates_doc")


def ensure_db_ready() -> None:
    """Chamada lazy: importa JSON se BD vazio (uma vez por processo)."""
    global _DB_BOOTSTRAPPED
    if _DB_BOOTSTRAPPED or not db_enabled():
        return
    _DB_BOOTSTRAPPED = True
    try:
        seed_from_json_files_if_empty()
    except Exception as e:
        logger.warning("Falha ao preparar dados no Postgres: %s", e)
