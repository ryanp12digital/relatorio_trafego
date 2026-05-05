"""
Persistência Postgres (Supabase) para clientes e documento de templates.

Ative com DATABASE_URL ou SUPABASE_DATABASE_URL (connection string do pooler).
Se não estiver definido, o projeto continua usando os JSON em data/ (clients, google_clients, message_templates).
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from execution.project_paths import (
    catalog_groups_json_path,
    catalog_webhook_listener_json_path,
    clients_json_path,
    ensure_data_dir,
    google_clients_json_path,
    message_templates_json_path,
    site_lead_routes_json_path,
)

logger = logging.getLogger(__name__)
_DB_BOOTSTRAPPED = False
_PG_POOL: Any = None
_PG_POOL_LOCK = threading.Lock()
_CATALOG_JSON_LOCK = threading.RLock()
_LISTENER_JSON_LOCK = threading.RLock()
_SITE_ROUTES_JSON_LOCK = threading.RLock()
_CATALOG_WEBHOOK_LISTENING_DEFAULT = True

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Json
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore
    Json = None  # type: ignore

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover
    ConnectionPool = None  # type: ignore[misc, assignment]


def database_url() -> str:
    return (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL") or "").strip()


def db_enabled() -> bool:
    return bool(database_url()) and psycopg is not None and Json is not None


def _pool_bounds() -> Tuple[int, int]:
    try:
        mx = int((os.environ.get("DATABASE_POOL_MAX") or "10").strip() or "10")
    except ValueError:
        mx = 10
    try:
        mn = int((os.environ.get("DATABASE_POOL_MIN") or "1").strip() or "1")
    except ValueError:
        mn = 1
    mn = max(1, min(mn, 50))
    mx = max(1, min(mx, 50))
    if mx < mn:
        mx = mn
    return mn, mx


def _psycopg_connect_kwargs() -> Dict[str, Any]:
    """
    kwargs de conexão compatíveis com poolers (ex.: Supabase PgBouncer).

    Por padrão, desativa prepared statements automáticos do psycopg (`prepare_threshold=0`),
    evitando erros como:
    - prepared statement "... already exists"
    - prepared statement "... does not exist"
    """
    kwargs: Dict[str, Any] = {"row_factory": dict_row}
    raw = (os.environ.get("PSYCOPG_PREPARE_THRESHOLD") or "0").strip()
    try:
        kwargs["prepare_threshold"] = int(raw)
    except ValueError:
        kwargs["prepare_threshold"] = 0
    return kwargs


def _connection_pool() -> Any:
    global _PG_POOL
    if ConnectionPool is None:
        return None
    with _PG_POOL_LOCK:
        if _PG_POOL is None:
            mn, mx = _pool_bounds()
            _PG_POOL = ConnectionPool(
                conninfo=database_url(),
                min_size=mn,
                max_size=mx,
                kwargs=_psycopg_connect_kwargs(),
            )
            logger.info("Postgres: pool iniciado (min=%s max=%s).", mn, mx)
        return _PG_POOL


@contextmanager
def _connect():
    if not db_enabled():
        raise RuntimeError("database_not_configured")
    pool = _connection_pool()
    if pool is not None:
        with pool.connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return
    conn = psycopg.connect(database_url(), **_psycopg_connect_kwargs())
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
                       lead_exclude_regex, enabled,
                       p12_report_group_id, p12_report_template, p12_data_report_template,
                       internal_notify_group_id, internal_notify_message,
                       internal_lead_template, internal_weekly_template
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
                "p12_report_group_id": str(r.get("p12_report_group_id") or ""),
                "p12_report_template": str(r.get("p12_report_template") or ""),
                "p12_data_report_template": str(r.get("p12_data_report_template") or ""),
                "internal_notify_group_id": str(r.get("internal_notify_group_id") or ""),
                "internal_notify_message": str(r.get("internal_notify_message") or ""),
                "internal_lead_template": str(r.get("internal_lead_template") or ""),
                "internal_weekly_template": str(r.get("internal_weekly_template") or ""),
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
                  lead_exclude_regex, enabled,
                  p12_report_group_id, p12_report_template, p12_data_report_template,
                  internal_notify_group_id, internal_notify_message,
                  internal_lead_template, internal_weekly_template
                ) VALUES (
                  %(client_name)s, %(ad_account_id)s, %(group_id)s, %(meta_page_id)s, %(lead_group_id)s,
                  %(lead_phone_number)s, %(lead_template)s, %(lead_exclude_fields)s,
                  %(lead_exclude_contains)s, %(lead_exclude_regex)s, %(enabled)s,
                  %(p12_report_group_id)s, %(p12_report_template)s, %(p12_data_report_template)s,
                  %(internal_notify_group_id)s, %(internal_notify_message)s,
                  %(internal_lead_template)s, %(internal_weekly_template)s
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
                    "p12_report_group_id": data.get("p12_report_group_id", ""),
                    "p12_report_template": data.get("p12_report_template", ""),
                    "p12_data_report_template": data.get("p12_data_report_template", ""),
                    "internal_notify_group_id": data.get("internal_notify_group_id", ""),
                    "internal_notify_message": data.get("internal_notify_message", ""),
                    "internal_lead_template": data.get("internal_lead_template", ""),
                    "internal_weekly_template": data.get("internal_weekly_template", ""),
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
                  p12_report_group_id = %(p12_report_group_id)s,
                  p12_report_template = %(p12_report_template)s,
                  p12_data_report_template = %(p12_data_report_template)s,
                  internal_notify_group_id = %(internal_notify_group_id)s,
                  internal_notify_message = %(internal_notify_message)s,
                  internal_lead_template = %(internal_lead_template)s,
                  internal_weekly_template = %(internal_weekly_template)s,
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
                    "p12_report_group_id": data.get("p12_report_group_id", ""),
                    "p12_report_template": data.get("p12_report_template", ""),
                    "p12_data_report_template": data.get("p12_data_report_template", ""),
                    "internal_notify_group_id": data.get("internal_notify_group_id", ""),
                    "internal_notify_message": data.get("internal_notify_message", ""),
                    "internal_lead_template": data.get("internal_lead_template", ""),
                    "internal_weekly_template": data.get("internal_weekly_template", ""),
                },
            )


def list_google_clients() -> List[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, client_name, google_customer_id, group_id, notes, google_template,
                       primary_conversions, enabled,
                       lead_phone_number, p12_report_group_id, p12_report_template,
                       p12_data_report_template, internal_notify_group_id, internal_notify_message,
                       internal_lead_template, internal_weekly_template
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
                "lead_phone_number": str(r.get("lead_phone_number") or ""),
                "p12_report_group_id": str(r.get("p12_report_group_id") or ""),
                "p12_report_template": str(r.get("p12_report_template") or ""),
                "p12_data_report_template": str(r.get("p12_data_report_template") or ""),
                "internal_notify_group_id": str(r.get("internal_notify_group_id") or ""),
                "internal_notify_message": str(r.get("internal_notify_message") or ""),
                "internal_lead_template": str(r.get("internal_lead_template") or ""),
                "internal_weekly_template": str(r.get("internal_weekly_template") or ""),
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
                  primary_conversions, enabled,
                  lead_phone_number, p12_report_group_id, p12_report_template,
                  p12_data_report_template, internal_notify_group_id, internal_notify_message,
                  internal_lead_template, internal_weekly_template
                ) VALUES (
                  %(client_name)s, %(google_customer_id)s, %(group_id)s, %(notes)s, %(google_template)s,
                  %(primary_conversions)s, %(enabled)s,
                  %(lead_phone_number)s, %(p12_report_group_id)s, %(p12_report_template)s,
                  %(p12_data_report_template)s, %(internal_notify_group_id)s, %(internal_notify_message)s,
                  %(internal_lead_template)s, %(internal_weekly_template)s
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
                    "lead_phone_number": data.get("lead_phone_number", ""),
                    "p12_report_group_id": data.get("p12_report_group_id", ""),
                    "p12_report_template": data.get("p12_report_template", ""),
                    "p12_data_report_template": data.get("p12_data_report_template", ""),
                    "internal_notify_group_id": data.get("internal_notify_group_id", ""),
                    "internal_notify_message": data.get("internal_notify_message", ""),
                    "internal_lead_template": data.get("internal_lead_template", ""),
                    "internal_weekly_template": data.get("internal_weekly_template", ""),
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
                  lead_phone_number = %(lead_phone_number)s,
                  p12_report_group_id = %(p12_report_group_id)s,
                  p12_report_template = %(p12_report_template)s,
                  p12_data_report_template = %(p12_data_report_template)s,
                  internal_notify_group_id = %(internal_notify_group_id)s,
                  internal_notify_message = %(internal_notify_message)s,
                  internal_lead_template = %(internal_lead_template)s,
                  internal_weekly_template = %(internal_weekly_template)s,
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
                    "lead_phone_number": data.get("lead_phone_number", ""),
                    "p12_report_group_id": data.get("p12_report_group_id", ""),
                    "p12_report_template": data.get("p12_report_template", ""),
                    "p12_data_report_template": data.get("p12_data_report_template", ""),
                    "internal_notify_group_id": data.get("internal_notify_group_id", ""),
                    "internal_notify_message": data.get("internal_notify_message", ""),
                    "internal_lead_template": data.get("internal_lead_template", ""),
                    "internal_weekly_template": data.get("internal_weekly_template", ""),
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
    _skip = frozenset({"filters", "variable_resolution", "custom_variables"})
    for k, v in channels.items():
        if k in _skip:
            continue
        if isinstance(v, dict):
            body[k] = deepcopy(v)
    save_message_templates_body(body)


def _migrate_db_schema() -> None:
    """ADD COLUMN para clientes Meta/Google (idempotente; Supabase/Postgres)."""
    if not db_enabled():
        return
    meta_sql = [
        "ALTER TABLE meta_clients ADD COLUMN IF NOT EXISTS p12_report_group_id text NOT NULL DEFAULT ''",
        "ALTER TABLE meta_clients ADD COLUMN IF NOT EXISTS p12_report_template text NOT NULL DEFAULT ''",
        "ALTER TABLE meta_clients ADD COLUMN IF NOT EXISTS p12_data_report_template text NOT NULL DEFAULT ''",
        "ALTER TABLE meta_clients ADD COLUMN IF NOT EXISTS internal_notify_group_id text NOT NULL DEFAULT ''",
        "ALTER TABLE meta_clients ADD COLUMN IF NOT EXISTS internal_notify_message text NOT NULL DEFAULT ''",
        "ALTER TABLE meta_clients ADD COLUMN IF NOT EXISTS internal_lead_template text NOT NULL DEFAULT ''",
        "ALTER TABLE meta_clients ADD COLUMN IF NOT EXISTS internal_weekly_template text NOT NULL DEFAULT ''",
    ]
    google_sql = [
        "ALTER TABLE google_clients ADD COLUMN IF NOT EXISTS lead_phone_number text NOT NULL DEFAULT ''",
        "ALTER TABLE google_clients ADD COLUMN IF NOT EXISTS p12_report_group_id text NOT NULL DEFAULT ''",
        "ALTER TABLE google_clients ADD COLUMN IF NOT EXISTS p12_report_template text NOT NULL DEFAULT ''",
        "ALTER TABLE google_clients ADD COLUMN IF NOT EXISTS p12_data_report_template text NOT NULL DEFAULT ''",
        "ALTER TABLE google_clients ADD COLUMN IF NOT EXISTS internal_notify_group_id text NOT NULL DEFAULT ''",
        "ALTER TABLE google_clients ADD COLUMN IF NOT EXISTS internal_notify_message text NOT NULL DEFAULT ''",
        "ALTER TABLE google_clients ADD COLUMN IF NOT EXISTS internal_lead_template text NOT NULL DEFAULT ''",
        "ALTER TABLE google_clients ADD COLUMN IF NOT EXISTS internal_weekly_template text NOT NULL DEFAULT ''",
    ]
    site_routes_sql = [
        """
        CREATE TABLE IF NOT EXISTS site_lead_routes (
          id bigserial PRIMARY KEY,
          form_id text NOT NULL UNIQUE,
          target_type text NOT NULL DEFAULT 'meta',
          target_client_name text NOT NULL DEFAULT '',
          group_id text NOT NULL DEFAULT '',
          lead_group_id text NOT NULL DEFAULT '',
          lead_phone_number text NOT NULL DEFAULT '',
          internal_notify_group_id text NOT NULL DEFAULT '',
          source_type text NOT NULL DEFAULT '',
          lead_template text NOT NULL DEFAULT 'default',
          internal_lead_template text NOT NULL DEFAULT '',
          lead_exclude_fields jsonb NOT NULL DEFAULT '[]'::jsonb,
          lead_exclude_contains jsonb NOT NULL DEFAULT '[]'::jsonb,
          lead_exclude_regex jsonb NOT NULL DEFAULT '[]'::jsonb,
          origem_anuncio text NOT NULL DEFAULT '',
          cliente_origem text NOT NULL DEFAULT '',
          enabled boolean NOT NULL DEFAULT true,
          notes text NOT NULL DEFAULT '',
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    ]
    site_routes_migrate_sql = [
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS group_id text NOT NULL DEFAULT ''",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS lead_group_id text NOT NULL DEFAULT ''",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS lead_phone_number text NOT NULL DEFAULT ''",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS internal_notify_group_id text NOT NULL DEFAULT ''",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS lead_template text NOT NULL DEFAULT 'default'",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS internal_lead_template text NOT NULL DEFAULT ''",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS lead_exclude_fields jsonb NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS lead_exclude_contains jsonb NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS lead_exclude_regex jsonb NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS origem_anuncio text NOT NULL DEFAULT ''",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS cliente_origem text NOT NULL DEFAULT ''",
        "ALTER TABLE site_lead_routes ADD COLUMN IF NOT EXISTS cors_allowed_origins jsonb NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE site_lead_routes ENABLE ROW LEVEL SECURITY",
    ]
    with _connect() as conn:
        with conn.cursor() as cur:
            for stmt in meta_sql + google_sql + site_routes_sql + site_routes_migrate_sql:
                cur.execute(stmt)


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
        if db_enabled():
            _migrate_db_schema()
        seed_from_json_files_if_empty()
    except Exception as e:
        logger.warning("Falha ao preparar dados no Postgres: %s", e)


def _catalog_row_from_db(r: Dict[str, Any]) -> Dict[str, Any]:
    la = r.get("last_activity_at")
    ua = r.get("updated_at")
    return {
        "group_jid": str(r.get("group_jid") or ""),
        "subject": str(r.get("subject") or ""),
        "monitoring_enabled": bool(r.get("monitoring_enabled", True)),
        "last_activity_at": la.isoformat() if hasattr(la, "isoformat") else str(la or ""),
        "last_event_type": str(r.get("last_event_type") or ""),
        "last_push_name": str(r.get("last_push_name") or ""),
        "last_preview": str(r.get("last_preview") or ""),
        "updated_at": ua.isoformat() if hasattr(ua, "isoformat") else str(ua or ""),
    }


def _load_catalog_json() -> List[Dict[str, Any]]:
    path = catalog_groups_json_path()
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def _save_catalog_json(rows: List[Dict[str, Any]]) -> None:
    ensure_data_dir()
    path = catalog_groups_json_path()
    with _CATALOG_JSON_LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
            f.write("\n")


def list_catalog_groups() -> List[Dict[str, Any]]:
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT group_jid, subject, monitoring_enabled, last_activity_at,
                           last_event_type, last_push_name, last_preview, updated_at
                    FROM whatsapp_catalog_groups
                    ORDER BY last_activity_at DESC
                    """
                )
                return [_catalog_row_from_db(dict(r)) for r in cur.fetchall()]
    rows = _load_catalog_json()
    rows.sort(key=lambda x: str(x.get("last_activity_at") or ""), reverse=True)
    return rows


def get_catalog_group(group_jid: str) -> Optional[Dict[str, Any]]:
    gj = (group_jid or "").strip()
    if not gj:
        return None
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT group_jid, subject, monitoring_enabled, last_activity_at,
                           last_event_type, last_push_name, last_preview, updated_at
                    FROM whatsapp_catalog_groups WHERE group_jid = %s
                    """,
                    (gj,),
                )
                r = cur.fetchone()
                return _catalog_row_from_db(dict(r)) if r else None
    for row in _load_catalog_json():
        if str(row.get("group_jid", "")).strip() == gj:
            return row
    return None


def catalog_group_should_process(group_jid: str) -> bool:
    """False se grupo existe e monitoring_enabled=false; True se novo ou monitoring ligado."""
    row = get_catalog_group(group_jid)
    if row is None:
        return True
    return bool(row.get("monitoring_enabled", True))


def new_catalog_group_monitoring_default() -> bool:
    """
    Valor inicial de monitoring_enabled ao criar um grupo novo.

    Sempre False: o catálogo só passa a actualizar actividade desse JID depois de alguém
    enviar no grupo a frase obrigatória «Ativar grupo» (ver evolution_catalog_webhook).
    """
    return False


def upsert_catalog_group_activity(
    group_jid: str,
    *,
    event_type: str = "",
    push_name: str = "",
    preview: str = "",
) -> bool:
    """
    Regista actividade no grupo. Retorna False se monitoring desligado (não altera).
    """
    gj = (group_jid or "").strip()
    if not gj or not gj.endswith("@g.us"):
        return False
    if not catalog_group_should_process(gj):
        return False
    et = (event_type or "")[:120]
    pn = (push_name or "")[:200]
    pv = (preview or "")[:500]
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT monitoring_enabled FROM whatsapp_catalog_groups WHERE group_jid = %s",
                    (gj,),
                )
                ex = cur.fetchone()
                if ex and not bool(ex.get("monitoring_enabled", True)):
                    return False
                mon_ins = new_catalog_group_monitoring_default()
                cur.execute(
                    """
                    INSERT INTO whatsapp_catalog_groups (
                      group_jid, subject, monitoring_enabled, last_activity_at,
                      last_event_type, last_push_name, last_preview, updated_at
                    ) VALUES (%s, '', %s, now(), %s, %s, %s, now())
                    ON CONFLICT (group_jid) DO UPDATE SET
                      last_activity_at = now(),
                      last_event_type = EXCLUDED.last_event_type,
                      last_push_name = EXCLUDED.last_push_name,
                      last_preview = EXCLUDED.last_preview,
                      updated_at = now()
                    WHERE whatsapp_catalog_groups.monitoring_enabled = true
                    """,
                    (gj, mon_ins, et, pn, pv),
                )
        return True
    with _CATALOG_JSON_LOCK:
        rows = _load_catalog_json()
        idx = next((i for i, r in enumerate(rows) if str(r.get("group_jid", "")).strip() == gj), None)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        if idx is not None:
            if not bool(rows[idx].get("monitoring_enabled", True)):
                return False
            rows[idx]["last_activity_at"] = now_iso
            rows[idx]["last_event_type"] = et
            rows[idx]["last_push_name"] = pn
            rows[idx]["last_preview"] = pv
            rows[idx]["updated_at"] = now_iso
        else:
            rows.append(
                {
                    "group_jid": gj,
                    "subject": "",
                    "monitoring_enabled": new_catalog_group_monitoring_default(),
                    "last_activity_at": now_iso,
                    "last_event_type": et,
                    "last_push_name": pn,
                    "last_preview": pv,
                    "updated_at": now_iso,
                }
            )
        _save_catalog_json(rows)
    return True


def update_catalog_group_subject(group_jid: str, subject: str) -> None:
    gj = (group_jid or "").strip()
    sub = (subject or "").strip()[:500]
    if not gj:
        return
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE whatsapp_catalog_groups
                    SET subject = %s, updated_at = now()
                    WHERE group_jid = %s
                    """,
                    (sub, gj),
                )
        return
    with _CATALOG_JSON_LOCK:
        rows = _load_catalog_json()
        for r in rows:
            if str(r.get("group_jid", "")).strip() == gj:
                r["subject"] = sub
                r["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
                break
        else:
            rows.append(
                {
                    "group_jid": gj,
                    "subject": sub,
                    "monitoring_enabled": True,
                    "last_activity_at": "",
                    "last_event_type": "",
                    "last_push_name": "",
                    "last_preview": "",
                    "updated_at": "",
                }
            )
        _save_catalog_json(rows)


def set_catalog_group_monitoring(group_jid: str, enabled: bool) -> None:
    gj = (group_jid or "").strip()
    if not gj:
        return
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO whatsapp_catalog_groups (group_jid, monitoring_enabled, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (group_jid) DO UPDATE SET
                      monitoring_enabled = EXCLUDED.monitoring_enabled,
                      updated_at = now()
                    """,
                    (gj, bool(enabled)),
                )
        return
    with _CATALOG_JSON_LOCK:
        rows = _load_catalog_json()
        for r in rows:
            if str(r.get("group_jid", "")).strip() == gj:
                r["monitoring_enabled"] = bool(enabled)
                r["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
                _save_catalog_json(rows)
                return
        rows.append(
            {
                "group_jid": gj,
                "subject": "",
                "monitoring_enabled": bool(enabled),
                "last_activity_at": "",
                "last_event_type": "",
                "last_push_name": "",
                "last_preview": "",
                "updated_at": "",
            }
        )
        _save_catalog_json(rows)


def patch_catalog_group_manual(
    group_jid: str,
    *,
    subject: Optional[str] = None,
    monitoring_enabled: Optional[bool] = None,
) -> Optional[Dict[str, Any]]:
    """Atualiza subject e/ou monitoring; retorna linha actualizada ou None se não existir (BD)."""
    gj = (group_jid or "").strip()
    if not gj:
        return None
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM whatsapp_catalog_groups WHERE group_jid = %s", (gj,))
                if not cur.fetchone():
                    return None
                if subject is not None:
                    cur.execute(
                        "UPDATE whatsapp_catalog_groups SET subject = %s, updated_at = now() WHERE group_jid = %s",
                        ((subject or "").strip()[:500], gj),
                    )
                if monitoring_enabled is not None:
                    cur.execute(
                        """
                        UPDATE whatsapp_catalog_groups
                        SET monitoring_enabled = %s, updated_at = now()
                        WHERE group_jid = %s
                        """,
                        (bool(monitoring_enabled), gj),
                    )
        return get_catalog_group(gj)
    with _CATALOG_JSON_LOCK:
        rows = _load_catalog_json()
        found = None
        for r in rows:
            if str(r.get("group_jid", "")).strip() == gj:
                found = r
                break
        if not found:
            return None
        if subject is not None:
            found["subject"] = (subject or "").strip()[:500]
        if monitoring_enabled is not None:
            found["monitoring_enabled"] = bool(monitoring_enabled)
        found["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        _save_catalog_json(rows)
    return get_catalog_group(gj)


def delete_catalog_group(group_jid: str) -> bool:
    """Remove o grupo do catálogo. Retorna True se existia e foi apagado."""
    gj = (group_jid or "").strip()
    if not gj:
        return False
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM whatsapp_catalog_groups WHERE group_jid = %s", (gj,))
                return cur.rowcount > 0
    with _CATALOG_JSON_LOCK:
        rows = _load_catalog_json()
        new_rows = [r for r in rows if str(r.get("group_jid", "")).strip() != gj]
        if len(new_rows) == len(rows):
            return False
        _save_catalog_json(new_rows)
        return True


def get_catalog_webhook_listening() -> bool:
    """
    Se False, POST /evolution-webhook responde 200 sem normalizar nem gravar (menos carga).
    Estado em data/catalog_webhook_listener.json (visível por dashboard e meta_lead_webhook).
    """
    path = catalog_webhook_listener_json_path()
    if not os.path.isfile(path):
        return _CATALOG_WEBHOOK_LISTENING_DEFAULT
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return _CATALOG_WEBHOOK_LISTENING_DEFAULT
        return bool(raw.get("listening", _CATALOG_WEBHOOK_LISTENING_DEFAULT))
    except (OSError, json.JSONDecodeError):
        return _CATALOG_WEBHOOK_LISTENING_DEFAULT


def set_catalog_webhook_listening(listening: bool) -> None:
    ensure_data_dir()
    path = catalog_webhook_listener_json_path()
    payload = {"listening": bool(listening)}
    with _LISTENER_JSON_LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")


def _load_site_routes_json() -> List[Dict[str, Any]]:
    path = site_lead_routes_json_path()
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def _save_site_routes_json(rows: List[Dict[str, Any]]) -> None:
    ensure_data_dir()
    path = site_lead_routes_json_path()
    with _SITE_ROUTES_JSON_LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
            f.write("\n")


def _site_route_row(row: Dict[str, Any]) -> Dict[str, Any]:
    codi_id = str(row.get("codi_id") or row.get("form_id") or "").strip()
    return {
        "id": int(row.get("id") or 0),
        "codi_id": codi_id,
        "target_type": str(row.get("target_type") or "site").strip() or "site",
        "target_client_name": str(row.get("target_client_name") or "").strip(),
        "group_id": str(row.get("group_id") or "").strip(),
        "lead_group_id": str(row.get("lead_group_id") or "").strip(),
        "lead_phone_number": str(row.get("lead_phone_number") or "").strip(),
        "internal_notify_group_id": str(row.get("internal_notify_group_id") or "").strip(),
        "source_type": str(row.get("source_type") or "").strip(),
        "origem_anuncio": str(row.get("origem_anuncio") or "").strip(),
        "cliente_origem": str(row.get("cliente_origem") or "").strip(),
        "lead_exclude_fields": _json_list(row.get("lead_exclude_fields")),
        "lead_exclude_contains": _json_list(row.get("lead_exclude_contains")),
        "lead_exclude_regex": _json_list(row.get("lead_exclude_regex")),
        "cors_allowed_origins": [str(x).strip() for x in _json_list(row.get("cors_allowed_origins")) if str(x).strip()],
        "lead_template": str(row.get("lead_template") or "default").strip() or "default",
        "internal_lead_template": str(row.get("internal_lead_template") or "").strip(),
        "enabled": bool(row.get("enabled", True)),
        "notes": str(row.get("notes") or "").strip(),
    }


# Identificador do formulário no site: na prática vê-se 30–32 dígitos; aceitamos 28–36 só numéricos.
def is_valid_site_codi_id(value: str) -> bool:
    return bool(re.fullmatch(r"\d{28,36}", (value or "").strip()))


def list_site_lead_routes() -> List[Dict[str, Any]]:
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, form_id, target_type, target_client_name, source_type,
                           group_id, lead_group_id, lead_phone_number, internal_notify_group_id,
                           origem_anuncio, cliente_origem,
                           lead_exclude_fields, lead_exclude_contains, lead_exclude_regex,
                           cors_allowed_origins,
                           lead_template, internal_lead_template, enabled, notes
                    FROM site_lead_routes
                    ORDER BY lower(form_id) ASC, id ASC
                    """
                )
                return [_site_route_row(dict(r)) for r in cur.fetchall()]
    rows = [_site_route_row(r) for r in _load_site_routes_json() if isinstance(r, dict)]
    rows.sort(key=lambda x: (x.get("codi_id", "").lower(), int(x.get("id") or 0)))
    return rows


def get_site_lead_route(route_id: int) -> Optional[Dict[str, Any]]:
    rid = int(route_id)
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, form_id, target_type, target_client_name, source_type,
                           group_id, lead_group_id, lead_phone_number, internal_notify_group_id,
                           origem_anuncio, cliente_origem,
                           lead_exclude_fields, lead_exclude_contains, lead_exclude_regex,
                           cors_allowed_origins,
                           lead_template, internal_lead_template, enabled, notes
                    FROM site_lead_routes
                    WHERE id = %s
                    """,
                    (rid,),
                )
                row = cur.fetchone()
                return _site_route_row(dict(row)) if row else None
    for row in list_site_lead_routes():
        if int(row.get("id") or 0) == rid:
            return row
    return None


def insert_site_lead_route(data: Dict[str, Any]) -> int:
    codi_id = str(data.get("codi_id") or data.get("form_id") or "").strip()
    if not codi_id:
        raise ValueError("codi_id_obrigatorio")
    if not is_valid_site_codi_id(codi_id):
        raise ValueError("codi_id_invalido_formato")
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO site_lead_routes (
                      form_id, target_type, target_client_name, group_id, lead_group_id,
                      lead_phone_number, internal_notify_group_id, source_type,
                      origem_anuncio, cliente_origem,
                      lead_exclude_fields, lead_exclude_contains, lead_exclude_regex,
                      cors_allowed_origins,
                      lead_template, internal_lead_template, enabled, notes
                    ) VALUES (
                      %(form_id)s, %(target_type)s, %(target_client_name)s, %(group_id)s, %(lead_group_id)s,
                      %(lead_phone_number)s, %(internal_notify_group_id)s, %(source_type)s,
                      %(origem_anuncio)s, %(cliente_origem)s,
                      %(lead_exclude_fields)s, %(lead_exclude_contains)s, %(lead_exclude_regex)s,
                      %(cors_allowed_origins)s,
                      %(lead_template)s, %(internal_lead_template)s, %(enabled)s, %(notes)s
                    )
                    RETURNING id
                    """,
                    {
                        "form_id": codi_id,
                        "target_type": str(data.get("target_type") or "site").strip() or "site",
                        "target_client_name": str(data.get("target_client_name") or "").strip(),
                        "group_id": str(data.get("group_id") or "").strip(),
                        "lead_group_id": str(data.get("lead_group_id") or "").strip(),
                        "lead_phone_number": str(data.get("lead_phone_number") or "").strip(),
                        "internal_notify_group_id": str(data.get("internal_notify_group_id") or "").strip(),
                        "source_type": str(data.get("source_type") or "").strip(),
                        "origem_anuncio": str(data.get("origem_anuncio") or "").strip(),
                        "cliente_origem": str(data.get("cliente_origem") or "").strip(),
                        "lead_exclude_fields": Json(data.get("lead_exclude_fields") or []),
                        "lead_exclude_contains": Json(data.get("lead_exclude_contains") or []),
                        "lead_exclude_regex": Json(data.get("lead_exclude_regex") or []),
                        "cors_allowed_origins": Json(data.get("cors_allowed_origins") or []),
                        "lead_template": str(data.get("lead_template") or "default").strip() or "default",
                        "internal_lead_template": str(data.get("internal_lead_template") or "").strip(),
                        "enabled": bool(data.get("enabled", True)),
                        "notes": str(data.get("notes") or "").strip(),
                    },
                )
                row = cur.fetchone()
                return int(row["id"])
    with _SITE_ROUTES_JSON_LOCK:
        rows = _load_site_routes_json()
        if any(
            str(r.get("codi_id") or r.get("form_id") or "").strip().lower() == codi_id.lower()
            for r in rows
            if isinstance(r, dict)
        ):
            raise ValueError("codi_id_duplicado")
        next_id = max([int(r.get("id") or 0) for r in rows if isinstance(r, dict)] or [0]) + 1
        rows.append(
            {
                "id": next_id,
                "codi_id": codi_id,
                "target_type": str(data.get("target_type") or "site").strip() or "site",
                "target_client_name": str(data.get("target_client_name") or "").strip(),
                "group_id": str(data.get("group_id") or "").strip(),
                "lead_group_id": str(data.get("lead_group_id") or "").strip(),
                "lead_phone_number": str(data.get("lead_phone_number") or "").strip(),
                "internal_notify_group_id": str(data.get("internal_notify_group_id") or "").strip(),
                "source_type": str(data.get("source_type") or "").strip(),
                "origem_anuncio": str(data.get("origem_anuncio") or "").strip(),
                "cliente_origem": str(data.get("cliente_origem") or "").strip(),
                "lead_exclude_fields": _norm_str_list(data.get("lead_exclude_fields")),
                "lead_exclude_contains": _norm_str_list(data.get("lead_exclude_contains")),
                "lead_exclude_regex": _norm_str_list(data.get("lead_exclude_regex")),
                "cors_allowed_origins": _norm_str_list(data.get("cors_allowed_origins")),
                "lead_template": str(data.get("lead_template") or "default").strip() or "default",
                "internal_lead_template": str(data.get("internal_lead_template") or "").strip(),
                "enabled": bool(data.get("enabled", True)),
                "notes": str(data.get("notes") or "").strip(),
            }
        )
        _save_site_routes_json(rows)
    return next_id


def update_site_lead_route(route_id: int, data: Dict[str, Any]) -> None:
    rid = int(route_id)
    codi_id = str(data.get("codi_id") or data.get("form_id") or "").strip()
    if not codi_id:
        raise ValueError("codi_id_obrigatorio")
    if not is_valid_site_codi_id(codi_id):
        raise ValueError("codi_id_invalido_formato")
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE site_lead_routes
                    SET
                      form_id = %(form_id)s,
                      target_type = %(target_type)s,
                      target_client_name = %(target_client_name)s,
                      group_id = %(group_id)s,
                      lead_group_id = %(lead_group_id)s,
                      lead_phone_number = %(lead_phone_number)s,
                      internal_notify_group_id = %(internal_notify_group_id)s,
                      source_type = %(source_type)s,
                      origem_anuncio = %(origem_anuncio)s,
                      cliente_origem = %(cliente_origem)s,
                      lead_exclude_fields = %(lead_exclude_fields)s,
                      lead_exclude_contains = %(lead_exclude_contains)s,
                      lead_exclude_regex = %(lead_exclude_regex)s,
                      cors_allowed_origins = %(cors_allowed_origins)s,
                      lead_template = %(lead_template)s,
                      internal_lead_template = %(internal_lead_template)s,
                      enabled = %(enabled)s,
                      notes = %(notes)s,
                      updated_at = now()
                    WHERE id = %(id)s
                    """,
                    {
                        "id": rid,
                        "form_id": codi_id,
                        "target_type": str(data.get("target_type") or "site").strip() or "site",
                        "target_client_name": str(data.get("target_client_name") or "").strip(),
                        "group_id": str(data.get("group_id") or "").strip(),
                        "lead_group_id": str(data.get("lead_group_id") or "").strip(),
                        "lead_phone_number": str(data.get("lead_phone_number") or "").strip(),
                        "internal_notify_group_id": str(data.get("internal_notify_group_id") or "").strip(),
                        "source_type": str(data.get("source_type") or "").strip(),
                        "origem_anuncio": str(data.get("origem_anuncio") or "").strip(),
                        "cliente_origem": str(data.get("cliente_origem") or "").strip(),
                        "lead_exclude_fields": Json(data.get("lead_exclude_fields") or []),
                        "lead_exclude_contains": Json(data.get("lead_exclude_contains") or []),
                        "lead_exclude_regex": Json(data.get("lead_exclude_regex") or []),
                        "cors_allowed_origins": Json(data.get("cors_allowed_origins") or []),
                        "lead_template": str(data.get("lead_template") or "default").strip() or "default",
                        "internal_lead_template": str(data.get("internal_lead_template") or "").strip(),
                        "enabled": bool(data.get("enabled", True)),
                        "notes": str(data.get("notes") or "").strip(),
                    },
                )
        return
    with _SITE_ROUTES_JSON_LOCK:
        rows = _load_site_routes_json()
        idx = next((i for i, r in enumerate(rows) if isinstance(r, dict) and int(r.get("id") or 0) == rid), None)
        if idx is None:
            return
        for i, r in enumerate(rows):
            if i == idx or not isinstance(r, dict):
                continue
            if str(r.get("codi_id") or r.get("form_id") or "").strip().lower() == codi_id.lower():
                raise ValueError("codi_id_duplicado")
        rows[idx] = {
            "id": rid,
            "codi_id": codi_id,
            "target_type": str(data.get("target_type") or "meta").strip() or "meta",
            "target_client_name": str(data.get("target_client_name") or "").strip(),
            "group_id": str(data.get("group_id") or "").strip(),
            "lead_group_id": str(data.get("lead_group_id") or "").strip(),
            "lead_phone_number": str(data.get("lead_phone_number") or "").strip(),
            "internal_notify_group_id": str(data.get("internal_notify_group_id") or "").strip(),
            "source_type": str(data.get("source_type") or "").strip(),
            "origem_anuncio": str(data.get("origem_anuncio") or "").strip(),
            "cliente_origem": str(data.get("cliente_origem") or "").strip(),
            "lead_exclude_fields": _norm_str_list(data.get("lead_exclude_fields")),
            "lead_exclude_contains": _norm_str_list(data.get("lead_exclude_contains")),
            "lead_exclude_regex": _norm_str_list(data.get("lead_exclude_regex")),
            "cors_allowed_origins": _norm_str_list(data.get("cors_allowed_origins")),
            "lead_template": str(data.get("lead_template") or "default").strip() or "default",
            "internal_lead_template": str(data.get("internal_lead_template") or "").strip(),
            "enabled": bool(data.get("enabled", True)),
            "notes": str(data.get("notes") or "").strip(),
        }
        _save_site_routes_json(rows)


def delete_site_lead_route(route_id: int) -> bool:
    rid = int(route_id)
    if db_enabled():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM site_lead_routes WHERE id = %s", (rid,))
                return cur.rowcount > 0
    with _SITE_ROUTES_JSON_LOCK:
        rows = _load_site_routes_json()
        new_rows = [r for r in rows if not (isinstance(r, dict) and int(r.get("id") or 0) == rid)]
        if len(new_rows) == len(rows):
            return False
        _save_site_routes_json(new_rows)
        return True
