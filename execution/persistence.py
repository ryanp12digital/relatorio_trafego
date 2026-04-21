"""
Persistência Postgres (Supabase) para clientes e documento de templates.

Ative com DATABASE_URL ou SUPABASE_DATABASE_URL (connection string do pooler).
Se não estiver definido, o projeto continua usando os JSON em data/ (clients, google_clients, message_templates).
"""

from __future__ import annotations

import json
import logging
import os
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from copy import deepcopy
from typing import Any, Dict, List, Optional

from execution.project_paths import (
    catalog_groups_json_path,
    catalog_webhook_listener_json_path,
    clients_json_path,
    ensure_data_dir,
    google_clients_json_path,
    message_templates_json_path,
)

logger = logging.getLogger(__name__)
_DB_BOOTSTRAPPED = False
_CATALOG_JSON_LOCK = threading.RLock()
_LISTENER_JSON_LOCK = threading.RLock()
_CATALOG_WEBHOOK_LISTENING_DEFAULT = True

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
