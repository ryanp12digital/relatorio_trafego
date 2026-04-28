"""
Dashboard viva de clientes da automação P12.

Recursos:
- Visualização de clientes e validações de configuração.
- Cadastro rápido de novo cliente em data/clients.json (sem Postgres).
- Stream em tempo real (SSE) de eventos do webhook/automação.
- Harness para simular fluxo de webhook por cliente.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import sys

# Raiz do projeto no path (ex.: python /app/execution/dashboard_app.py no container)
_proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj_root not in sys.path:
    sys.path.insert(0, _proj_root)
import re
import secrets
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from flask import Flask, Response, jsonify, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException

from execution import persistence
from execution.evolution_catalog_webhook import (
    log_evolution_catalog_warning,
    process_evolution_catalog_payload,
)
from execution.flask_server import serve_flask_app
from execution.project_paths import (
    clients_json_path,
    ensure_data_dir,
    google_clients_json_path,
    site_lead_routes_json_path,
)

from execution.live_events import (
    get_events_file_path,
    publish_event,
    read_events_since,
    read_recent_events,
)
from execution.message_templates import (
    list_templates_payload,
    render_template_text,
    upsert_filter_rules,
    upsert_template,
)
from execution.meta_client import MetaAPIAuthError, list_business_ad_accounts, list_business_pages

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "..", "templates"),
    static_folder=os.path.join(os.path.dirname(__file__), "..", "static"),
)
app.secret_key = (
    os.environ.get("DASHBOARD_SESSION_SECRET")
    or os.environ.get("FLASK_SECRET_KEY")
    or secrets.token_hex(32)
)

logger = logging.getLogger(__name__)


@app.errorhandler(HTTPException)
def _http_exception_for_api(e: HTTPException) -> Any:
    """404/405/etc. em rotas /api/* passam a devolver JSON em vez de HTML."""
    path = request.path or ""
    if path.startswith("/api/") or path.startswith("/dash/api/"):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": e.name.lower().replace(" ", "_"),
                    "message": e.description,
                }
            ),
            e.code,
        )
    return e.get_response()


@app.errorhandler(Exception)
def _unhandled_exception_for_api(e: Exception) -> Any:
    if isinstance(e, HTTPException):
        raise e
    path = request.path or ""
    if not (path.startswith("/api/") or path.startswith("/dash/api/")):
        raise e
    logger.exception("Erro não tratado na API da dashboard")
    return jsonify({"ok": False, "error": "internal_error", "message": str(e)}), 500


_CLIENTS_LOCK = threading.Lock()
_GOOGLE_CLIENTS_LOCK = threading.Lock()
_SITE_ROUTES_LOCK = threading.Lock()


def _clients_path() -> str:
    return clients_json_path()


def _google_clients_path() -> str:
    return google_clients_json_path()


def _site_routes_path() -> str:
    return site_lead_routes_json_path()


def _load_clients() -> List[Dict[str, Any]]:
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        return persistence.list_meta_clients()
    path = _clients_path()
    if not os.path.exists(path):
        return []
    with _CLIENTS_LOCK:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    if not isinstance(data, list):
        return []
    out: List[Dict[str, Any]] = []
    for idx, c in enumerate(data):
        if not isinstance(c, dict):
            continue
        row = dict(c)
        row["id"] = idx
        out.append(row)
    return out


def _save_clients(clients: List[Dict[str, Any]]) -> None:
    if persistence.db_enabled():
        raise RuntimeError("use_persistence_insert_update")
    path = _clients_path()
    serializable = []
    for c in clients:
        if not isinstance(c, dict):
            continue
        serializable.append({k: v for k, v in c.items() if k != "id"})
    ensure_data_dir()
    with _CLIENTS_LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
            f.write("\n")


def _load_google_clients() -> List[Dict[str, Any]]:
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        return persistence.list_google_clients()
    path = _google_clients_path()
    if not os.path.exists(path):
        return []
    with _GOOGLE_CLIENTS_LOCK:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    if not isinstance(data, list):
        return []
    out: List[Dict[str, Any]] = []
    for idx, c in enumerate(data):
        if not isinstance(c, dict):
            continue
        row = dict(c)
        row.pop("id", None)
        row["id"] = idx
        out.append(row)
    return out


def _save_google_clients(clients: List[Dict[str, Any]]) -> None:
    if persistence.db_enabled():
        raise RuntimeError("use_persistence_insert_update")
    path = _google_clients_path()
    ensure_data_dir()
    serializable: List[Dict[str, Any]] = []
    for c in clients:
        if not isinstance(c, dict):
            continue
        d = dict(c)
        if isinstance(d.get("id"), int):
            d.pop("id", None)
        serializable.append(d)
    with _GOOGLE_CLIENTS_LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
            f.write("\n")


def _load_site_lead_routes() -> List[Dict[str, Any]]:
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        return persistence.list_site_lead_routes()
    path = _site_routes_path()
    if not os.path.exists(path):
        return []
    with _SITE_ROUTES_LOCK:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    if not isinstance(data, list):
        return []
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(data):
        if not isinstance(row, dict):
            continue
        item = dict(row)
        item["id"] = int(item.get("id") or idx)
        out.append(item)
    return out


def _save_site_lead_routes(rows: List[Dict[str, Any]]) -> None:
    if persistence.db_enabled():
        raise RuntimeError("use_persistence_insert_update_site_routes")
    ensure_data_dir()
    path = _site_routes_path()
    serializable: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        serializable.append({k: v for k, v in row.items() if k != "id"})
    with _SITE_ROUTES_LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
            f.write("\n")


def _normalize_act_id(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if value.startswith("act_"):
        return value
    if value.isdigit():
        return f"act_{value}"
    return value


def _as_bool(v: Any, default: bool = True) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _csv_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _is_valid_site_codi_id(value: str) -> bool:
    return bool(re.fullmatch(r"\d{32}", (value or "").strip()))


def _validate_client(client: Dict[str, Any]) -> Dict[str, Any]:
    ad_account_id = str(client.get("ad_account_id", "")).strip()
    group_id = str(client.get("group_id", "")).strip()
    lead_group_id = str(client.get("lead_group_id", "")).strip()
    page_id = str(client.get("meta_page_id", "")).strip()
    enabled = bool(client.get("enabled", True))

    ad_ok = bool(re.fullmatch(r"act_\d{6,}", ad_account_id))
    group_ok = bool(re.fullmatch(r"\d+@g\.us", group_id))
    lead_group_ok = (not lead_group_id) or bool(re.fullmatch(r"\d+@g\.us", lead_group_id))
    p12_g = str(client.get("p12_report_group_id", "")).strip()
    p12_ok = (not p12_g) or bool(re.fullmatch(r"\d+@g\.us", p12_g))
    int_g = str(client.get("internal_notify_group_id", "")).strip()
    int_ok = (not int_g) or bool(re.fullmatch(r"\d+@g\.us", int_g))
    page_ok = (not page_id) or page_id.isdigit()
    ready_for_report = enabled and ad_ok and group_ok
    ready_for_lead_route = enabled and bool(page_id) and lead_group_ok

    if not enabled:
        status_label = "Pausado"
    elif ready_for_report and ready_for_lead_route:
        status_label = "Ativo completo"
    elif ready_for_report:
        status_label = "Ativo parcial"
    else:
        status_label = "Inconsistente"

    return {
        "ad_account_ok": ad_ok,
        "group_id_ok": group_ok,
        "lead_group_id_ok": lead_group_ok,
        "p12_report_group_id_ok": p12_ok,
        "internal_notify_group_id_ok": int_ok,
        "meta_page_id_ok": page_ok,
        "ready_for_report": ready_for_report,
        "ready_for_lead_route": ready_for_lead_route,
        "status_label": status_label,
    }


def _normalize_google_customer_id(raw: str) -> str:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return str(raw or "").strip()


def _validate_google_client(client: Dict[str, Any]) -> Dict[str, Any]:
    customer_id = str(client.get("google_customer_id", "")).strip()
    group_id = str(client.get("group_id", "")).strip()
    enabled = bool(client.get("enabled", True))
    cid_ok = bool(re.fullmatch(r"\d{3}-\d{3}-\d{4}", customer_id))
    group_ok = bool(re.fullmatch(r"\d+@g\.us", group_id))
    p12_g = str(client.get("p12_report_group_id", "")).strip()
    p12_ok = (not p12_g) or bool(re.fullmatch(r"\d+@g\.us", p12_g))
    int_g = str(client.get("internal_notify_group_id", "")).strip()
    int_ok = (not int_g) or bool(re.fullmatch(r"\d+@g\.us", int_g))
    if not enabled:
        status = "Pausado"
    elif cid_ok and group_ok:
        status = "Ativo completo"
    elif cid_ok:
        status = "Ativo parcial"
    else:
        status = "Inconsistente"
    return {
        "customer_id_ok": cid_ok,
        "group_id_ok": group_ok,
        "p12_report_group_id_ok": p12_ok,
        "internal_notify_group_id_ok": int_ok,
        "status_label": status,
    }


def _public_google_client_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    primary = raw.get("primary_conversions")
    if not isinstance(primary, list):
        primary = []
    gid = int(raw["id"])
    client = {
        "id": gid,
        "client_name": str(raw.get("client_name", "")).strip(),
        "google_customer_id": _normalize_google_customer_id(str(raw.get("google_customer_id", "")).strip()),
        "group_id": str(raw.get("group_id", "")).strip(),
        "enabled": bool(raw.get("enabled", True)),
        "notes": str(raw.get("notes", "")).strip(),
        "google_template": str(raw.get("google_template", "default")).strip() or "default",
        "primary_conversions": [str(x).strip() for x in primary if str(x).strip()],
        "lead_phone_number": str(raw.get("lead_phone_number", "")).strip(),
        "p12_report_group_id": str(raw.get("p12_report_group_id", "")).strip(),
        "p12_report_template": str(raw.get("p12_report_template", "")).strip(),
        "p12_data_report_template": str(raw.get("p12_data_report_template", "")).strip(),
        "internal_notify_group_id": str(raw.get("internal_notify_group_id", "")).strip(),
        "internal_lead_template": str(raw.get("internal_lead_template", "")).strip(),
        "internal_weekly_template": str(raw.get("internal_weekly_template", "")).strip(),
    }
    client["checks"] = _validate_google_client(client)
    return client


def _latest_events_by_client(limit: int = 300, per_client: int = 25) -> Dict[str, List[Dict[str, Any]]]:
    events = read_recent_events(limit=limit)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for ev in events:
        client_name = str(ev.get("client_name", "")).strip() or "__global__"
        grouped.setdefault(client_name, []).append(ev)
    for key, rows in grouped.items():
        grouped[key] = rows[-per_client:]
    return grouped


def _public_client_payload(raw: Dict[str, Any], events_map: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    cid = int(raw["id"])
    client = {
        "client_name": str(raw.get("client_name", "")).strip(),
        "ad_account_id": _normalize_act_id(str(raw.get("ad_account_id", ""))),
        "group_id": str(raw.get("group_id", "")).strip(),
        "meta_page_id": str(raw.get("meta_page_id", "")).strip(),
        "lead_group_id": str(raw.get("lead_group_id", "")).strip(),
        "lead_phone_number": str(raw.get("lead_phone_number", "")).strip(),
        "lead_template": str(raw.get("lead_template", "")).strip() or "default",
        "lead_exclude_fields": _csv_list(raw.get("lead_exclude_fields")),
        "lead_exclude_contains": _csv_list(raw.get("lead_exclude_contains")),
        "lead_exclude_regex": _csv_list(raw.get("lead_exclude_regex")),
        "enabled": bool(raw.get("enabled", True)),
        "p12_report_group_id": str(raw.get("p12_report_group_id", "")).strip(),
        "p12_report_template": str(raw.get("p12_report_template", "")).strip(),
        "p12_data_report_template": str(raw.get("p12_data_report_template", "")).strip(),
        "internal_notify_group_id": str(raw.get("internal_notify_group_id", "")).strip(),
        "internal_lead_template": str(raw.get("internal_lead_template", "")).strip(),
        "internal_weekly_template": str(raw.get("internal_weekly_template", "")).strip(),
    }
    checks = _validate_client(client)
    client_events = events_map.get(client["client_name"], [])
    return {
        "id": cid,
        **client,
        "checks": checks,
        "events": client_events,
    }


def _build_clients_response() -> Dict[str, Any]:
    clients = _load_clients()
    events_map = _latest_events_by_client()
    payload = [_public_client_payload(c, events_map) for c in clients]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "clients": payload,
        "global_events": events_map.get("__global__", []),
    }


def _build_google_clients_response() -> Dict[str, Any]:
    clients = _load_google_clients()
    payload = [_public_google_client_payload(c) for c in clients]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "clients": payload,
    }


def _public_site_lead_route_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    target_type = str(raw.get("target_type", "meta")).strip().lower()
    if target_type not in {"meta", "google"}:
        target_type = "meta"
    return {
        "id": int(raw.get("id", 0)),
        "codi_id": str(raw.get("codi_id", raw.get("form_id", ""))).strip(),
        "target_type": target_type,
        "target_client_name": str(raw.get("target_client_name", "")).strip(),
        "source_type": str(raw.get("source_type", "")).strip().lower(),
        "enabled": bool(raw.get("enabled", True)),
        "notes": str(raw.get("notes", "")).strip(),
    }


def _build_site_lead_routes_response() -> Dict[str, Any]:
    routes = [_public_site_lead_route_payload(r) for r in _load_site_lead_routes()]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "routes": routes,
    }


def _simulate_webhook_flow(client: Dict[str, Any], scenario: str = "success") -> None:
    name = client.get("client_name", "Cliente")
    page_id = client.get("meta_page_id", "")
    group_id = client.get("lead_group_id") or client.get("group_id") or ""
    stages: List[Tuple[str, str, str, float]] = [
        ("RECEBIDO", "info", "Webhook recebido no endpoint /meta-new-lead", 0.6),
        ("PAYLOAD_OK", "ok", "Payload validado e normalizado", 0.6),
        ("ROTA_RESOLVIDA", "ok", "Cliente identificado pelo roteamento", 0.8),
        ("MENSAGEM_FORMATADA", "ok", "Mensagem formatada para WhatsApp", 0.7),
    ]
    if scenario == "send_fail":
        stages.append(("WHATSAPP_FALHA", "error", "Evolution retornou falha no envio", 0.7))
        stages.append(("CONCLUIDO_FALHA", "error", "Fluxo concluído com erro", 0.0))
    elif scenario == "route_fail":
        stages[2] = ("IGNORADO_ROUTE", "warning", "Lead sem page_id mapeado para cliente", 0.6)
        stages = stages[:3] + [("CONCLUIDO_OK", "warning", "Fluxo concluído sem envio", 0.0)]
    else:
        stages.append(("WHATSAPP_ENVIADO_OK", "ok", "Mensagem enviada ao grupo do cliente", 0.8))
        stages.append(("CONCLUIDO_OK", "ok", "Fluxo concluído sem erros", 0.0))

    for stage, status, detail, delay in stages:
        publish_event(
            source="dashboard_harness",
            stage=stage,
            status=status,
            detail=detail,
            client_name=name,
            page_id=page_id,
            group_id=group_id,
            payload={"scenario": scenario},
        )
        if delay > 0:
            time.sleep(delay)


def _run_flow_async(client: Dict[str, Any], scenario: str) -> None:
    t = threading.Thread(target=_simulate_webhook_flow, args=(client, scenario), daemon=True)
    t.start()


def _dashboard_auth_password() -> str:
    return (os.environ.get("DASHBOARD_AUTH_PASSWORD") or "").strip()


def _dashboard_auth_users() -> List[Dict[str, str]]:
    """
    Contas e-mail+senha em JSON, variável DASHBOARD_AUTH_USERS.
    Formato: [{"email":"a@b.com","password":"..."}, ...]
    """
    raw = (os.environ.get("DASHBOARD_AUTH_USERS") or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    out: List[Dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        em = (item.get("email") or "").strip()
        pw = item.get("password")
        if not em or pw is None:
            continue
        out.append({"email": em, "password": str(pw)})
    return out


def dashboard_auth_configured() -> bool:
    return bool(_dashboard_auth_password()) or bool(_dashboard_auth_users())


def verify_dashboard_password(sent: str) -> bool:
    pwd = _dashboard_auth_password()
    return bool(pwd) and hmac.compare_digest(sent or "", pwd)


def _norm_email(s: str) -> str:
    return (s or "").strip().casefold()


def verify_dashboard_credentials(email: str, password: str) -> bool:
    users = _dashboard_auth_users()
    if users:
        want = _norm_email(email)
        for u in users:
            if _norm_email(u.get("email", "")) == want and hmac.compare_digest(
                password or "", (u.get("password") or "")
            ):
                return True
        return False
    return verify_dashboard_password(password)


def dashboard_require_email_login() -> bool:
    return bool(_dashboard_auth_users())


def dashboard_login_page_context(
    *,
    next_url: str,
    error: Optional[str],
    form_action: str,
) -> Dict[str, Any]:
    use_users = bool(_dashboard_auth_users())
    return {
        "next_url": next_url,
        "error": error,
        "form_action": form_action,
        "require_email": use_users,
        "login_subtext": (
            "Informe e-mail e senha (contas em DASHBOARD_AUTH_USERS)."
            if use_users
            else "Informe a senha definida em DASHBOARD_AUTH_PASSWORD."
        ),
    }


def dashboard_auth_gate_response() -> Optional[Any]:
    """
    Retorno None = seguir request. Usado pelo Flask da dashboard (porta 8091)
    e pelo webhook em rotas /dash/* (mesmo DASHBOARD_SESSION_SECRET para o cookie).
    """
    if not dashboard_auth_configured():
        return None
    raw = request.path or ""
    if raw.startswith("/static/"):
        return None
    if raw.rstrip("/") in ("/api/health", "/dash/api/health", "/health"):
        return None
    if raw.rstrip("/") == "/evolution-webhook":
        return None
    base = raw.rstrip("/") or "/"
    if base in ("/login", "/dash/login", "/logout", "/dash/logout"):
        return None
    if session.get("dashboard_ok"):
        return None
    if raw.startswith("/api/") or raw.startswith("/dash/api/"):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if raw.startswith("/dash"):
        return redirect(f"/dash/login?next={quote(raw, safe='')}")
    return redirect(url_for("login", next=raw))


@app.before_request
def _dashboard_auth_gate() -> Optional[Any]:
    return dashboard_auth_gate_response()


@app.get("/login")
def login() -> str:
    next_url = request.args.get("next") or "/"
    return render_template(
        "login.html", **dashboard_login_page_context(next_url=next_url, error=None, form_action=url_for("login_post"))
    )


@app.post("/login")
def login_post() -> Any:
    next_url = (request.form.get("next") or "/").strip() or "/"
    if not dashboard_auth_configured():
        return redirect(next_url)
    email = (request.form.get("email") or "").strip()
    pwd = request.form.get("password") or ""
    if verify_dashboard_credentials(email, pwd):
        session["dashboard_ok"] = True
        return redirect(next_url)
    err = "E-mail ou senha incorretos." if dashboard_require_email_login() else "Senha incorreta."
    return render_template(
        "login.html",
        **dashboard_login_page_context(
            next_url=next_url,
            error=err,
            form_action=url_for("login_post"),
        ),
    )


@app.get("/logout")
def logout() -> Any:
    session.clear()
    return redirect(url_for("login"))


@app.get("/api/health")
def api_health() -> Any:
    ensure_data_dir()
    cpath = clients_json_path()
    gpath = google_clients_json_path()
    checks: Dict[str, Any] = {
        "data_dir": True,
        "db_configured": persistence.db_enabled(),
        "clients_source": "postgres" if persistence.db_enabled() else ("json_file" if os.path.isfile(cpath) else "json_missing"),
        "google_clients_json": "postgres" if persistence.db_enabled() else ("present" if os.path.isfile(gpath) else "missing"),
    }
    return jsonify({"ok": True, "db": persistence.db_enabled(), "checks": checks})


@app.get("/health")
def root_health() -> Any:
    """Mesmo contrato que /api/health (probes na raiz da porta 8091)."""
    return api_health()


def evolution_catalog_webhook_view() -> Any:
    """POST público (secret); usado pela dashboard e pelo meta_lead_webhook."""
    raw = request.get_json(silent=True, force=True)
    if raw is None:
        try:
            text = request.get_data(as_text=True)
        except Exception:
            text = ""
        if text:
            text = text.lstrip("\ufeff").strip()
            try:
                raw = json.loads(text)
            except json.JSONDecodeError:
                raw = None
    if raw is None:
        log_evolution_catalog_warning("JSON_INVALIDO", "corpo vazio ou nao e JSON valido")
        publish_event(
            source="catalog_http",
            stage="JSON_INVALIDO",
            status="error",
            detail="Corpo do POST vazio ou não é JSON válido",
            payload={"content_type": (request.content_type or "")[:120]},
        )
        return jsonify({"ok": False, "error": "invalid_json"}), 400
    cl = request.content_length
    publish_event(
        source="catalog_http",
        stage="POST_RECEBIDO",
        status="info",
        detail="POST /evolution-webhook recebido" + (f" (Content-Length: {cl})" if cl is not None else ""),
        payload={"path": (request.path or "")[:120]},
    )
    hdr = (request.headers.get("X-Webhook-Secret") or "").strip()
    auth = request.headers.get("Authorization") or ""
    bearer = ""
    if auth.lower().startswith("bearer "):
        bearer = auth[7:].strip()
    q_secret = (request.args.get("catalog_secret") or request.args.get("secret") or "").strip()
    body, status = process_evolution_catalog_payload(
        raw, header_secret=hdr, auth_bearer=bearer, query_secret=q_secret
    )
    return jsonify(body), status


@app.post("/evolution-webhook")
def evolution_catalog_webhook_route() -> Any:
    return evolution_catalog_webhook_view()


@app.get("/api/catalog-groups")
def api_catalog_groups_list() -> Any:
    persistence.ensure_db_ready()
    rows = persistence.list_catalog_groups()
    return jsonify({"ok": True, "groups": rows})


@app.patch("/api/catalog-groups")
def api_catalog_groups_patch() -> Any:
    persistence.ensure_db_ready()
    payload = request.get_json(silent=True) or {}
    gj = str(payload.get("group_jid") or "").strip()
    if not gj:
        return jsonify({"ok": False, "error": "group_jid_obrigatorio"}), 400
    sub = payload.get("subject")
    mon = payload.get("monitoring_enabled")
    sub_opt = str(sub).strip() if sub is not None else None
    mon_opt: Optional[bool] = None
    if mon is not None:
        mon_opt = bool(mon)
    updated = persistence.patch_catalog_group_manual(
        gj,
        subject=sub_opt if sub is not None else None,
        monitoring_enabled=mon_opt,
    )
    if updated is None:
        return jsonify({"ok": False, "error": "grupo_nao_encontrado"}), 404
    return jsonify({"ok": True, "group": updated})


@app.delete("/api/catalog-groups")
def api_catalog_groups_delete() -> Any:
    persistence.ensure_db_ready()
    payload = request.get_json(silent=True) or {}
    gj = str(payload.get("group_jid") or "").strip()
    if not gj:
        return jsonify({"ok": False, "error": "group_jid_obrigatorio"}), 400
    if not persistence.delete_catalog_group(gj):
        return jsonify({"ok": False, "error": "grupo_nao_encontrado"}), 404
    return jsonify({"ok": True})


@app.route("/api/catalog-groups/webhook-listener", methods=["GET", "POST"])
def api_catalog_webhook_listener() -> Any:
    """Liga/desliga processamento do POST /evolution-webhook (ficheiro partilhado entre processos)."""
    persistence.ensure_db_ready()
    if request.method == "GET":
        return jsonify({"ok": True, "listening": persistence.get_catalog_webhook_listening()})
    payload = request.get_json(silent=True) or {}
    if "listening" not in payload:
        return jsonify({"ok": False, "error": "listening_obrigatorio_boolean"}), 400
    persistence.set_catalog_webhook_listening(bool(payload["listening"]))
    return jsonify({"ok": True, "listening": persistence.get_catalog_webhook_listening()})


@app.post("/api/catalog-groups/refresh")
def api_catalog_groups_refresh() -> Any:
    persistence.ensure_db_ready()
    payload = request.get_json(silent=True) or {}
    gj = str(payload.get("group_jid") or "").strip()
    if not gj or not gj.endswith("@g.us"):
        return jsonify({"ok": False, "error": "group_jid_invalido"}), 400
    try:
        from execution.evolution_client import get_evolution_client

        client = get_evolution_client()
        info = client.fetch_group_info(gj)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 503
    if not info:
        return jsonify({"ok": False, "error": "findGroupInfos_sem_dados"}), 502
    sub = (
        str(info.get("subject") or "")
        or str(info.get("name") or "")
        or str(info.get("groupName") or "")
    ).strip()
    if sub:
        persistence.update_catalog_group_subject(gj, sub)
    row = persistence.get_catalog_group(gj)
    return jsonify({"ok": True, "group": row, "fetched": bool(sub)})


def _dashboard_public_url_prefix() -> str:
    """
    Prefixo publico da app (ex. /dash) quando o Easypanel expoe a Pulseboard em subcaminho.
    Define DASHBOARD_URL_PREFIX=/dash para o meta tag dashboard-base e fetch() correctos.
    """
    return (os.getenv("DASHBOARD_URL_PREFIX") or "").strip().rstrip("/")


def _catalog_ad_accounts_from_clients() -> List[Dict[str, str]]:
    """Contas distintas já salvas em clientes (fallback para o select)."""
    seen: set[str] = set()
    out: List[Dict[str, str]] = []
    for c in _load_clients():
        aid = _normalize_act_id(str(c.get("ad_account_id", "") or "").strip())
        if not aid or not re.fullmatch(r"act_\d{6,}", aid) or aid in seen:
            continue
        seen.add(aid)
        nm = str(c.get("client_name", "") or "").strip() or aid
        out.append({"id": aid, "label": f"{nm} — {aid}"})
    return sorted(out, key=lambda x: x["id"])


def _catalog_pages_from_clients() -> List[Dict[str, str]]:
    """Page IDs distintos já salvos em clientes (fallback para o select)."""
    seen: set[str] = set()
    out: List[Dict[str, str]] = []
    for c in _load_clients():
        pid = str(c.get("meta_page_id", "") or "").strip()
        if not pid or not pid.isdigit() or pid in seen:
            continue
        seen.add(pid)
        nm = str(c.get("client_name", "") or "").strip() or pid
        out.append({"id": pid, "label": f"{nm} — {pid}"})
    return sorted(out, key=lambda x: x["id"])


def meta_catalog_ad_accounts_payload() -> Dict[str, Any]:
    """Catálogo híbrido: Graph API (Business) + contas já cadastradas nos clientes."""
    warnings: List[str] = []
    token = (os.getenv("META_ACCESS_TOKEN") or "").strip()
    bid = (os.getenv("META_BUSINESS_ID") or "").strip()
    api_attempted = bool(token and bid)
    from_api: List[Dict[str, str]] = []

    if api_attempted:
        try:
            raw = list_business_ad_accounts(token, bid, max_retries=2)
            for acc in raw:
                aid_raw = acc.get("id") or acc.get("account_id")
                if not aid_raw:
                    continue
                aid = _normalize_act_id(str(aid_raw).strip())
                if not re.fullmatch(r"act_\d{6,}", aid):
                    continue
                name = str(acc.get("name") or "").strip() or aid
                from_api.append({"id": aid, "label": f"{name} — {aid}"})
        except MetaAPIAuthError as e:
            warnings.append(f"Meta API (autenticação): {e!s}")
        except Exception as e:
            warnings.append(f"Meta API (contas): {e!s}")
    else:
        if not token:
            warnings.append("META_ACCESS_TOKEN ausente: usando só contas já cadastradas nos clientes.")
        if not bid:
            warnings.append("META_BUSINESS_ID ausente: usando só contas já cadastradas nos clientes.")

    from_clients = _catalog_ad_accounts_from_clients()
    api_ids = {x["id"] for x in from_api}
    merged: Dict[str, Dict[str, str]] = {x["id"]: x for x in from_api}
    for row in from_clients:
        if row["id"] not in merged:
            merged[row["id"]] = row

    items = sorted(merged.values(), key=lambda x: x["id"])
    extras = [c for c in from_clients if c["id"] not in api_ids]
    if from_api:
        source = "hybrid" if extras else "api"
    elif api_attempted:
        source = "hybrid" if from_clients else "api"
    else:
        source = "fallback"

    return {"ok": True, "source": source, "items": items, "warnings": warnings}


def meta_catalog_pages_payload() -> Dict[str, Any]:
    """Catálogo híbrido: Graph API (páginas do Business) + page_id já cadastrados nos clientes."""
    warnings: List[str] = []
    token = (os.getenv("META_ACCESS_TOKEN") or "").strip()
    bid = (os.getenv("META_BUSINESS_ID") or "").strip()
    api_attempted = bool(token and bid)
    from_api: List[Dict[str, str]] = []

    if api_attempted:
        try:
            raw = list_business_pages(token, bid, max_retries=2)
            for p in raw:
                pid = str(p.get("id") or "").strip()
                if not pid.isdigit():
                    continue
                name = str(p.get("name") or "").strip() or pid
                from_api.append({"id": pid, "label": f"{name} — {pid}"})
        except MetaAPIAuthError as e:
            warnings.append(f"Meta API (autenticação): {e!s}")
        except Exception as e:
            warnings.append(f"Meta API (páginas): {e!s}")
    else:
        if not token:
            warnings.append("META_ACCESS_TOKEN ausente: usando só páginas já cadastradas nos clientes.")
        if not bid:
            warnings.append("META_BUSINESS_ID ausente: usando só páginas já cadastradas nos clientes.")

    from_clients = _catalog_pages_from_clients()
    api_ids = {x["id"] for x in from_api}
    merged: Dict[str, Dict[str, str]] = {x["id"]: x for x in from_api}
    for row in from_clients:
        if row["id"] not in merged:
            merged[row["id"]] = row

    items = sorted(merged.values(), key=lambda x: x["id"])
    extras = [c for c in from_clients if c["id"] not in api_ids]
    if from_api:
        source = "hybrid" if extras else "api"
    elif api_attempted:
        source = "hybrid" if from_clients else "api"
    else:
        source = "fallback"

    return {"ok": True, "source": source, "items": items, "warnings": warnings}


@app.get("/api/meta-catalog/ad-accounts")
def api_meta_catalog_ad_accounts() -> Any:
    return jsonify(meta_catalog_ad_accounts_payload())


@app.get("/api/meta-catalog/pages")
def api_meta_catalog_pages() -> Any:
    return jsonify(meta_catalog_pages_payload())


@app.get("/")
def dashboard_home() -> str:
    return render_template("dashboard.html", dashboard_base=_dashboard_public_url_prefix())


@app.get("/api/clients")
def api_clients() -> Any:
    return jsonify(_build_clients_response())


@app.post("/api/clients")
def api_add_client() -> Any:
    payload = request.get_json(silent=True) or {}

    client_name = str(payload.get("client_name", "")).strip()
    ad_account_id = _normalize_act_id(str(payload.get("ad_account_id", "")).strip())
    group_id = str(payload.get("group_id", "")).strip()
    meta_page_id = str(payload.get("meta_page_id", "")).strip()
    lead_group_id = str(payload.get("lead_group_id", "")).strip()
    lead_phone_number = str(payload.get("lead_phone_number", "")).strip()
    lead_template = str(payload.get("lead_template", "default")).strip() or "default"
    lead_exclude_fields = _csv_list(payload.get("lead_exclude_fields"))
    lead_exclude_contains = _csv_list(payload.get("lead_exclude_contains"))
    lead_exclude_regex = _csv_list(payload.get("lead_exclude_regex"))
    enabled = _as_bool(payload.get("enabled"), default=True)

    if not client_name:
        return jsonify({"ok": False, "error": "client_name_obrigatorio"}), 400
    if not ad_account_id:
        return jsonify({"ok": False, "error": "ad_account_id_obrigatorio"}), 400
    if not group_id:
        return jsonify({"ok": False, "error": "group_id_obrigatorio"}), 400

    new_client = {
        "client_name": client_name,
        "ad_account_id": ad_account_id,
        "group_id": group_id,
        "meta_page_id": meta_page_id,
        "lead_group_id": lead_group_id,
        "lead_phone_number": lead_phone_number,
        "lead_template": lead_template,
        "lead_exclude_fields": lead_exclude_fields,
        "lead_exclude_contains": lead_exclude_contains,
        "lead_exclude_regex": lead_exclude_regex,
        "enabled": enabled,
        "p12_report_group_id": str(payload.get("p12_report_group_id", "")).strip(),
        "p12_report_template": str(payload.get("p12_report_template", "")).strip(),
        "p12_data_report_template": str(payload.get("p12_data_report_template", "")).strip(),
        "internal_notify_group_id": str(payload.get("internal_notify_group_id", "")).strip(),
        "internal_lead_template": str(payload.get("internal_lead_template", "")).strip(),
        "internal_weekly_template": str(payload.get("internal_weekly_template", "")).strip(),
        "internal_notify_message": "",
    }
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        new_id = persistence.insert_meta_client(new_client)
        fresh = persistence.get_meta_client(new_id)
        if not fresh:
            return jsonify({"ok": False, "error": "falha_ao_ler_cliente"}), 500
    else:
        clients = _load_clients()
        clients.append(new_client)
        new_id = len(clients) - 1
        fresh = {**new_client, "id": new_id}
        _save_clients(clients)

    publish_event(
        source="dashboard_app",
        stage="CLIENTE_ADICIONADO",
        status="ok",
        detail="Novo cliente adicionado via dashboard",
        client_name=client_name,
        ad_account_id=ad_account_id,
        page_id=meta_page_id,
        group_id=group_id,
    )

    return jsonify({"ok": True, "client": _public_client_payload(fresh, _latest_events_by_client())})


@app.put("/api/clients/<int:client_id>")
def api_update_client(client_id: int) -> Any:
    payload = request.get_json(silent=True) or {}
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        current = persistence.get_meta_client(client_id)
        if not current:
            return jsonify({"ok": False, "error": "cliente_nao_encontrado"}), 404
        current = dict(current)
    else:
        clients = _load_clients()
        if client_id < 0 or client_id >= len(clients):
            return jsonify({"ok": False, "error": "cliente_nao_encontrado"}), 404
        current = dict(clients[client_id])

    updatable_fields = {
        "client_name",
        "ad_account_id",
        "group_id",
        "meta_page_id",
        "lead_group_id",
        "lead_phone_number",
        "lead_template",
        "lead_exclude_fields",
        "lead_exclude_contains",
        "lead_exclude_regex",
        "enabled",
        "p12_report_group_id",
        "p12_report_template",
        "p12_data_report_template",
        "internal_notify_group_id",
        "internal_lead_template",
        "internal_weekly_template",
    }
    for key in updatable_fields:
        if key not in payload:
            continue
        if key == "ad_account_id":
            current[key] = _normalize_act_id(str(payload[key]))
        elif key == "enabled":
            current[key] = _as_bool(payload[key], default=True)
        elif key in {"lead_exclude_fields", "lead_exclude_contains", "lead_exclude_regex"}:
            current[key] = _csv_list(payload[key])
        else:
            current[key] = str(payload[key]).strip()

    if persistence.db_enabled():
        persistence.update_meta_client(client_id, current)
        fresh = persistence.get_meta_client(client_id) or current
    else:
        clients = _load_clients()
        current["id"] = client_id
        clients[client_id] = current
        _save_clients(clients)
        fresh = current

    publish_event(
        source="dashboard_app",
        stage="CLIENTE_ATUALIZADO",
        status="info",
        detail="Cliente atualizado via dashboard",
        client_name=str(fresh.get("client_name", "")).strip(),
        ad_account_id=str(fresh.get("ad_account_id", "")).strip(),
        page_id=str(fresh.get("meta_page_id", "")).strip(),
        group_id=str(fresh.get("group_id", "")).strip(),
    )
    return jsonify({"ok": True, "client": _public_client_payload(fresh, _latest_events_by_client())})


@app.get("/api/google-clients")
def api_google_clients() -> Any:
    return jsonify(_build_google_clients_response())


@app.post("/api/google-clients")
def api_add_google_client() -> Any:
    payload = request.get_json(silent=True) or {}
    client_name = str(payload.get("client_name", "")).strip()
    google_customer_id = _normalize_google_customer_id(str(payload.get("google_customer_id", "")).strip())
    group_id = str(payload.get("group_id", "")).strip()
    notes = str(payload.get("notes", "")).strip()
    google_template = str(payload.get("google_template", "default")).strip() or "default"
    enabled = _as_bool(payload.get("enabled"), default=True)
    primary = payload.get("primary_conversions")
    if isinstance(primary, str):
        primary_conversions = [x.strip() for x in primary.split(",") if x.strip()]
    elif isinstance(primary, list):
        primary_conversions = [str(x).strip() for x in primary if str(x).strip()]
    else:
        primary_conversions = []

    if not client_name:
        return jsonify({"ok": False, "error": "client_name_obrigatorio"}), 400
    if not google_customer_id:
        return jsonify({"ok": False, "error": "google_customer_id_obrigatorio"}), 400
    if not group_id:
        return jsonify({"ok": False, "error": "group_id_obrigatorio"}), 400

    new_client = {
        "client_name": client_name,
        "google_customer_id": google_customer_id,
        "group_id": group_id,
        "enabled": enabled,
        "primary_conversions": primary_conversions,
        "notes": notes,
        "google_template": google_template,
        "lead_phone_number": str(payload.get("lead_phone_number", "")).strip(),
        "p12_report_group_id": str(payload.get("p12_report_group_id", "")).strip(),
        "p12_report_template": str(payload.get("p12_report_template", "")).strip(),
        "p12_data_report_template": str(payload.get("p12_data_report_template", "")).strip(),
        "internal_notify_group_id": str(payload.get("internal_notify_group_id", "")).strip(),
        "internal_lead_template": "",
        "internal_weekly_template": str(payload.get("internal_weekly_template", "")).strip(),
        "internal_notify_message": "",
    }
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        new_id = persistence.insert_google_client(new_client)
        fresh = persistence.get_google_client(new_id)
        if not fresh:
            return jsonify({"ok": False, "error": "falha_ao_ler_cliente"}), 500
    else:
        clients = _load_google_clients()
        slug = re.sub(r"[^a-z0-9_]+", "_", client_name.lower()).strip("_") or f"google_{len(clients) + 1}"
        row_to_file = {**new_client, "id": slug}
        clients.append(row_to_file)
        new_id = len(clients) - 1
        fresh = {**new_client, "id": new_id}
        _save_google_clients(clients)

    publish_event(
        source="dashboard_app",
        stage="GOOGLE_CLIENTE_ADICIONADO",
        status="ok",
        detail="Novo cliente Google Ads adicionado via dashboard",
        client_name=client_name,
        group_id=group_id,
    )
    return jsonify({"ok": True, "client": _public_google_client_payload(fresh)})


@app.put("/api/google-clients/<int:client_id>")
def api_update_google_client(client_id: int) -> Any:
    payload = request.get_json(silent=True) or {}
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        current = persistence.get_google_client(client_id)
        if not current:
            return jsonify({"ok": False, "error": "cliente_nao_encontrado"}), 404
        current = dict(current)
    else:
        clients = _load_google_clients()
        if client_id < 0 or client_id >= len(clients):
            return jsonify({"ok": False, "error": "cliente_nao_encontrado"}), 404
        current = dict(clients[client_id])

    updatable = {
        "client_name",
        "google_customer_id",
        "group_id",
        "enabled",
        "notes",
        "google_template",
        "primary_conversions",
        "lead_phone_number",
        "p12_report_group_id",
        "p12_report_template",
        "p12_data_report_template",
        "internal_notify_group_id",
        "internal_lead_template",
        "internal_weekly_template",
    }
    for key in updatable:
        if key not in payload:
            continue
        if key == "enabled":
            current[key] = _as_bool(payload[key], default=True)
        elif key == "google_customer_id":
            current[key] = _normalize_google_customer_id(str(payload[key]))
        elif key == "primary_conversions":
            val = payload[key]
            if isinstance(val, str):
                current[key] = [x.strip() for x in val.split(",") if x.strip()]
            elif isinstance(val, list):
                current[key] = [str(x).strip() for x in val if str(x).strip()]
            else:
                current[key] = []
        else:
            current[key] = str(payload[key]).strip()

    if persistence.db_enabled():
        persistence.update_google_client(client_id, current)
        fresh = persistence.get_google_client(client_id) or current
    else:
        clients = _load_google_clients()
        current["id"] = client_id
        clients[client_id] = current
        _save_google_clients(clients)
        fresh = current

    publish_event(
        source="dashboard_app",
        stage="GOOGLE_CLIENTE_ATUALIZADO",
        status="info",
        detail="Cliente Google Ads atualizado via dashboard",
        client_name=str(fresh.get("client_name", "")).strip(),
        group_id=str(fresh.get("group_id", "")).strip(),
    )
    return jsonify({"ok": True, "client": _public_google_client_payload(fresh)})


@app.get("/api/site-lead-routes")
def api_site_lead_routes() -> Any:
    return jsonify({"ok": True, **_build_site_lead_routes_response()})


@app.post("/api/site-lead-routes")
def api_add_site_lead_route() -> Any:
    payload = request.get_json(silent=True) or {}
    route_data = {
        "codi_id": str(payload.get("codi_id", payload.get("form_id", ""))).strip(),
        "target_type": str(payload.get("target_type", "meta")).strip().lower() or "meta",
        "target_client_name": str(payload.get("target_client_name", "")).strip(),
        "source_type": str(payload.get("source_type", "")).strip().lower(),
        "enabled": _as_bool(payload.get("enabled"), default=True),
        "notes": str(payload.get("notes", "")).strip(),
    }
    if not route_data["codi_id"]:
        return jsonify({"ok": False, "error": "codi_id_obrigatorio"}), 400
    if not _is_valid_site_codi_id(route_data["codi_id"]):
        return jsonify({"ok": False, "error": "codi_id_invalido_32_digitos_numericos"}), 400
    if route_data["target_type"] not in {"meta", "google"}:
        return jsonify({"ok": False, "error": "target_type_invalido"}), 400
    if not route_data["target_client_name"]:
        return jsonify({"ok": False, "error": "target_client_name_obrigatorio"}), 400
    try:
        if persistence.db_enabled():
            persistence.ensure_db_ready()
            new_id = persistence.insert_site_lead_route(route_data)
            fresh = persistence.get_site_lead_route(new_id)
        else:
            rows = _load_site_lead_routes()
            if any(
                str(r.get("codi_id", r.get("form_id", ""))).strip().lower() == route_data["codi_id"].lower()
                for r in rows
                if isinstance(r, dict)
            ):
                return jsonify({"ok": False, "error": "codi_id_duplicado"}), 409
            new_id = max([int(r.get("id", 0)) for r in rows if isinstance(r, dict)] or [0]) + 1
            fresh = {**route_data, "id": new_id}
            rows.append(fresh)
            _save_site_lead_routes(rows)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    publish_event(
        source="dashboard_app",
        stage="SITE_ROUTE_ADICIONADA",
        status="ok",
        detail=f"Rota de lead site adicionada ({route_data['codi_id']})",
        payload={"codi_id": route_data["codi_id"], "target_type": route_data["target_type"]},
    )
    return jsonify({"ok": True, "route": _public_site_lead_route_payload(fresh or {**route_data, "id": new_id})})


@app.put("/api/site-lead-routes/<int:route_id>")
def api_update_site_lead_route(route_id: int) -> Any:
    payload = request.get_json(silent=True) or {}
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        current = persistence.get_site_lead_route(route_id)
    else:
        current = next((r for r in _load_site_lead_routes() if int(r.get("id", -1)) == route_id), None)
    if not current:
        return jsonify({"ok": False, "error": "route_nao_encontrada"}), 404
    current = dict(current)
    updatable = {"codi_id", "form_id", "target_type", "target_client_name", "source_type", "enabled", "notes"}
    for key in updatable:
        if key not in payload:
            continue
        if key == "form_id":
            current["codi_id"] = str(payload[key]).strip()
        elif key == "enabled":
            current[key] = _as_bool(payload[key], default=True)
        elif key in {"target_type", "source_type"}:
            current[key] = str(payload[key]).strip().lower()
        else:
            current[key] = str(payload[key]).strip()
    if "form_id" in current and "codi_id" not in current:
        current["codi_id"] = str(current.get("form_id", "")).strip()
    if not str(current.get("codi_id", "")).strip():
        return jsonify({"ok": False, "error": "codi_id_obrigatorio"}), 400
    if not _is_valid_site_codi_id(str(current.get("codi_id", ""))):
        return jsonify({"ok": False, "error": "codi_id_invalido_32_digitos_numericos"}), 400
    if str(current.get("target_type", "")).strip() not in {"meta", "google"}:
        return jsonify({"ok": False, "error": "target_type_invalido"}), 400
    if not str(current.get("target_client_name", "")).strip():
        return jsonify({"ok": False, "error": "target_client_name_obrigatorio"}), 400
    try:
        if persistence.db_enabled():
            persistence.update_site_lead_route(route_id, current)
            fresh = persistence.get_site_lead_route(route_id) or current
        else:
            rows = _load_site_lead_routes()
            for row in rows:
                if int(row.get("id", -1)) == route_id:
                    continue
                if str(row.get("codi_id", row.get("form_id", ""))).strip().lower() == str(current.get("codi_id", "")).strip().lower():
                    return jsonify({"ok": False, "error": "codi_id_duplicado"}), 409
            for i, row in enumerate(rows):
                if int(row.get("id", -1)) == route_id:
                    rows[i] = {**current, "id": route_id}
                    break
            _save_site_lead_routes(rows)
            fresh = {**current, "id": route_id}
    except ValueError as exc:
        code = 409 if str(exc) == "codi_id_duplicado" else 400
        return jsonify({"ok": False, "error": str(exc)}), code
    publish_event(
        source="dashboard_app",
        stage="SITE_ROUTE_ATUALIZADA",
        status="info",
        detail=f"Rota de lead site atualizada ({fresh.get('codi_id', '')})",
        payload={"route_id": route_id},
    )
    return jsonify({"ok": True, "route": _public_site_lead_route_payload(fresh)})


@app.delete("/api/site-lead-routes/<int:route_id>")
def api_delete_site_lead_route(route_id: int) -> Any:
    if persistence.db_enabled():
        persistence.ensure_db_ready()
        ok = persistence.delete_site_lead_route(route_id)
    else:
        rows = _load_site_lead_routes()
        new_rows = [r for r in rows if int(r.get("id", -1)) != route_id]
        ok = len(new_rows) != len(rows)
        if ok:
            _save_site_lead_routes(new_rows)
    if not ok:
        return jsonify({"ok": False, "error": "route_nao_encontrada"}), 404
    publish_event(
        source="dashboard_app",
        stage="SITE_ROUTE_REMOVIDA",
        status="warning",
        detail=f"Rota de lead site removida (id={route_id})",
        payload={"route_id": route_id},
    )
    return jsonify({"ok": True})


@app.get("/api/message-templates")
def api_message_templates() -> Any:
    return jsonify({"ok": True, **list_templates_payload()})


@app.put("/api/message-templates/<channel>/<template_id>")
def api_upsert_message_template(channel: str, template_id: str) -> Any:
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", template_id)).strip()
    description = str(payload.get("description", "")).strip()
    content = str(payload.get("content", "")).rstrip()
    try:
        data = upsert_template(channel, template_id, name=name, description=description, content=content)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    publish_event(
        source="dashboard_app",
        stage="TEMPLATE_SALVO",
        status="ok",
        detail=f"Template {channel}/{template_id} salvo",
        payload={"channel": channel, "template_id": template_id},
    )
    return jsonify({"ok": True, "template": data})


@app.post("/api/message-templates/preview")
def api_message_template_preview() -> Any:
    payload = request.get_json(silent=True) or {}
    content = str(payload.get("content", "")).rstrip()
    context = payload.get("context") or {}
    if not isinstance(context, dict):
        context = {}
    rendered = render_template_text(content, context)
    return jsonify({"ok": True, "preview": rendered})


@app.put("/api/message-filters/<channel>")
def api_upsert_message_filters(channel: str) -> Any:
    payload = request.get_json(silent=True) or {}
    exclude_exact = _csv_list(payload.get("exclude_exact"))
    exclude_contains = _csv_list(payload.get("exclude_contains"))
    exclude_regex = _csv_list(payload.get("exclude_regex"))
    data = upsert_filter_rules(
        channel,
        exclude_exact=exclude_exact,
        exclude_contains=exclude_contains,
        exclude_regex=exclude_regex,
    )
    publish_event(
        source="dashboard_app",
        stage="FILTRO_SALVO",
        status="ok",
        detail=f"Filtros de mensagem salvos para {channel}",
        payload={"channel": channel},
    )
    return jsonify({"ok": True, "filters": data})


@app.post("/api/harness/simulate-webhook")
def api_harness_simulate_webhook() -> Any:
    payload = request.get_json(silent=True) or {}
    client_id = payload.get("client_id")
    scenario = str(payload.get("scenario", "success")).strip().lower()
    if scenario not in {"success", "send_fail", "route_fail"}:
        return jsonify({"ok": False, "error": "scenario_invalido"}), 400

    clients = _load_clients()
    target: Optional[Dict[str, Any]] = None
    for c in clients:
        if int(c.get("id", -1)) == int(client_id):
            target = c
            break
    if not target:
        return jsonify({"ok": False, "error": "client_id_invalido"}), 400
    _run_flow_async(target, scenario)
    return jsonify(
        {
            "ok": True,
            "message": "simulação iniciada",
            "client_name": target.get("client_name", ""),
            "scenario": scenario,
        }
    )


@app.get("/api/events/recent")
def api_events_recent() -> Any:
    limit = int(request.args.get("limit", "200"))
    limit = max(1, min(limit, 1000))
    return jsonify({"ok": True, "events": read_recent_events(limit=limit)})


@app.get("/api/events/stream")
def api_events_stream() -> Response:
    def sse_pack(event_name: str, data: Dict[str, Any]) -> str:
        return f"event: {event_name}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

    def gen() -> Any:
        bootstrap = read_recent_events(limit=120)
        yield sse_pack("bootstrap", {"events": bootstrap})

        events_file = get_events_file_path()
        try:
            offset = os.path.getsize(events_file)
        except OSError:
            offset = 0

        while True:
            events, new_offset = read_events_since(offset)
            offset = new_offset
            if events:
                for ev in events:
                    yield sse_pack("event", ev)
            else:
                yield ": ping\n\n"
            time.sleep(1.0)

    return Response(gen(), mimetype="text/event-stream")


def main() -> None:
    port = int(os.getenv("DASHBOARD_PORT", "8091"))
    publish_event(
        source="dashboard_app",
        stage="DASHBOARD_START",
        status="info",
        detail=f"Dashboard iniciada na porta {port}",
    )
    serve_flask_app(app, port=port)


if __name__ == "__main__":
    main()
