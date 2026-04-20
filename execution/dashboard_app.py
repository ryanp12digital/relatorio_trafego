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

from execution import persistence
from execution.evolution_catalog_webhook import process_evolution_catalog_payload
from execution.flask_server import serve_flask_app
from execution.project_paths import (
    clients_json_path,
    ensure_data_dir,
    google_clients_json_path,
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

_CLIENTS_LOCK = threading.Lock()
_GOOGLE_CLIENTS_LOCK = threading.Lock()


def _clients_path() -> str:
    return clients_json_path()


def _google_clients_path() -> str:
    return google_clients_json_path()


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


def _validate_client(client: Dict[str, Any]) -> Dict[str, Any]:
    ad_account_id = str(client.get("ad_account_id", "")).strip()
    group_id = str(client.get("group_id", "")).strip()
    lead_group_id = str(client.get("lead_group_id", "")).strip()
    page_id = str(client.get("meta_page_id", "")).strip()
    enabled = bool(client.get("enabled", True))

    ad_ok = bool(re.fullmatch(r"act_\d{6,}", ad_account_id))
    group_ok = bool(re.fullmatch(r"\d+@g\.us", group_id))
    lead_group_ok = (not lead_group_id) or bool(re.fullmatch(r"\d+@g\.us", lead_group_id))
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
    return jsonify({"ok": True, "db": persistence.db_enabled()})


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
        return jsonify({"ok": False, "error": "invalid_json"}), 400
    hdr = (request.headers.get("X-Webhook-Secret") or "").strip()
    auth = request.headers.get("Authorization") or ""
    bearer = ""
    if auth.lower().startswith("bearer "):
        bearer = auth[7:].strip()
    body, status = process_evolution_catalog_payload(raw, header_secret=hdr, auth_bearer=bearer)
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


@app.get("/")
def dashboard_home() -> str:
    return render_template("dashboard.html", dashboard_base="")


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
