"""
Dashboard viva de clientes da automação P12.

Recursos:
- Visualização de clientes e validações de configuração.
- Cadastro rápido de novo cliente em clients.json.
- Stream em tempo real (SSE) de eventos do webhook/automação.
- Harness para simular fluxo de webhook por cliente.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from flask import Flask, Response, jsonify, render_template, request

from execution.live_events import (
    get_events_file_path,
    publish_event,
    read_events_since,
    read_recent_events,
)

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "..", "templates"),
    static_folder=os.path.join(os.path.dirname(__file__), "..", "static"),
)

_CLIENTS_LOCK = threading.Lock()


def _clients_path() -> str:
    return os.path.join(os.path.dirname(__file__), "..", "clients.json")


def _load_clients() -> List[Dict[str, Any]]:
    path = _clients_path()
    if not os.path.exists(path):
        return []
    with _CLIENTS_LOCK:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    if isinstance(data, list):
        return [c for c in data if isinstance(c, dict)]
    return []


def _save_clients(clients: List[Dict[str, Any]]) -> None:
    path = _clients_path()
    with _CLIENTS_LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(clients, f, ensure_ascii=False, indent=2)
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


def _latest_events_by_client(limit: int = 300, per_client: int = 25) -> Dict[str, List[Dict[str, Any]]]:
    events = read_recent_events(limit=limit)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for ev in events:
        client_name = str(ev.get("client_name", "")).strip() or "__global__"
        grouped.setdefault(client_name, []).append(ev)
    for key, rows in grouped.items():
        grouped[key] = rows[-per_client:]
    return grouped


def _public_client_payload(index: int, raw: Dict[str, Any], events_map: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    client = {
        "client_name": str(raw.get("client_name", "")).strip(),
        "ad_account_id": _normalize_act_id(str(raw.get("ad_account_id", ""))),
        "group_id": str(raw.get("group_id", "")).strip(),
        "meta_page_id": str(raw.get("meta_page_id", "")).strip(),
        "lead_group_id": str(raw.get("lead_group_id", "")).strip(),
        "lead_phone_number": str(raw.get("lead_phone_number", "")).strip(),
        "lead_template": str(raw.get("lead_template", "")).strip() or "default",
        "enabled": bool(raw.get("enabled", True)),
    }
    checks = _validate_client(client)
    client_events = events_map.get(client["client_name"], [])
    return {
        "id": index,
        **client,
        "checks": checks,
        "events": client_events,
    }


def _build_clients_response() -> Dict[str, Any]:
    clients = _load_clients()
    events_map = _latest_events_by_client()
    payload = [_public_client_payload(i, c, events_map) for i, c in enumerate(clients)]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "clients": payload,
        "global_events": events_map.get("__global__", []),
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
    enabled = _as_bool(payload.get("enabled"), default=True)

    if not client_name:
        return jsonify({"ok": False, "error": "client_name_obrigatorio"}), 400
    if not ad_account_id:
        return jsonify({"ok": False, "error": "ad_account_id_obrigatorio"}), 400
    if not group_id:
        return jsonify({"ok": False, "error": "group_id_obrigatorio"}), 400

    clients = _load_clients()
    new_client = {
        "client_name": client_name,
        "ad_account_id": ad_account_id,
        "group_id": group_id,
        "meta_page_id": meta_page_id,
        "lead_group_id": lead_group_id,
        "lead_phone_number": lead_phone_number,
        "lead_template": lead_template,
        "enabled": enabled,
    }
    clients.append(new_client)
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

    return jsonify({"ok": True, "client": _public_client_payload(len(clients) - 1, new_client, _latest_events_by_client())})


@app.put("/api/clients/<int:client_id>")
def api_update_client(client_id: int) -> Any:
    payload = request.get_json(silent=True) or {}
    clients = _load_clients()
    if client_id < 0 or client_id >= len(clients):
        return jsonify({"ok": False, "error": "cliente_nao_encontrado"}), 404

    current = clients[client_id]
    updatable_fields = {
        "client_name",
        "ad_account_id",
        "group_id",
        "meta_page_id",
        "lead_group_id",
        "lead_phone_number",
        "lead_template",
        "enabled",
    }
    for key in updatable_fields:
        if key not in payload:
            continue
        if key == "ad_account_id":
            current[key] = _normalize_act_id(str(payload[key]))
        elif key == "enabled":
            current[key] = _as_bool(payload[key], default=True)
        else:
            current[key] = str(payload[key]).strip()

    clients[client_id] = current
    _save_clients(clients)

    publish_event(
        source="dashboard_app",
        stage="CLIENTE_ATUALIZADO",
        status="info",
        detail="Cliente atualizado via dashboard",
        client_name=str(current.get("client_name", "")).strip(),
        ad_account_id=str(current.get("ad_account_id", "")).strip(),
        page_id=str(current.get("meta_page_id", "")).strip(),
        group_id=str(current.get("group_id", "")).strip(),
    )
    return jsonify({"ok": True, "client": _public_client_payload(client_id, current, _latest_events_by_client())})


@app.post("/api/harness/simulate-webhook")
def api_harness_simulate_webhook() -> Any:
    payload = request.get_json(silent=True) or {}
    client_id = payload.get("client_id")
    scenario = str(payload.get("scenario", "success")).strip().lower()
    if scenario not in {"success", "send_fail", "route_fail"}:
        return jsonify({"ok": False, "error": "scenario_invalido"}), 400

    clients = _load_clients()
    if not isinstance(client_id, int) or client_id < 0 or client_id >= len(clients):
        return jsonify({"ok": False, "error": "client_id_invalido"}), 400

    target = clients[client_id]
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
    app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
