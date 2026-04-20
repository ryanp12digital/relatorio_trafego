"""Servidor HTTP unificado: Waitress em producao, Flask dev como fallback."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def serve_flask_app(app: Any, *, port: int) -> None:
    raw = (os.getenv("USE_WAITRESS") or "1").strip().lower()
    use_waitress = raw not in ("0", "false", "no")
    if use_waitress:
        try:
            from waitress import serve

            threads = int(os.getenv("WAITRESS_THREADS") or "6")
            # Linha visível no Easypanel (emoji) + silencia o "Serving on..." duplicado do waitress
            print(
                f"P12 Relatorios: 🌐 Waitress a escutar em http://0.0.0.0:{port} "
                f"(threads={threads}; USE_WAITRESS=1)",
                flush=True,
            )
            logging.getLogger("waitress").setLevel(logging.WARNING)
            serve(app, host="0.0.0.0", port=port, threads=threads, channel_timeout=120)
            return
        except ImportError:
            logger.warning("USE_WAITRESS ativo mas waitress nao instalado; usando servidor Flask")
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host="0.0.0.0", port=port, threaded=True)
