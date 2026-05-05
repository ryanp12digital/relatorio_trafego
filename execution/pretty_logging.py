"""
Logs mais legíveis no terminal: campos separados por ` | ` viram linhas com seta;
continuações alinhadas sob o cabeçalho da linha.

Desativar quebra por pipes: LOG_PIPE_MULTILINE=0
"""

from __future__ import annotations

import copy
import logging
import os


def pipes_to_lines(message: str) -> str:
    """Transforma `a | b | c` em bloco com primeira parte na 1ª linha e `↳` nas seguintes."""
    if (os.getenv("LOG_PIPE_MULTILINE") or "1").strip().lower() in ("0", "false", "no", "off"):
        return message
    if " | " not in message:
        return message
    parts = [p.strip() for p in message.split(" | ") if p.strip()]
    if len(parts) <= 1:
        return message
    return parts[0] + "\n" + "\n".join(f"   ↳ {p}" for p in parts[1:])


class ServiceConsoleFormatter(logging.Formatter):
    """
    Console: hora + nível + nome curto do logger + mensagem.
    Mensagens com \\n alinham continuações à direita do cabeçalho.
    """

    def __init__(self) -> None:
        super().__init__(datefmt="%H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        try:
            msg = record.getMessage()
        except Exception:
            return super().format(record)
        ct = self.formatTime(record, self.datefmt)
        lv = (record.levelname or "NA")[:5].ljust(5)
        short_name = record.name.split(".")[-1]
        head = f"{ct} {lv} │ {short_name:22.22s} │ "
        if "\n" not in msg:
            base = head + msg
        else:
            lines = msg.split("\n")
            parts = [head + lines[0]]
            pad = " " * len(head)
            for ln in lines[1:]:
                if ln.strip():
                    parts.append(pad + ln)
            base = "\n".join(parts)
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


class FlattenMultilineFileFormatter(logging.Formatter):
    """Ficheiro: uma linha por registo (útil para grep / agregadores)."""

    def __init__(self, fmt: str) -> None:
        super().__init__(fmt=fmt)

    def format(self, record: logging.LogRecord) -> str:
        try:
            msg = record.getMessage()
        except Exception:
            return super().format(record)
        if "\n" not in msg:
            return super().format(record)
        r = copy.copy(record)
        r.msg = " | ".join(x.strip() for x in msg.split("\n") if x.strip())
        r.args = ()
        return super().format(r)


def configure_process_logging(
    *,
    log_file: Optional[str],
    level: int = logging.INFO,
) -> None:
    """
    Reconfigura o root logger: stdout com formatação legível, ficheiro compacto por linha.
    """
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
    root.setLevel(level)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(
            FlattenMultilineFileFormatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        root.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(ServiceConsoleFormatter())
    root.addHandler(sh)
