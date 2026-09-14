"""Журналирование Nomos.

Три канала, все с редакцией секретов:

1. Консоль — человекочитаемый формат Auto-Tasker
   (``%(asctime)s - %(name)s - %(levelname)s - %(message)s``), уровень из settings.
2. ``logs/nomos.log`` — тот же формат, всегда DEBUG (полная картина для отладки
   независимо от уровня консоли).
3. ``logs/{run_id}.jsonl`` — машинный журнал: каждая строка — JSON с run_id,
   этапом (stage), фильтром, номером пачки и traceback-ом. Именно его читает
   Claude при разборе принесённого оператором диагностического бандла.

Контекст (stage/filter/batch) задаётся через contextvars и попадает в каждую
запись автоматически — см. :func:`log_context`.

Использование::

    from nomos.runlog import init_run_logging, log_context

    run_id = init_run_logging(level="INFO")           # в точке входа
    with log_context(stage="events", filter="_1_AD_audit", batch=3):
        logger.info("запрос пачки")                    # контекст попадёт в JSONL
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from nomos.compat import UTC
from nomos.redact import redact

TEXT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_run_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("nomos_run_id", default="-")
_stage_var: contextvars.ContextVar[str] = contextvars.ContextVar("nomos_stage", default="-")
_filter_var: contextvars.ContextVar[str] = contextvars.ContextVar("nomos_filter", default="-")
_batch_var: contextvars.ContextVar[str] = contextvars.ContextVar("nomos_batch", default="-")


def current_run_id() -> str:
    return _run_id_var.get()


@contextmanager
def log_context(stage: str | None = None, filter: str | None = None, batch=None):
    """Временный контекст для JSONL-записей (этап / фильтр / номер пачки)."""
    tokens = []
    if stage is not None:
        tokens.append((_stage_var, _stage_var.set(stage)))
    if filter is not None:
        tokens.append((_filter_var, _filter_var.set(filter)))
    if batch is not None:
        tokens.append((_batch_var, _batch_var.set(str(batch))))
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


class _RedactingFilter(logging.Filter):
    """Редакция секретов и подмешивание контекста в каждую запись."""

    def filter(self, record: logging.LogRecord) -> bool:
        # Сообщение фиксируем как строку и редактируем один раз.
        record.msg = redact(record.getMessage())
        record.args = ()
        record.nomos_run_id = _run_id_var.get()
        record.nomos_stage = _stage_var.get()
        record.nomos_filter = _filter_var.get()
        record.nomos_batch = _batch_var.get()
        return True


class _JsonlFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "nomos_run_id", "-"),
            "stage": getattr(record, "nomos_stage", "-"),
            "filter": getattr(record, "nomos_filter", "-"),
            "batch": getattr(record, "nomos_batch", "-"),
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exc"] = redact(
                "".join(traceback.format_exception(*record.exc_info)).strip()
            )
        return json.dumps(entry, ensure_ascii=False)


def init_run_logging(
    level: str = "INFO",
    logs_dir: Path | str = Path("logs"),
    run_id: str | None = None,
) -> str:
    """Настраивает root-логгер на три канала. Возвращает run_id прогона.

    Идемпотентна: повторный вызов переинициализирует хендлеры (важно для тестов).
    """
    logs_dir = Path(logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    _run_id_var.set(run_id)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(logging.DEBUG)

    redacting = _RedactingFilter()
    text_formatter = logging.Formatter(TEXT_FORMAT, datefmt=DATE_FORMAT)

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(getattr(logging, level.upper(), logging.INFO))
    console.setFormatter(text_formatter)
    console.addFilter(redacting)
    root.addHandler(console)

    text_file = logging.FileHandler(logs_dir / "nomos.log", encoding="utf-8")
    text_file.setLevel(logging.DEBUG)
    text_file.setFormatter(text_formatter)
    text_file.addFilter(redacting)
    root.addHandler(text_file)

    jsonl_file = logging.FileHandler(logs_dir / f"{run_id}.jsonl", encoding="utf-8")
    jsonl_file.setLevel(logging.DEBUG)
    jsonl_file.setFormatter(_JsonlFormatter())
    jsonl_file.addFilter(redacting)
    root.addHandler(jsonl_file)

    logging.getLogger("nomos.runlog").info(f"run_id={run_id}; журналы в {logs_dir.resolve()}")
    return run_id
