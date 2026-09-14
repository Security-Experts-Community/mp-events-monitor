"""Пакет nomos — новая кодовая база (этапы 0+ рефакторинга).

Legacy-прототип живёт в ``lib/`` и ``Nomos.py`` и постепенно переезжает сюда.
На этапе 0 пакет содержит отладочную обвязку: журналирование, редакцию
секретов, HTTP-трассировку и диагностический бандл.
"""

from nomos.debug_bundle import make_debug_bundle
from nomos.httptrace import TraceConfig, install_http_tracing
from nomos.redact import redact
from nomos.runlog import current_run_id, init_run_logging, log_context

__all__ = [
    "init_run_logging",
    "log_context",
    "current_run_id",
    "install_http_tracing",
    "TraceConfig",
    "redact",
    "make_debug_bundle",
]


def init_debug_harness(settings) -> str:
    """Единая точка включения обвязки из точки входа.

    Принимает объект Settings прототипа (или совместимый), возвращает run_id.
    """
    run_id = init_run_logging(level=getattr(settings, "logging_level", "INFO"))
    install_http_tracing(
        TraceConfig(
            run_id=run_id,
            debug_dump=bool(getattr(settings, "debug_dump", False)),
            record_fixtures=bool(getattr(settings, "record_fixtures", False)),
        )
    )
    # Контекст для автозавершения: бандл соберётся даже при падении
    # на аутентификации, т.е. до входа в main-блок.
    from nomos.autofinish import set_run_context

    set_run_context(settings, run_id)
    return run_id
