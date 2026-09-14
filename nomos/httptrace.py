"""Прозрачная трассировка HTTP-обменов с MaxPatrol.

Задача этапа 0: снять полную картину «запрос → ответ» с прототипа,
НЕ трогая legacy-код (`lib/`), чтобы золотой эталон снимался с
немодифицированной логики. Поэтому инструментация ставится обёрткой
поверх ``requests.Session.request`` и ``aiohttp.ClientSession._request``.

Это осознанно временное решение: на этапе 2 HTTP-слой переезжает в
``maxpatrol_sdk``, и трассировка станет штатной частью клиента.

Каналы вывода:

* лог DEBUG ``nomos.http``: метод, URL, статус, длительность, размер;
* при ``debug_dump=true`` — ``debug/{run_id}/{seq}_{METHOD}_{slug}.json``
  с телами запроса/ответа (секреты редактируются, ответы аутентификации
  не дампятся вовсе);
* при ``record_fixtures=true`` — ``fixtures/{run_id}/`` в том же формате
  плюс ``manifest.jsonl`` — по нему на этапе 1 строится offline-реплей
  для регрессионных тестов.
"""

from __future__ import annotations

import functools
import itertools
import json
import logging
import re
import threading
import time
from pathlib import Path

from nomos.redact import redact, redact_obj

logger = logging.getLogger("nomos.http")

_seq = itertools.count(1)
_seq_lock = threading.Lock()
_installed = False

# Эндпоинты аутентификации: тела не дампим никогда (только статус/URL в логе).
_AUTH_URL_MARKERS = (
    "/connect/token",
    "/account/login",
    "/ui/login",
    "personal_access_tokens",
)

_MAX_INLINE_BODY = 64 * 1024  # в лог — только размер; в дамп — полное тело


class TraceConfig:
    def __init__(
        self,
        run_id: str,
        debug_dump: bool = False,
        record_fixtures: bool = False,
        debug_dir: Path | str = Path("debug"),
        fixtures_dir: Path | str = Path("fixtures"),
    ):
        self.run_id = run_id
        self.debug_dump = debug_dump
        self.record_fixtures = record_fixtures
        self.debug_dir = Path(debug_dir) / run_id
        self.fixtures_dir = Path(fixtures_dir) / run_id
        if debug_dump:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
        if record_fixtures:
            self.fixtures_dir.mkdir(parents=True, exist_ok=True)
            self._manifest = (self.fixtures_dir / "manifest.jsonl").open(
                "a", encoding="utf-8"
            )
        else:
            self._manifest = None


_config: TraceConfig | None = None


def _is_auth_url(url: str) -> bool:
    return any(marker in url for marker in _AUTH_URL_MARKERS)


def _slug(method: str, url: str) -> str:
    path = re.sub(r"^https?://[^/]+", "", url).split("?")[0]
    path = re.sub(r"[0-9a-fA-F-]{32,}", "ID", path)  # UUID/токены в пути → ID
    return f"{method}_{re.sub(r'[^A-Za-z0-9]+', '_', path).strip('_')[:60]}"


def _next_seq() -> int:
    with _seq_lock:
        return next(_seq)


def _write_record(seq: int, record: dict) -> None:
    assert _config is not None
    name = f"{seq:04d}_{record['slug']}.json"
    payload = json.dumps(record, ensure_ascii=False, indent=2, default=str)
    if _config.debug_dump:
        (_config.debug_dir / name).write_text(payload, encoding="utf-8")
    if _config.record_fixtures:
        (_config.fixtures_dir / name).write_text(payload, encoding="utf-8")
        _config._manifest.write(
            json.dumps(
                {
                    "seq": seq,
                    "file": name,
                    "method": record["request"]["method"],
                    "url": record["request"]["url"],
                    "status": record["response"]["status"],
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        _config._manifest.flush()


def _capture(seq: int, method: str, url: str, req_body, status: int, elapsed: float, resp_body):
    """Общий путь: лог + при включённых режимах — дамп на диск."""
    size = len(resp_body) if isinstance(resp_body, (str, bytes)) else "-"
    logger.debug(f"[{seq:04d}] {method} {redact(url)} -> {status} за {elapsed:.2f}с, ~{size} байт")
    if _config is None or not (_config.debug_dump or _config.record_fixtures):
        return
    if _is_auth_url(url):
        return  # тела аутентификации не пишем никогда
    if isinstance(resp_body, bytes):
        try:
            resp_body = resp_body.decode("utf-8")
        except UnicodeDecodeError:
            resp_body = f"<binary {len(resp_body)} bytes>"
    if isinstance(resp_body, str):
        try:
            resp_body = json.loads(resp_body)
        except (json.JSONDecodeError, ValueError):
            pass
    record = {
        "slug": _slug(method, url),
        "request": {
            "method": method,
            "url": redact(url),
            "body": redact_obj(req_body) if req_body is not None else None,
        },
        "response": {
            "status": status,
            "elapsed_s": round(elapsed, 3),
            "body": redact_obj(resp_body),
        },
    }
    try:
        _write_record(seq, record)
    except OSError as err:  # диск/права — не роняем прогон из-за отладки
        logger.warning(f"не удалось записать HTTP-дамп seq={seq}: {err}")


def install_http_tracing(config: TraceConfig) -> None:
    """Ставит обёртки на requests и aiohttp. Повторный вызов обновляет конфиг."""
    global _config, _installed
    _config = config
    if _installed:
        return
    _installed = True

    # ---- requests -------------------------------------------------------
    import requests

    original_request = requests.sessions.Session.request

    @functools.wraps(original_request)
    def traced_request(self, method, url, **kwargs):
        seq = _next_seq()
        req_body = kwargs.get("json")
        if req_body is None and isinstance(kwargs.get("data"), (dict, str)):
            req_body = kwargs["data"]
        start = time.monotonic()
        try:
            response = original_request(self, method, url, **kwargs)
        except Exception as err:
            logger.debug(
                f"[{seq:04d}] {method} {redact(str(url))} -> "
                f"EXC {type(err).__name__}: {redact(str(err))}"
            )
            raise
        elapsed = time.monotonic() - start
        body = None
        need = _config and (_config.debug_dump or _config.record_fixtures)
        if need and not _is_auth_url(str(url)):
            # .content уже прочитан requests-ом (stream=False в прототипе) — безопасно
            if not kwargs.get("stream"):
                body = response.content
        else:
            body = response.headers.get("Content-Length", "-")
        _capture(seq, str(method).upper(), str(url), req_body, response.status_code, elapsed, body)
        return response

    requests.sessions.Session.request = traced_request
    logger.debug("трассировка requests установлена")

    # ---- aiohttp ---------------------------------------------------------
    import aiohttp

    original_arequest = aiohttp.ClientSession._request

    @functools.wraps(original_arequest)
    async def traced_arequest(self, method, url, **kwargs):
        seq = _next_seq()
        req_body = kwargs.get("json")
        start = time.monotonic()
        try:
            response = await original_arequest(self, method, url, **kwargs)
        except Exception as err:
            logger.debug(
                f"[{seq:04d}] {method} {redact(str(url))} -> "
                f"EXC {type(err).__name__}: {redact(str(err))}"
            )
            raise
        elapsed = time.monotonic() - start

        need_body = bool(
            _config
            and (_config.debug_dump or _config.record_fixtures)
            and not _is_auth_url(str(url))
        )
        if need_body:
            # Перехватываем чтение тела вызывающим кодом (.json()/.text()),
            # не потребляя поток самостоятельно.
            original_json = response.json

            @functools.wraps(original_json)
            async def tee_json(*a, **kw):
                data = await original_json(*a, **kw)
                _capture(
                    seq, str(method).upper(), str(url), req_body,
                    response.status, elapsed, json.dumps(data, ensure_ascii=False),
                )
                return data

            response.json = tee_json
        else:
            _capture(
                seq, str(method).upper(), str(url), req_body,
                response.status, elapsed,
                response.headers.get("Content-Length", "-"),
            )
        return response

    aiohttp.ClientSession._request = traced_arequest
    logger.debug("трассировка aiohttp установлена")
