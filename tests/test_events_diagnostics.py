"""Диагностика опроса SIEM: сообщения и файлы запросов (репорт 10.09).

Оператор видел «Server error 500 for out\\...\\w_os_Win_Ess_4688_cmd_0.txt»,
а папка была пуста: файл пишется только при DEBUG, тогда как сообщение
ссылалось на него всегда. Теперь в сообщениях — политика, под-фильтр и
пачка, а тела запросов сохраняются ещё и при dump_queries.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

sys.argv = ["x"]

import pytest  # noqa: E402


class FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Отдаёт заранее заданную последовательность ответов."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, **kwargs):
        self.calls.append(kwargs)
        status, payload = self._responses.pop(0) if self._responses else (200, {"rows": []})
        return FakeResponse(status, payload)


def _worker(tmp_path, dump_queries=False, retries=1, level="INFO"):
    from lib.events import EventsWorker

    worker = object.__new__(EventsWorker)
    worker.settings = SimpleNamespace(
        mpx_host="stand", logging_level=level, dump_queries=dump_queries,
        reconnect_times=retries, time_delta_hours=168,
    )
    worker.logger = logging.getLogger("events-test")
    worker.semaphore = asyncio.Semaphore(4)
    worker.statistic = {}
    return worker


def _policy():
    return {
        "name": "w os Win Ess 4688 cmd",
        "number": 0,
        "filter": 'msgid = "4688"',
        "full_filter": 'filter(msgid = "4688") | select(event_src.host)',
    }


def _run(worker, out_dir, responses):
    worker.async_session = FakeSession(responses)
    return asyncio.run(
        worker.take_events("-1", 1788894152, 'msgid = "4688"', out_dir, _policy())
    )


class TestMessages:
    def test_error_names_the_query_not_a_missing_file(self, tmp_path, caplog):
        worker = _worker(tmp_path)
        with caplog.at_level(logging.WARNING, logger="events-test"):
            _run(worker, tmp_path, [(500, {})])
        message = "\n".join(caplog.messages)
        assert "Server error 500" in message
        assert "w os Win Ess 4688 cmd" in message, "не видно, какую политику спрашивали"
        assert "#1" in message, "не виден номер под-фильтра"
        assert ".txt" not in message, "ссылка на файл, которого нет"

    def test_exhausted_retries_are_reported(self, tmp_path, caplog):
        """Молчаливый провал = ложный 'no os events' в отчёте."""
        worker = _worker(tmp_path)
        with caplog.at_level(logging.ERROR, logger="events-test"):
            result = _run(worker, tmp_path, [(500, {})])
        assert result == {}
        message = "\n".join(caplog.messages)
        assert "не выполнен" in message and "no os events" in message

    def test_success_is_silent(self, tmp_path, caplog):
        worker = _worker(tmp_path)
        with caplog.at_level(logging.WARNING, logger="events-test"):
            _run(worker, tmp_path, [(200, {"rows": []})])
        assert not caplog.messages


class TestRequestDumps:
    def test_nothing_written_by_default(self, tmp_path):
        worker = _worker(tmp_path)
        _run(worker, tmp_path, [(200, {"rows": []})])
        assert not list(tmp_path.iterdir()), "лишние файлы при обычном прогоне"

    def test_dump_queries_writes_request_body(self, tmp_path):
        worker = _worker(tmp_path, dump_queries=True)
        _run(worker, tmp_path, [(200, {"rows": []})])
        request_files = list(tmp_path.glob("*.txt"))
        assert request_files, "папка пуста, хотя dump_queries включён"
        body = json.loads(request_files[0].read_text(encoding="utf-8"))
        assert body["data"]["filter"] == 'msgid = "4688"'
        assert body["data"]["timeFrom"] == 1788894152
        assert body["params"] == {"groupId": "-1"}
        assert list(tmp_path.glob("*.json")), "ответ политики тоже сохраняется"

    def test_debug_level_still_writes(self, tmp_path):
        worker = _worker(tmp_path, level="DEBUG")
        _run(worker, tmp_path, [(200, {"rows": []})])
        assert list(tmp_path.glob("*.txt"))


def test_source_has_no_phantom_file_references():
    source = (Path(__file__).resolve().parent.parent / "lib/events.py").read_text(
        encoding="utf-8"
    )
    assert "for {file_path}" not in source, "сообщение снова ссылается на файл"


@pytest.mark.parametrize("status", [500, 502, 503])
def test_server_errors_retry_and_report(tmp_path, caplog, status):
    worker = _worker(tmp_path, retries=2)
    caplog.set_level(logging.WARNING, logger="events-test")

    async def no_sleep(_):
        return None

    original = asyncio.sleep
    asyncio.sleep = no_sleep
    try:
        result = _run(worker, tmp_path, [(status, {}), (200, {"rows": []})])
    finally:
        asyncio.sleep = original
    assert result == {}
    assert f"Server error {status}" in "\n".join(caplog.messages)
