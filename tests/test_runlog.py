import json
import logging

from nomos.runlog import init_run_logging, log_context


def test_jsonl_contains_context_and_redacts(tmp_path):
    run_id = init_run_logging(level="DEBUG", logs_dir=tmp_path, run_id="testrun")
    assert run_id == "testrun"
    logger = logging.getLogger("nomos.test")
    with log_context(stage="events", filter="_1_AD_audit", batch=2):
        logger.info("пачка ушла, token=pat_SECRETSECRETSECRET1234")
    for handler in logging.getLogger().handlers:
        handler.flush()
    lines = (tmp_path / "testrun.jsonl").read_text(encoding="utf-8").strip().splitlines()
    entry = json.loads(lines[-1])
    assert entry["stage"] == "events"
    assert entry["filter"] == "_1_AD_audit"
    assert entry["batch"] == "2"
    assert entry["run_id"] == "testrun"
    assert "pat_SECRET" not in entry["msg"]


def test_exception_traceback_in_jsonl(tmp_path):
    init_run_logging(level="DEBUG", logs_dir=tmp_path, run_id="excrun")
    logger = logging.getLogger("nomos.test")
    try:
        raise RuntimeError("boom password=Qwerty123")
    except RuntimeError:
        logger.exception("упало")
    for handler in logging.getLogger().handlers:
        handler.flush()
    entries = [
        json.loads(line)
        for line in (tmp_path / "excrun.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    exc_entry = next(entry for entry in entries if "exc" in entry)
    assert "RuntimeError" in exc_entry["exc"]
    assert "Qwerty123" not in exc_entry["exc"]
