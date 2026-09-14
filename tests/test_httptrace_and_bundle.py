import json
import zipfile

import pytest

pytest.importorskip("requests")

import nomos.httptrace as httptrace
from nomos.debug_bundle import make_debug_bundle
from nomos.httptrace import TraceConfig, _capture, _is_auth_url, _slug
from nomos.runlog import init_run_logging


def test_auth_urls_never_dumped():
    assert _is_auth_url("https://mp:3334/connect/token")
    assert _is_auth_url("https://mp:3334/ui/login")
    assert not _is_auth_url("https://mp/api/events/v3/events/aggregation")


def test_slug_normalizes_uuid():
    slug = _slug("GET", "https://mp/api/groups/1e2c2278-58c0-0001-0000-00000000081a?x=1")
    assert "1e2c2278" not in slug and slug.startswith("GET_api_groups")


def test_capture_writes_redacted_dump(tmp_path, monkeypatch):
    cfg = TraceConfig("r1", debug_dump=True, record_fixtures=True,
                      debug_dir=tmp_path / "debug", fixtures_dir=tmp_path / "fix")
    monkeypatch.setattr(httptrace, "_config", cfg)
    _capture(1, "POST", "https://mp/api/x",
             {"pdql": "select(...)", "password": "Hunter2!"},
             200, 0.5, '{"token": "abcabcabc", "rows": 3}')
    dump = json.loads(next((tmp_path / "debug" / "r1").glob("*.json")).read_text())
    assert "Hunter2!" not in json.dumps(dump)
    assert dump["response"]["status"] == 200
    manifest = (tmp_path / "fix" / "r1" / "manifest.jsonl").read_text()
    assert '"status": 200' in manifest


def test_bundle_redacts_env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    init_run_logging(level="INFO", logs_dir=tmp_path / "logs", run_id="b1")
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / ".config.env").write_text(
        "HOST=mp.local\nPERSONAL_TOKEN=pat_ZZZZYYYYXXXX0000AAAA\n", encoding="utf-8")
    bundle = make_debug_bundle(run_id="b1", logs_dir=tmp_path / "logs",
                               debug_dir=tmp_path / "debug", out_dir=tmp_path / "out",
                               configs_dir=tmp_path / "configs", bundle_dir=tmp_path)
    with zipfile.ZipFile(bundle) as zf:
        env = zf.read("configs/config.env.redacted").decode()
        assert "pat_ZZZZ" not in env and "HOST=mp.local" in env
        assert "environment.json" in zf.namelist()
        assert "logs/b1.jsonl" in zf.namelist()
