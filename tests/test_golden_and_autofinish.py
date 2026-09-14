import json
from types import SimpleNamespace

from nomos.autofinish import finalize_run
from nomos.golden import capture_golden, compare_golden
from nomos.runlog import init_run_logging


def _make_out(tmp_path, status="ok"):
    out = tmp_path / "out" / "_1_AD_audit"
    out.mkdir(parents=True, exist_ok=True)
    (out / "!asset_dict.json").write_text(json.dumps({
        "uuid-1": {"statistic": {"STATUS": status, "COUNT": 42, "dur_audit": 1.5}},
    }), encoding="utf-8")
    return tmp_path / "out"


def test_capture_then_compare_ok(tmp_path):
    out = _make_out(tmp_path)
    golden = tmp_path / "golden"
    assert capture_golden(out_dir=out, golden_dir=golden) == 1
    # COUNT/dur_audit волатильны — их изменение не считается расхождением
    _make_out(tmp_path, status="ok")
    (out / "_1_AD_audit" / "!asset_dict.json").write_text(json.dumps({
        "uuid-1": {"statistic": {"STATUS": "ok", "COUNT": 9999, "dur_audit": 77.7}},
    }), encoding="utf-8")
    assert compare_golden(out_dir=out, golden_dir=golden) == 0


def test_compare_detects_status_change(tmp_path):
    out = _make_out(tmp_path)
    golden = tmp_path / "golden"
    capture_golden(out_dir=out, golden_dir=golden)
    _make_out(tmp_path, status="no audit")
    assert compare_golden(out_dir=out, golden_dir=golden) == 1


def test_capture_refuses_overwrite(tmp_path):
    out = _make_out(tmp_path)
    golden = tmp_path / "golden"
    capture_golden(out_dir=out, golden_dir=golden)
    assert capture_golden(out_dir=out, golden_dir=golden) == 0  # пропуск
    assert capture_golden(out_dir=out, golden_dir=golden, overwrite=True) == 1


def test_finalize_run_full_cycle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    init_run_logging(level="INFO", logs_dir=tmp_path / "logs", run_id="fin1")
    out = _make_out(tmp_path)
    (tmp_path / "configs").mkdir()
    settings = SimpleNamespace(debug_dump=True, record_fixtures=True, out_folder=out)

    # Прогон 1: эталона нет -> снимается + бандл
    finalize_run(settings, "fin1", failed=False)
    assert (tmp_path / "golden").is_dir()
    assert (tmp_path / "nomos_debug_fin1.zip").exists()

    # Прогон 2: эталон есть -> сверка (совпадает), эталон не перезаписан
    finalize_run(settings, "fin1", failed=False)

    # Прогон 3: упавший -> эталон не трогаем, бандл собирается
    _make_out(tmp_path, status="no audit")
    finalize_run(settings, "fin1", failed=True)
    golden_text = (tmp_path / "golden" / "_1_AD_audit" / "!asset_dict.json").read_text()
    assert '"STATUS": "ok"' in golden_text  # не перезаписан упавшим прогоном


def test_finalize_noop_without_flags(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = SimpleNamespace(debug_dump=False, record_fixtures=False, out_folder=tmp_path)
    finalize_run(settings, "x", failed=False)
    assert not (tmp_path / "nomos_debug_x.zip").exists()
