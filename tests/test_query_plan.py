"""План предстоящих запросов (флаг dump_queries, запрос оператора 10.09).

Проверяется главное: план обязан совпадать с тем, что реально спросит
EventsWorker этого фильтра, и не должен портить общий объект политик
прогона.
"""

import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

sys.argv = ["x"]

import pytest  # noqa: E402
from webcheck import requires_web  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CONFIGS = ROOT / "configs"


@pytest.fixture()
def settings(tmp_path):
    return SimpleNamespace(
        event_policies_file=CONFIGS / "event_policies.json",
        out_folder=tmp_path,
        mpx_host="stand.example",
        mode="Assets_filters",
        time_delta_hours=168,
        audit_hack=False,
        dl_mode=False,
    )


@pytest.fixture(autouse=True)
def clean_plans():
    from nomos.queryplan import reset_plan_cache

    reset_plan_cache()
    yield
    reset_plan_cache()


def _real_filter(name: str = "_0_networkdevices_for_audit") -> tuple[str, dict]:
    config = json.loads((CONFIGS / "assets_filters.json").read_text(encoding="utf-8"))
    if name in config:
        return name, config[name]
    first = next(k for k in config if k != "comments")
    return first, config[first]


class TestPlanMatchesRun:
    def test_queries_equal_to_what_events_worker_would_ask(self, settings):
        """План строится тем же отбором, что и боевой прогон."""
        from lib.policies_checker import EventPolicies
        from nomos.queryplan import build_filter_plan

        name, body = _real_filter()
        blacklist = body.get("default_politics_blacklist")
        whitelist = body.get("default_politics_whitelist")
        plan = build_filter_plan(
            settings, name, body["PDQL"], blacklist, whitelist,
            body.get("specific_politics"), body.get("mandatory_policies"),
        )
        reference = EventPolicies(settings.event_policies_file, logging.getLogger("t"))
        reference.filter_policies(
            blacklist, whitelist, body.get("specific_politics"),
            body.get("mandatory_policies"),
        )
        planned = sorted(
            (policy["name"], query["number"], query["filter"])
            for policy in plan["policies"]
            for query in policy["queries"]
        )
        expected = sorted(
            (query["name"], query["number"], query["filter"])
            for query in reference.rebuilt_policies
        )
        assert planned == expected
        assert plan["queries_total"] == len(reference.rebuilt_policies)
        assert plan["policies_total"] == len({q["name"] for q in reference.rebuilt_policies})

    def test_full_filter_text_is_included(self, settings):
        from nomos.queryplan import build_filter_plan

        name, body = _real_filter()
        plan = build_filter_plan(
            settings, name, body["PDQL"],
            body.get("default_politics_blacklist"),
            body.get("default_politics_whitelist"),
        )
        query = plan["policies"][0]["queries"][0]
        assert query["full_filter"].startswith("filter(")
        assert "group(key:" in query["full_filter"]
        assert plan["pdql"], "PDQL фильтра должен попадать в план"

    def test_audit_only_filter_plans_no_event_queries(self, settings):
        """Фильтр с whitelist '.*' (только аудит) — событий не спрашиваем."""
        from nomos.queryplan import build_filter_plan

        name, body = _real_filter("_1_AD_audit")
        plan = build_filter_plan(
            settings, name, body["PDQL"],
            body.get("default_politics_blacklist"),
            body.get("default_politics_whitelist"),
        )
        assert plan["queries_total"] == 0
        assert plan["pdql"]

    def test_pdql_list_is_joined_like_prototype(self, settings):
        from nomos.queryplan import build_filter_plan, pdql_text

        assert pdql_text(["filter(", "host.@id != 0)"]) == "filter(host.@id != 0)"
        plan = build_filter_plan(settings, "f", ["a", "b"], ["w os"], None)
        assert plan["pdql"] == "ab"

    def test_audit_hack_policy_planned(self, settings):
        from nomos.queryplan import build_filter_plan

        plan = build_filter_plan(
            settings, "f", "pdql", ["w os Win Ess common"], None, audit_hack=True
        )
        assert any(p["name"] == "Audit Events Hack" for p in plan["policies"])

    def test_shared_policies_object_untouched(self, settings):
        """План не должен подменять политики выполняющемуся фильтру."""
        from lib.policies_checker import EventPolicies
        from nomos.queryplan import build_filter_plan

        shared = EventPolicies(settings.event_policies_file, logging.getLogger("t"))
        shared.filter_policies(["w os Win Ess common"], None, None, None)
        before = [dict(q) for q in shared.rebuilt_policies]
        build_filter_plan(settings, "other", "pdql", [".*"], None)
        assert shared.rebuilt_policies == before


class TestIncrementalFile:
    def test_appends_per_filter_and_stays_valid_json(self, settings):
        from nomos.queryplan import build_filter_plan, record_filter_plan

        path = None
        for name in ("filter_one", "filter_two"):
            plan = build_filter_plan(settings, name, "pdql", ["w os Win Ess common"], None)
            path = record_filter_plan(settings, plan)
            # файл валиден и содержит уже пройденные фильтры — прогон длинный
            document = json.loads(path.read_text(encoding="utf-8"))
            assert [f["filter"] for f in document["filters"]][-1] == name
        document = json.loads(path.read_text(encoding="utf-8"))
        assert [f["filter"] for f in document["filters"]] == ["filter_one", "filter_two"]
        assert document["host"] == "stand.example"
        assert document["time_delta_hours"] == 168
        assert path.name == "query_fixtures.json"
        assert not list(path.parent.glob("*.tmp")), "временный файл не убран"

    def test_repeated_filter_replaces_not_duplicates(self, settings):
        from nomos.queryplan import build_filter_plan, record_filter_plan

        for _ in range(2):
            plan = build_filter_plan(settings, "same", "pdql", ["w os Win Ess common"], None)
            path = record_filter_plan(settings, plan)
        document = json.loads(path.read_text(encoding="utf-8"))
        assert [f["filter"] for f in document["filters"]] == ["same"]

    def test_existing_file_is_continued(self, settings):
        from nomos.queryplan import build_filter_plan, record_filter_plan, reset_plan_cache

        plan = build_filter_plan(settings, "first", "pdql", ["w os Win Ess common"], None)
        record_filter_plan(settings, plan)
        reset_plan_cache()  # имитация перезапуска процесса в ту же папку out/
        plan = build_filter_plan(settings, "second", "pdql", ["w os Win Ess common"], None)
        path = record_filter_plan(settings, plan)
        document = json.loads(path.read_text(encoding="utf-8"))
        assert [f["filter"] for f in document["filters"]] == ["first", "second"]

    def test_plan_is_logged(self, settings, caplog):
        from nomos.queryplan import build_filter_plan, log_filter_plan

        plan = build_filter_plan(settings, "logged", "PDQL_TEXT", ["w os Win Ess common"], None)
        with caplog.at_level(logging.INFO, logger="nomos.queryplan"):
            log_filter_plan(plan)
        text = "\n".join(caplog.messages)
        assert "logged" in text and "PDQL_TEXT" in text
        assert "w os Win Ess common" in text


class TestWiring:
    def test_settings_has_flag(self):
        from lib.settings_checker import Settings

        assert "dump_queries" in Settings.model_fields

    def test_run_loop_plans_before_asking(self):
        source = (ROOT / "Nomos.py").read_text(encoding="utf-8")
        plan_at = source.index("plan_asset_worker(aw")
        ask_at = source.index("aw.assets_take_info(out_folder, True, all_search_values)")
        assert plan_at < ask_at, "план обязан строиться до опроса"
        assert "if self.settings.dump_queries:" in source

    @requires_web
    def test_frontend_checkbox_and_payload(self):
        html = (ROOT / "nomos/web/static/index.html").read_text(encoding="utf-8")
        app = (ROOT / "nomos/web/static/app.js").read_text(encoding="utf-8")
        assert 'id="run-queries"' in html
        assert "dump_queries" in html
        assert 'dump_queries: $("#run-queries").checked' in app

    @requires_web
    def test_api_passes_flag(self, tmp_path, monkeypatch):
        from fastapi.testclient import TestClient

        import nomos.web.server as server
        from nomos.storage import Storage

        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(server, "storage", Storage(tmp_path / "t.db"))
        box = {}

        class FakeRun:
            def __init__(self, settings_overrides=None, storage=None, progress=None):
                box["overrides"] = settings_overrides

        monkeypatch.setattr(server, "CheckRun", FakeRun)
        monkeypatch.setattr(server, "run_in_thread", lambda run: SimpleNamespace(
            is_alive=lambda: False))
        monkeypatch.setattr(server, "_current_thread", None)
        client = TestClient(server.app, raise_server_exceptions=False)
        assert client.post("/api/runs", json={
            "mode": "Assets_filters", "dump_queries": True}).status_code == 202
        assert box["overrides"]["dump_queries"] is True

    def test_plan_goes_into_debug_bundle(self, tmp_path, monkeypatch):
        import zipfile

        from nomos.debug_bundle import make_debug_bundle

        monkeypatch.chdir(tmp_path)
        logs = tmp_path / "logs"
        logs.mkdir()
        (logs / "nomos.log").write_text("log\n", encoding="utf-8")
        (logs / "run1.jsonl").write_text("{}\n", encoding="utf-8")
        out = tmp_path / "out"
        out.mkdir()
        (out / "query_fixtures.json").write_text(
            '{"filters": []}', encoding="utf-8"
        )
        bundle = make_debug_bundle(run_id="run1", out_dir=out)
        with zipfile.ZipFile(bundle) as zf:
            assert "out/query_fixtures.json" in zf.namelist()


class TestAssetIdsOfFilter:
    """UUID активов, которые вернул PDQL фильтра (запрос оператора 10.09)."""

    def _worker(self, settings, ids, no_assets=0):
        return SimpleNamespace(
            settings=settings,
            filter_name="_0_networkdevices_for_audit",
            logger=logging.getLogger("t"),
            _asset_ids=ids,
            _no_asset_rows=no_assets,
        )

    def test_ids_land_in_the_same_record_as_the_plan(self, settings):
        from nomos.queryplan import (
            build_filter_plan,
            record_filter_assets,
            record_filter_plan,
        )

        name, body = _real_filter()
        plan = build_filter_plan(
            settings, name, body["PDQL"],
            body.get("default_politics_blacklist"),
            body.get("default_politics_whitelist"),
        )
        record_filter_plan(settings, plan)
        ids = [f"0000000{i}-0000-0000-0000-00000000000{i}" for i in range(3)]
        path = record_filter_assets(self._worker(settings, ids, no_assets=7))

        document = json.loads(path.read_text(encoding="utf-8"))
        assert len(document["filters"]) == 1, "запись фильтра должна быть одна"
        entry = document["filters"][0]
        assert entry["assets_total"] == 3
        assert entry["asset_ids"] == ids
        assert entry["no_asset_rows"] == 7
        # план не потерян
        assert entry["pdql"] and entry["policies"] and entry["queries_total"]

    def test_works_without_prior_plan(self, settings):
        from nomos.queryplan import record_filter_assets

        path = record_filter_assets(self._worker(settings, ["a", "b"]))
        entry = json.loads(path.read_text(encoding="utf-8"))["filters"][0]
        assert entry["assets_total"] == 2 and entry["asset_ids"] == ["a", "b"]

    def test_empty_selection_is_recorded(self, settings):
        from nomos.queryplan import record_filter_assets

        path = record_filter_assets(self._worker(settings, []))
        entry = json.loads(path.read_text(encoding="utf-8"))["filters"][0]
        assert entry["assets_total"] == 0 and entry["asset_ids"] == []

    def test_count_and_ids_are_logged(self, settings, caplog):
        from nomos.queryplan import record_filter_assets

        ids = [f"uuid-{i}" for i in range(80)]
        with caplog.at_level(logging.INFO, logger="t"):
            record_filter_assets(self._worker(settings, ids))
        text = "\n".join(caplog.messages)
        assert "80" in text and "uuid-0" in text
        assert "ещё 30" in text, "длинный список обрезается в журнале"
        assert "uuid-79" not in text

    def test_replanning_keeps_recorded_ids(self, settings):
        """Повторный план фильтра не должен затирать уже полученные UUID."""
        from nomos.queryplan import (
            build_filter_plan,
            record_filter_assets,
            record_filter_plan,
        )

        name, body = _real_filter()
        record_filter_assets(self._worker(settings, ["x", "y"]))
        plan = build_filter_plan(
            settings, name, body["PDQL"],
            body.get("default_politics_blacklist"),
            body.get("default_politics_whitelist"),
        )
        path = record_filter_plan(settings, plan)
        entry = json.loads(path.read_text(encoding="utf-8"))["filters"][0]
        assert entry["asset_ids"] == ["x", "y"]

    def test_legacy_stores_selection_for_the_plan(self):
        source = (ROOT / "lib/asset.py").read_text(encoding="utf-8")
        assert "self._asset_ids = list(asset_dict.keys())" in source
        assert source.index("asset_dict, asset_fields, no_assets = self.work") < \
            source.index("self._asset_ids")

    def test_ids_written_before_events_are_polled(self):
        """UUID должны появляться в файле в момент «find N assets»,
        а не после многочасового опроса событий (репорт 10.09)."""
        source = (ROOT / "lib/asset.py").read_text(encoding="utf-8")
        record_at = source.index("record_filter_assets(self, self.logger)")
        found_log_at = source.index('self.logger.info(f"find {num_assets} assets")')
        polling_at = source.index("ev = EventsWorker(")
        assert found_log_at < record_at < polling_at

    def test_recording_is_not_deferred_to_the_run_loop(self):
        """Дозаписи после фильтра больше нет — иначе она дублирует раннюю."""
        source = (ROOT / "Nomos.py").read_text(encoding="utf-8")
        assert "record_filter_assets" not in source


class TestRecordedAtSelectionTime:
    """Файл плана должен содержать UUID уже к началу опроса событий."""

    def test_file_has_ids_when_events_worker_starts(self, settings, tmp_path, monkeypatch):
        import lib.asset as asset_module
        from nomos.queryplan import PLAN_FILE_NAME

        seen = {}
        plan_path = tmp_path / PLAN_FILE_NAME

        class FakeEventsWorker:
            def __init__(self, *args, **kwargs):
                # момент старта опроса событий: что уже лежит в плане?
                seen["at_start"] = (
                    json.loads(plan_path.read_text(encoding="utf-8"))
                    if plan_path.exists()
                    else None
                )
                self.statistic = {}
                self.policies = SimpleNamespace(small_policies={})

            async def work(self, *args, **kwargs):
                return None

            def make_readable_out(self, *args, **kwargs):
                return None

        monkeypatch.setattr(asset_module, "EventsWorker", FakeEventsWorker)
        worker_settings = SimpleNamespace(
            dl_mode=False, dump_queries=True, out_folder=tmp_path,
            event_policies_file=settings.event_policies_file,
            event_policies=["w os"], max_uuids_in_siem_query=1000,
            max_threads_for_siem_api=4, mpx_host="stand",
            mode="Assets_filters", time_delta_hours=168, audit_hack=False,
            mpx_group="-1",
        )
        worker = asset_module.AssetWorker(
            worker_settings, None, logging.getLogger("t"), None, "demo_filter",
            {"PDQL": "select(@host)", "group": "-1",
             "default_politics_blacklist": "n os"},
        )
        ids = [f"uuid-{i}" for i in range(28)]
        monkeypatch.setattr(worker, "work", lambda folder: ({i: {} for i in ids}, [], []))
        out_folder = tmp_path / "flt"
        out_folder.mkdir()
        worker.assets_take_info(out_folder, True, {})

        document = seen["at_start"]
        assert document is not None, "план не записан до опроса событий"
        entry = document["filters"][0]
        assert entry["assets_total"] == 28
        assert entry["asset_ids"] == ids


class TestExecutableQueries:
    """Тексты запросов должны быть готовы к вставке в интерфейс MaxPatrol."""

    def test_in_list_appended_exactly_as_requested(self):
        from nomos.queryplan import with_assets

        base = ('filter(event_src.title = "3com") | select(event_src.host) | '
                "group(key: [event_src.asset, event_src.host], agg: COUNT(*) as Cnt)"
                " | sort(Cnt desc) | limit(100000)")
        result = with_assets(base, ["1e297847-adc0-0001-0000-00000000001c"], "event_src.asset")
        assert result == (
            'filter(event_src.title = "3com" and in_list('
            '["1e297847-adc0-0001-0000-00000000001c"], event_src.asset))'
            " | select(event_src.host) | group(key: [event_src.asset, event_src.host],"
            " agg: COUNT(*) as Cnt) | sort(Cnt desc) | limit(100000)"
        )

    def test_values_are_escaped(self):
        from nomos.queryplan import asset_list_literal

        assert asset_list_literal(['a"b', "c\\d"]) == '["a\\"b","c\\\\d"]'

    def test_nested_parentheses_survive(self):
        from nomos.queryplan import with_assets

        base = 'filter(lower(event_src.host) = "x" and (a = 1 or b = 2)) | select(time)'
        result = with_assets(base, ["u1"], "event_src.asset")
        assert result == (
            'filter(lower(event_src.host) = "x" and (a = 1 or b = 2)'
            ' and in_list(["u1"], event_src.asset)) | select(time)'
        )

    def test_no_assets_keeps_filter_untouched(self):
        from nomos.queryplan import with_assets

        base = "filter(a = 1) | select(time)"
        assert with_assets(base, [], "event_src.asset") == base

    def test_substitution_is_not_doubled(self, settings):
        """Повторная запись выборки не должна вкладывать in_list в in_list."""
        from nomos.queryplan import (
            build_filter_plan,
            record_filter_plan,
            update_filter_assets,
        )

        name, body = _real_filter()
        record_filter_plan(settings, build_filter_plan(
            settings, name, body["PDQL"],
            body.get("default_politics_blacklist"),
            body.get("default_politics_whitelist")))
        update_filter_assets(settings, name, ["u1", "u2"])
        path = update_filter_assets(settings, name, ["u3"])
        entry = json.loads(path.read_text(encoding="utf-8"))["filters"][0]
        query = entry["policies"][0]["queries"][0]
        assert query["full_filter"].count("in_list(") == 1
        assert "u1" not in query["full_filter"] and "u3" in query["full_filter"]

    def test_replanning_keeps_substitution(self, settings):
        from nomos.queryplan import (
            build_filter_plan,
            record_filter_plan,
            update_filter_assets,
        )

        name, body = _real_filter()
        args = (settings, name, body["PDQL"],
                body.get("default_politics_blacklist"),
                body.get("default_politics_whitelist"))
        record_filter_plan(settings, build_filter_plan(*args))
        update_filter_assets(settings, name, ["u1"])
        path = record_filter_plan(settings, build_filter_plan(*args))
        entry = json.loads(path.read_text(encoding="utf-8"))["filters"][0]
        assert 'in_list(["u1"], event_src.asset)' in \
            entry["policies"][0]["queries"][0]["full_filter"]

    def test_audit_hack_matches_by_dst_asset(self, settings):
        from nomos.queryplan import build_filter_plan, record_filter_plan, update_filter_assets

        plan = build_filter_plan(
            settings, "f", "pdql", ["w os Win Ess common"], None, audit_hack=True
        )
        record_filter_plan(settings, plan)
        path = update_filter_assets(settings, "f", ["u1"])
        entry = json.loads(path.read_text(encoding="utf-8"))["filters"][0]
        hack = next(p for p in entry["policies"] if p["name"] == "Audit Events Hack")
        assert 'in_list(["u1"], dst.asset)' in hack["queries"][0]["full_filter"]

    def test_packages_are_not_in_the_plan(self, settings):
        """Пакеты экспертизы убраны: к предстоящим запросам не относятся."""
        from nomos.queryplan import build_filter_plan

        name, body = _real_filter()
        plan = build_filter_plan(
            settings, name, body["PDQL"],
            body.get("default_politics_blacklist"),
            body.get("default_politics_whitelist"))
        assert plan["policies"], "политики должны быть"
        assert all("packages" not in policy for policy in plan["policies"])

    def test_batching_is_reported(self, settings, caplog):
        from nomos.queryplan import update_filter_assets

        settings.max_uuids_in_siem_query = 10
        with caplog.at_level(logging.INFO, logger="nomos.queryplan"):
            update_filter_assets(settings, "big", [f"u{i}" for i in range(25)])
        assert "пачками по 10" in "\n".join(caplog.messages)
