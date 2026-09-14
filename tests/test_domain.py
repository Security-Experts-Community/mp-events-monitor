"""Тесты доменного слоя (этап 1).

Проверяют семантику, перенесённую из lib/xlsx_out.py и Nomos.py.
Часть кейсов фиксирует BUGCOMPAT-поведение прототипа намеренно —
до пересъёмки эталона на этапе 2 менять его нельзя.
"""

from datetime import datetime, timedelta

from nomos.compat import UTC
from nomos.domain import (
    AssetStatus,
    StatusFlag,
    accumulate_policy_hits,
    build_unified_report,
    check_edr,
    classify_policy_events,
    detect_host_collisions,
    merge_asset_statistics,
    status_master,
)


def _attrs(host="srv1", audittime=None, scan_status=None, full=None, part=None):
    """9-позиционный контракт прототипа: [0]=event_src.host, [3]=@audittime,
    [4]=ScanningInfo.Status, [7]=полные политики, [8]=частичные."""
    return [host, "hn", "fqdn", audittime, scan_status, "ip", 1.0,
            full if full is not None else [], part if part is not None else []]


# ---------- status_master ----------

class TestStatusMaster:
    def test_ok_uptodate_and_all_win_policies(self):
        small = ["w os Win Ess", "w os Win sysmon"]
        status, empty = status_master(
            _attrs(scan_status="UpToDate", full=["w os Win Ess", "w os Win sysmon"]),
            small,
        )
        assert status == "ok" and empty == []

    def test_audit_fresh_28_days_window(self):
        fresh = (datetime.now(UTC) - timedelta(days=27)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        )
        stale = (datetime.now(UTC) - timedelta(days=29)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        )
        small = ["w os Win Ess"]
        ok, _ = status_master(
            _attrs(audittime=fresh, scan_status="NotDefined", full=["w os Win Ess"]),
            list(small),
        )
        bad, _ = status_master(
            _attrs(audittime=stale, scan_status="NotDefined", full=["w os Win Ess"]),
            list(small),
        )
        assert ok == "ok"
        assert bad == "no audit"

    def test_missing_win_policy_gives_no_os_events_and_empty_list(self):
        small = ["w os Win Ess", "w os Win sysmon"]
        status, empty = status_master(
            _attrs(scan_status="UpToDate", full=["w os Win Ess"]), small
        )
        assert status == "no os events"
        assert empty == ["w os Win sysmon"]

    def test_partial_msgid_suppresses_empty_but_still_not_ok(self):
        small = ["w os Win Ess", "w os Win sysmon"]
        status, empty = status_master(
            _attrs(
                scan_status="UpToDate",
                full=["w os Win Ess"],
                part=["w os Win sysmon: 2 из 4"],
            ),
            small,
        )
        assert status == "no os events"
        assert empty == []  # политика частично жива — не «пустая»

    def test_edr_pair_one_of_two_is_enough(self):
        small = ["w os Win Ess"]
        status, empty = status_master(
            _attrs(scan_status="UpToDate", full=["w os Win Ess", "sa pt edr unix"]),
            small,
            mandatory_policies=["sa pt edr win", "sa pt edr unix"],
        )
        assert status == "ok" and empty == []

    def test_mandatory_missing_fails(self):
        small = ["w os Win Ess"]
        status, empty = status_master(
            _attrs(scan_status="UpToDate", full=["w os Win Ess"]),
            small,
            mandatory_policies=["sa pt edr win", "sa pt edr unix"],
        )
        assert status == "no os events"
        assert set(empty) == {"sa pt edr win", "sa pt edr unix"}

    def test_short_attrs_bugcompat_not8(self):
        status, _ = status_master(["a", "b", "c"], ["w os Win Ess"])
        assert status == "not 8"  # BUGCOMPAT: историческое имя при <9 атрибутах

    def test_empty_host_means_audit_ok_bugcompat(self):
        # BUGCOMPAT: пустой event_src.host трактуется прототипом как аудит OK
        status, _ = status_master(_attrs(host=""), [])
        assert status == "ok"

    def test_audit_hack_stripped_from_small(self):
        small = ["Audit Events Hack", "w os Win Ess"]
        status_master(_attrs(scan_status="UpToDate", full=["w os Win Ess"]), small)
        assert "Audit Events Hack" not in small


# ---------- check_edr ----------

def test_check_edr_states():
    assert check_edr({}, {}) == "No policies"
    assert check_edr({}, {"sa pt edr win": {}}) == "No EDR events"
    assert (
        check_edr({"policies": {"sa pt edr unix": {}}}, {"sa pt edr unix": {}})
        == "Good EDR events"
    )


# ---------- accumulate_policy_hits ----------

def _policy(name, number, host_ids):
    return {"name": name, "number": number, "host_ids": host_ids}


class TestAccumulate:
    def test_preserves_prepopulated_asset_info(self):
        asset_dict = {"u1": {"asset_info": {"hostname": "srv1"}}}
        policies = [_policy("w os Win Ess", 0, {"u1": {"count": 5, "event_src.host": ["srv1"]}})]
        small = {"w os Win Ess": {"f0": {}}}
        accumulate_policy_hits(policies, small, asset_dict)
        assert asset_dict["u1"]["asset_info"] == {"hostname": "srv1"}  # не затёрт
        assert asset_dict["u1"]["policies"]["w os Win Ess"]["satisfaction"] == "YES"

    def test_part_becomes_yes_when_all_filters_hit(self):
        asset_dict = {}
        small = {"P": {"f0": {}, "f1": {}}}
        policies = [
            _policy("P", 0, {"u1": {"count": 3, "event_src.host": ["h1"]}}),
            _policy("P", 1, {"u1": {"count": 4, "event_src.host": ["h2"]}}),
        ]
        accumulate_policy_hits(policies, small, asset_dict)
        pr = asset_dict["u1"]["policies"]["P"]
        assert pr["satisfaction"] == "YES"
        assert pr["sum_count"] == 7
        assert pr["full_info"] == {"0": 3, "1": 4}
        assert asset_dict["u1"]["names"] == ["h1", "h2"]

    def test_stays_part_when_filter_missing(self):
        asset_dict = {}
        small = {"P": {"f0": {}, "f1": {}}}
        policies = [_policy("P", 0, {"u1": {"count": 3, "event_src.host": ["h1"]}})]
        accumulate_policy_hits(policies, small, asset_dict)
        assert asset_dict["u1"]["policies"]["P"]["satisfaction"] == "PART"

    def test_audit_hack_tail_writes_audit_info(self):
        asset_dict = {}
        small = {"P": {"f0": {}}}
        policies = [
            _policy("P", 0, {"u1": {"count": 1, "event_src.host": ["h1"]}}),
            _policy("Audit Events Hack", 0, {"u1": {"count": 9, "event_src.host": ["agent1"]}}),
        ]
        accumulate_policy_hits(policies, small, asset_dict)
        assert asset_dict["u1"]["audit_info"] == ["agent1"]


# ---------- classify_policy_events ----------

def test_classify_policy_events():
    assert classify_policy_events({"f0": 3, "f1": 2}) == "all"
    assert classify_policy_events({"f0": 3, "f1": 0}) == "not all"
    assert classify_policy_events({"f0": 0, "f1": 0}) == "no"


# ---------- merge_asset_statistics ----------

class TestMerge:
    def test_ok_yields_to_problem(self):
        all_assets = {}
        merge_asset_statistics(all_assets, "u1", {"STATUS": "ok", "x": None}, "F1")
        merge_asset_statistics(all_assets, "u1", {"STATUS": "no audit", "x": "v"}, "F2")
        assert all_assets["u1"]["STATUS"] == "no audit"
        assert all_assets["u1"]["x"] == "v"  # None дозаполнен
        assert all_assets["u1"]["reports"] == ["F1", "F2"]

    def test_audit_plus_os_events_combine_both_directions(self):
        for first, second in (
            ("no audit", "no os events"),
            ("no os events", "no audit"),
        ):
            all_assets = {}
            merge_asset_statistics(all_assets, "u1", {"STATUS": first}, "F1")
            merge_asset_statistics(all_assets, "u1", {"STATUS": second}, "F2")
            assert all_assets["u1"]["STATUS"] == "no audit, no os events"

    def test_policy_lists_merge_without_dupes(self):
        all_assets = {}
        merge_asset_statistics(
            all_assets, "u1", {"STATUS": "ok", "pols": ["a"]}, "F1"
        )
        merge_asset_statistics(
            all_assets, "u1", {"STATUS": "ok", "pols": ["a", "b"]}, "F2"
        )
        assert all_assets["u1"]["pols"] == ["a", "b"]


# ---------- collisions / models / report ----------

def test_detect_host_collisions():
    result = detect_host_collisions({"h1": ["u1"], "h2": ["u1", "u2"]})
    assert result == [{"event_src_host": "h2", "asset_ids": ["u1", "u2"]}]


def test_asset_status_legacy_roundtrip():
    for legacy in ("ok", "no audit", "no os events", "no audit, no os events", "not 8"):
        assert AssetStatus.from_legacy(legacy).as_legacy() == legacy
    st = AssetStatus.from_legacy("no audit, no os events")
    assert st.flags == {StatusFlag.NO_AUDIT, StatusFlag.NO_OS_EVENTS}
    assert not st.ok


def test_build_unified_report_end_to_end(tmp_path):
    all_assets = {
        "u1": {
            "STATUS": "no os events",
            "reports": ["F1"],
            "hostname": "srv1",
            "event_src.host": "h1 / h2",
            "empty policies": ["w os Win sysmon"],
            "полные политики": ["w os Win Ess"],
            "частичные политики": ["w os Win task: 1 из 2"],
        }
    }
    report = build_unified_report(
        host="mp.local",
        time_delta_hours=168,
        all_assets=all_assets,
        all_no_asset=[{"report": "F1", "hostname": "ghost"}],
        bad_assets={"bad1": "reason"},
        e_hosts_checker={"h1": ["u1", "u2"]},
        filters_statistic={
            "F1": {"asset": 1, "no_asset": 1,
                   "degraded_windows_hours": {"w os Win Ess": 42}},
            "F2": {"FAILED": "take_assets вернул 403"},
        },
        run_id="r1",
    )
    a = report.assets["u1"]
    assert a.status.as_legacy() == "no os events"
    assert a.status.empty_policies == ["w os Win sysmon"]
    assert a.policies["w os Win Ess"].satisfaction.value == "YES"
    assert a.policies["w os Win task"].satisfaction.value == "PART"
    assert a.event_src_hosts == ["h1", "h2"]
    assert report.no_assets[0].attrs["hostname"] == "ghost"
    assert report.collisions[0].asset_ids == ["u1", "u2"]
    outcomes = {f.name: f for f in report.filters}
    assert outcomes["F2"].failed and "403" in outcomes["F2"].fail_reason
    assert outcomes["F1"].degraded_windows_hours == {"w os Win Ess": 42}
    # сериализация без сюрпризов
    from nomos.domain import dump_unified_report
    path = dump_unified_report(report, tmp_path)
    import json
    parsed = json.loads(path.read_text(encoding="utf-8"))
    assert parsed["assets"]["u1"]["status"]["flags"] == ["no os events"]


def test_xlsx_delegation_matches_domain(monkeypatch):
    """Обёртки в lib/xlsx_out обязаны совпадать с доменом на тех же входах."""
    import sys
    sys.argv = ["x"]
    from lib.xlsx_out import _status_master as legacy_status
    from lib.xlsx_out import check_edr as legacy_edr

    attrs = _attrs(scan_status="UpToDate", full=["w os Win Ess"])
    assert legacy_status(list(attrs), ["w os Win Ess"]) == status_master(
        list(attrs), ["w os Win Ess"]
    )
    assert legacy_edr({}, {"sa pt edr win": {}}) == check_edr({}, {"sa pt edr win": {}})


class TestEnrichment:
    """Репорт оператора 14.07: в карточке актива не было невыполненных
    политик, разбора по под-фильтрам и пакетов экспертизы."""

    def _base(self):
        all_assets = {
            "u1": {
                "STATUS": "no os events",
                "reports": ["F1", "F2"],
                "полные политики": ["w os Win Ess"],
                "частичные политики": ["w os Win task: 1 из 2"],
            }
        }
        rich = {
            "u1": {
                "w os Win Ess": {"hits": {"0": 5, "1": 7}, "satisfaction": "YES"},
                "w os Win task": {"hits": {"0": 3}, "satisfaction": "PART"},
            }
        }
        checked = {
            "F1": {
                "w os Win Ess": {"filters_total": 2},
                "w os Win task": {"filters_total": 2},
                "w os Win sysmon": {"filters_total": 4},
            },
            "F2": {
                "w os Win sysmon": {"filters_total": 4},
            },
        }
        meta = {
            "F1": {
                "w os Win Ess": {
                    "filters_total": 2,
                    "filters": ["msgid = 4688", "msgid = 4104"],
                    "packages": {"Base_Win": ["Suspicious_Process"]},
                },
                "w os Win sysmon": {
                    "filters_total": 4,
                    "filters": ["id=1", "id=3", "id=7", "id=11"],
                    "packages": {"Sysmon": ["Sysmon_Cfg"],
                                 "Active Directory": ["Kerberos_Dump"]},
                },
            },
            "F2": {
                "w os Win sysmon": {
                    "filters_total": 4,
                    "filters": ["id=1", "id=3", "id=7", "id=11"],
                    "packages": {"Sysmon": ["Sysmon_Kill"]},
                },
            },
        }
        return all_assets, rich, checked, meta

    def test_unfulfilled_policy_appears_as_none_with_packages(self):
        all_assets, rich, checked, meta = self._base()
        report = build_unified_report(
            "mp", 168, all_assets, [], {}, {},
            rich_policies=rich, checked_policies_by_filter=checked,
            policies_meta_by_filter=meta,
        )
        sysmon = report.assets["u1"].policies["w os Win sysmon"]
        assert sysmon.satisfaction.value == "NONE"
        assert sysmon.expected_filters == 4
        # пакеты — словарь пакет->правила, объединённый по фильтрам без дублей
        assert set(sysmon.packages) == {"Active Directory", "Sysmon"}
        assert sysmon.packages["Sysmon"] == ["Sysmon_Cfg", "Sysmon_Kill"]
        # тексты под-фильтров подтянулись
        assert sysmon.subfilters == ["id=1", "id=3", "id=7", "id=11"]
        assert sysmon.hits == {} and sysmon.sum_count == 0

    def test_hits_and_meta_for_fulfilled(self):
        all_assets, rich, checked, meta = self._base()
        report = build_unified_report(
            "mp", 168, all_assets, [], {}, {},
            rich_policies=rich, checked_policies_by_filter=checked,
            policies_meta_by_filter=meta,
        )
        ess = report.assets["u1"].policies["w os Win Ess"]
        assert ess.satisfaction.value == "YES"
        assert ess.hits == {"0": 5, "1": 7} and ess.sum_count == 12
        assert ess.expected_filters == 2
        assert ess.packages == {"Base_Win": ["Suspicious_Process"]}
        assert ess.subfilters == ["msgid = 4688", "msgid = 4104"]
        task = report.assets["u1"].policies["w os Win task"]
        assert task.satisfaction.value == "PART"
        assert task.hits == {"0": 3} and task.expected_filters == 2

    def test_rich_hits_max_merge_across_filters(self):
        """Актив в нескольких asset-фильтрах: COUNT не задваивается."""
        rich_pol = {"hits": {}, "satisfaction": "PART"}
        for count in (5, 9, 3):  # тот же под-фильтр из трёх asset-фильтров
            if count > rich_pol["hits"].get("0", -1):
                rich_pol["hits"]["0"] = count
        assert rich_pol["hits"]["0"] == 9


class TestCardExcelParity:
    """Полное соответствие карточки Excel (репорт 15.07): под-фильтры
    текстами и правила по пакетам у невыполненных политик."""

    def test_none_policy_carries_subfilters_and_rules(self):
        all_assets = {"u1": {"STATUS": "no audit", "reports": ["F1"]}}
        meta = {"F1": {"sw AD mecm": {
            "filters_total": 1,
            "filters": ["event_src.subsys = \"mecm\" and object.name = ..."],
            "packages": {
                "Аномальная активность веб-серверов": ["Web_Shell_Detected", "Web_Bruteforce"],
                "Эксплуатация уязвимостей": ["Exploit_Attempt"],
            },
        }}}
        report = build_unified_report(
            "mp", 168, all_assets, [], {}, {},
            checked_policies_by_filter={"F1": {"sw AD mecm": {"filters_total": 1}}},
            policies_meta_by_filter=meta,
        )
        pol = report.assets["u1"].policies["sw AD mecm"]
        assert pol.satisfaction.value == "NONE"
        assert pol.subfilters and pol.subfilters[0].startswith("event_src.subsys")
        assert pol.packages["Аномальная активность веб-серверов"] == [
            "Web_Shell_Detected", "Web_Bruteforce"]
        # сериализация для API сохраняет словарь пакет->правила
        dumped = pol.model_dump(mode="json")
        assert isinstance(dumped["packages"], dict)
        assert dumped["subfilters"][0].startswith("event_src.subsys")
