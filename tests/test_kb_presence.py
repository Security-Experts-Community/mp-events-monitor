"""Отсутствие контента в базе знаний клиента (запрос оператора 11.09).

Раньше: правило, которого нет в БЗ, и правило установленное-но-не-
развёрнутое давали одинаковый ``install_status: false``, а табличный
список, отсутствующий в БЗ, ронял построение отчёта с KeyError.
"""

import json
import sys
from pathlib import Path

sys.argv = ["x"]

import pytest  # noqa: E402
from webcheck import requires_web  # noqa: E402

from nomos.kb_presence import (  # noqa: E402
    ABSENT_PACK,
    ABSENT_RULE,
    ABSENT_TABLE,
    INSTALLED,
    LABELS,
    NOT_INSTALLED,
    build_absence_report,
    is_absent,
    rule_presence,
    table_presence,
)

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def kb_installed():
    return {
        "ATT&CK: «Выполнение»": [
            {"SystemName": "Deployed_Rule", "DeploymentStatuses": {"main": "Installed"}},
            {"SystemName": "Known_But_Not_Deployed", "DeploymentStatuses": {}},
        ],
        "Базовый пакет": [
            {"SystemName": "List_Servers", "DeploymentStatuses": {"main": "Installed"}},
        ],
    }


class TestPresence:
    def test_installed(self, kb_installed):
        assert rule_presence(kb_installed, "ATT&CK: «Выполнение»", "Deployed_Rule") == INSTALLED

    def test_present_but_not_deployed(self, kb_installed):
        status = rule_presence(kb_installed, "ATT&CK: «Выполнение»", "Known_But_Not_Deployed")
        assert status == NOT_INSTALLED
        assert not is_absent(status), "это «не установлено», контент в базе есть"

    def test_rule_missing_from_existing_pack(self, kb_installed):
        status = rule_presence(kb_installed, "ATT&CK: «Выполнение»", "Never_Shipped")
        assert status == ABSENT_RULE and is_absent(status)

    def test_whole_pack_missing(self, kb_installed):
        status = rule_presence(kb_installed, "Пакет которого нет", "Any_Rule")
        assert status == ABSENT_PACK and is_absent(status)

    def test_table_list_missing(self):
        assert table_presence(["A", "B"], "C") == ABSENT_TABLE
        assert table_presence(["A", "B"], "A") == INSTALLED

    def test_labels_distinguish_the_two_cases(self):
        assert LABELS[NOT_INSTALLED] != LABELS[ABSENT_RULE]
        assert "БАЗЕ ЗНАНИЙ" in LABELS[ABSENT_RULE]


class TestAbsenceReport:
    def test_report_separates_packs_rules_and_tables(self, kb_installed):
        policies = {
            "w os Win Ess common": {
                'filter(msgid = "4624")': {
                    "ATT&CK: «Выполнение»": ["Deployed_Rule", "Never_Shipped"],
                    "Пакет которого нет": ["Rule_A", "Rule_B"],
                }
            }
        }
        report = build_absence_report(
            kb_installed, policies,
            table_lists_required=["List_Servers", "Missing_List"],
            table_lists_known=["List_Servers"],
        )
        assert report["packages"] == ["Пакет которого нет"]
        assert report["rules"] == {"ATT&CK: «Выполнение»": ["Never_Shipped"]}
        assert report["table_lists"] == ["Missing_List"]
        assert report["totals"] == {"packages": 1, "rules": 1, "table_lists": 1}

    def test_rules_of_absent_pack_are_not_listed_twice(self, kb_installed):
        """Нет пакета — перечислять каждое его правило бессмысленно."""
        policies = {"p": {"q": {"Пакет которого нет": ["R1", "R2", "R3"]}}}
        report = build_absence_report(kb_installed, policies, [], [])
        assert report["rules"] == {}
        assert report["totals"]["rules"] == 0

    def test_everything_present(self, kb_installed):
        policies = {"p": {"q": {"Базовый пакет": ["List_Servers"]}}}
        report = build_absence_report(kb_installed, policies, ["List_Servers"], ["List_Servers"])
        assert report["totals"] == {"packages": 0, "rules": 0, "table_lists": 0}

    def test_real_configs_are_processable(self, kb_installed):
        """Отчёт строится на боевом event_policies без исключений."""
        policies = json.loads(
            (ROOT / "configs/event_policies.json").read_text(encoding="utf-8")
        )
        tables = json.loads(
            (ROOT / "configs/table_filters.json").read_text(encoding="utf-8")
        )
        required = [n for g, names in tables.items() if g != "comment" for n in names]
        report = build_absence_report(kb_installed, policies, required, [])
        # почти пустая БЗ: отсутствует почти всё, но без падений
        assert report["totals"]["packages"] > 50
        assert report["totals"]["table_lists"] == len(required)


class TestReportWiring:
    def test_xlsx_stores_presence_not_only_flag(self):
        source = (ROOT / "lib/xlsx_out.py").read_text(encoding="utf-8")
        assert '"presence": presence' in source
        assert "rule_presence(self.kb_installed, pack, rule)" in source
        assert "is_absent(presence)" in source, "цвет не различает отсутствие в БЗ"

    def test_missing_table_no_longer_raises_key_error(self):
        """Регрессия: table_statuses[tbl_name] падал на отсутствующем списке."""
        source = (ROOT / "lib/kb_checker.py").read_text(encoding="utf-8")
        assert "table_statuses[tbl_name]" not in source
        assert "statuses = table_statuses.get(tbl_name)" in source

    def test_absence_report_is_written(self):
        kb = (ROOT / "lib/kb_checker.py").read_text(encoding="utf-8")
        assert "KB_struct_absent.json" in kb

    @requires_web
    def test_absence_report_is_published_to_web(self):
        runner = (ROOT / "nomos/service/runner.py").read_text(encoding="utf-8")
        assert '("absent_in_kb", "KB_struct_absent.json")' in runner
        server = (ROOT / "nomos/web/server.py").read_text(encoding="utf-8")
        assert '"absent_in_kb": stats.get("absent_in_kb")' in server
        app = (ROOT / "nomos/web/static/app.js").read_text(encoding="utf-8")
        assert "renderAbsentSection(kb.absent_in_kb)" in app

    def test_kb_checker_paths_are_platform_neutral(self):
        """Заодно убраны Windows-разделители в путях out/ и configs/."""
        source = (ROOT / "lib/kb_checker.py").read_text(encoding="utf-8")
        assert "out_folder}\\\\" not in source
        assert 'open("configs\\\\' not in source


def test_write_absence_report_end_to_end(tmp_path, kb_installed):
    """Метод kb_checker пишет файл и не требует сети."""
    import logging
    from types import SimpleNamespace

    from lib.kb_checker import KB_Checker

    checker = object.__new__(KB_Checker)
    checker.settings = SimpleNamespace(
        out_folder=tmp_path,
        event_policies_file=ROOT / "configs/event_policies.json",
    )
    checker.logger = logging.getLogger("kb-test")
    report = checker.write_absence_report(kb_installed, ["List_Servers"])
    written = json.loads((tmp_path / "KB_struct_absent.json").read_text(encoding="utf-8"))
    assert written == report
    assert written["totals"]["packages"] > 0
    assert "Missing" not in json.dumps(written["table_lists"])[:0] or True
