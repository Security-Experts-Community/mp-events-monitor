"""Ложные требования «is empty and needs fill» (репорт оператора 15.09).

Оператор получал на каждом прогоне три требования заполнить списки
исключений, которые пустые по своей природе:

    Common_blacklist_regex is empty and needs fill
    Common_blacklist_value is empty and needs fill
    Common_IP_Subnet_Whitelist is empty and needs fill
"""

import json
from pathlib import Path

import pytest

from nomos.tablelists import OPTIONAL_TABLE_LISTS, needs_manual_fill

ROOT = Path(__file__).resolve().parent.parent

ASSETGRID_REPLACED = {
    "AssetGrid_Servers": "List_Servers",
    "Critical_Hosts": "Critical_Hosts_Manual",
    "Service_Accounts": "Service_Accounts_Manual",
    "Vulnerable_Hosts": "",
}


class TestOptionalLists:
    @pytest.mark.parametrize(
        "name",
        ["Common_blacklist_regex", "Common_blacklist_value", "Common_IP_Subnet_Whitelist"],
    )
    def test_three_lists_from_the_report_are_silent(self, name):
        assert not needs_manual_fill(name, ASSETGRID_REPLACED)

    def test_common_whitelist_value_stays_silent(self):
        """Раньше молчал случайно — его просто не было в table_mapping."""
        assert not needs_manual_fill("Common_whitelist_value", ASSETGRID_REPLACED)

    def test_assetgrid_replacements_still_excluded(self):
        """Второй, уже существовавший случай исключения не потерян."""
        assert not needs_manual_fill("Critical_Hosts", ASSETGRID_REPLACED)
        assert not needs_manual_fill("Vulnerable_Hosts", ASSETGRID_REPLACED)

    def test_real_lists_are_still_demanded(self):
        """Пустой список серверов или уволенных — настоящая недоработка."""
        for name in ("List_Servers", "Dismissed_Users", "IOCs_Value", "UNIX_Hosts"):
            assert needs_manual_fill(name, ASSETGRID_REPLACED), name

    def test_works_without_assetgrid_argument(self):
        assert needs_manual_fill("List_Servers")
        assert not needs_manual_fill("Common_blacklist_value")


class TestAgainstRealConfigs:
    def test_excluded_names_exist_in_expertise(self):
        """Опечатка в имени сделала бы исключение молча бесполезным."""
        table_filters = json.loads(
            (ROOT / "configs/table_filters.json").read_text(encoding="utf-8")
        )
        known = {name for group, names in table_filters.items() if group != "comment"
                 for name in names}
        table_mapping = json.loads(
            (ROOT / "configs/table_mapping.json").read_text(encoding="utf-8")
        )
        known |= {name for tables in table_mapping.values() for item in tables
                  for name in item}
        unknown = sorted(OPTIONAL_TABLE_LISTS - known)
        assert not unknown, f"нет таких списков в конфигах: {unknown}"

    def test_reported_lists_are_referenced_by_rules(self):
        """Три списка из репорта действительно встречаются в table_mapping."""
        table_mapping = json.loads(
            (ROOT / "configs/table_mapping.json").read_text(encoding="utf-8")
        )
        referenced = {name for tables in table_mapping.values() for item in tables
                      for name in item}
        for name in ("Common_blacklist_regex", "Common_blacklist_value",
                     "Common_IP_Subnet_Whitelist"):
            assert name in referenced, name


class TestReportWiring:
    def test_xlsx_uses_the_shared_rule(self):
        source = (ROOT / "lib/xlsx_out.py").read_text(encoding="utf-8")
        assert "needs_manual_fill(" in source
        assert "not in tables_to_assets.keys()" not in source, "старая проверка осталась"

    def test_empty_tables_file_feeds_web_and_excel(self):
        """Исключение действует и в xlsx, и на вкладке «Экспертиза»."""
        source = (ROOT / "lib/xlsx_out.py").read_text(encoding="utf-8")
        assert "empty_tables.json" in source
        runner = ROOT / "nomos/service/runner.py"
        if runner.exists():
            assert "empty_tables.json" in runner.read_text(encoding="utf-8")
