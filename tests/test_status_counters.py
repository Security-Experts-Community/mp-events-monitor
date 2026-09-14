"""Регрессия инцидента 06.07.2026 (скриншот оператора, находка N25).

Данные скриншота: 4 идентифицированных актива со статусами
2×'no audit, no os events', 1×'no os events', 1×'ok', 19 no-asset.
Ожидание (подтверждено оператором):
  C10 «Процент актуального аудита»: количество = 2  (ok + no os events)
  C11 «Хостов с полным покрытием»:  количество = 1  (ok + no audit)
Прототип показывал 1/1: ветки счётчиков сравнивали с 'os events'/'audit'
и не срабатывали никогда.
"""

import logging
import sys

sys.argv = ["x"]

from nomos.domain.analysis import status_counter_field  # noqa: E402

SCREENSHOT_STATUSES = [
    "no audit, no os events",
    "no os events",
    "ok",
    "no audit, no os events",
]


class _Stats:
    ok = 0
    no_os_events = 0
    no_audit = 0
    no_audit_no_os_event = 0


def _count(statuses):
    stats = _Stats()
    for status in statuses:
        field = status_counter_field(status)
        setattr(stats, field, getattr(stats, field) + 1)
    return stats


def test_screenshot_scenario_domain_classifier():
    stats = _count(SCREENSHOT_STATUSES)
    assert stats.ok + stats.no_os_events == 2, "C10: аудит актуален у ok и no os events"
    assert stats.ok + stats.no_audit == 1, "C11: события полны у ok и no audit"
    assert stats.no_audit_no_os_event == 2  # комбинированный — ни в одном критерии


def test_no_os_events_branch_alive():
    """Именно эта ветка была мёртвой в прототипе."""
    assert status_counter_field("no os events") == "no_os_events"
    assert status_counter_field("no audit") == "no_audit"


def test_unified_writer_counts_and_writes_c10(tmp_path):
    """Сквозная проверка через реальный XlsxUnited: счётчики и ячейки."""
    from lib.xlsx_unified import XlsxUnited

    u = XlsxUnited(tmp_path, "mp.test", 168, logging.getLogger("t"))
    attrs = ["STATUS", "reports", "asset_info"]
    u.add_united_start_info(attrs)
    all_assets = {
        f"u{i}": {"STATUS": status, "reports": ["F1"], "asset_info": f"host{i}"}
        for i, status in enumerate(SCREENSHOT_STATUSES)
    }
    u.write_assets(all_assets)
    u.write_no_assets([])
    s = u.statistics
    assert (s.ok, s.no_os_events, s.no_audit, s.no_audit_no_os_event) == (1, 1, 0, 2)
    assert s.ok + s.no_os_events == 2 and s.ok + s.no_audit == 1
    u.workbook.close()
    # ячейки в файле: C10=2, C11=1
    import zipfile
    from xml.etree import ElementTree as ET

    with zipfile.ZipFile(u.workbook_path) as zf:
        sheet = zf.read("xl/worksheets/sheet1.xml").decode()
    root = ET.fromstring(sheet)
    ns = {"m": root.tag.split("}")[0].strip("{")}
    cells = {
        c.get("r"): (c.find("m:v", ns).text if c.find("m:v", ns) is not None else None)
        for c in root.iter(f"{{{ns['m']}}}c")
    }
    assert cells.get("C10") == "2", f"C10={cells.get('C10')}"
    assert cells.get("C11") == "1", f"C11={cells.get('C11')}"


def test_per_filter_writer_uses_domain_classifier():
    """Лист simple (xlsx_out) обязан использовать ту же классификацию."""
    import inspect

    from lib import xlsx_out

    src = inspect.getsource(xlsx_out.MonitorXlsxWriter.work_with_asset_dict)
    assert "status_counter_field" in src
    assert '== "os events"' not in src and '== "audit"' not in src  # мёртвые ветки удалены
