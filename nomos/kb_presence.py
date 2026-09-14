"""Отличие «не установлено» от «вообще нет в базе знаний».

Конфиги Nomos описывают эталонную экспертизу: пакеты, правила корреляции
и табличные списки. У клиента какого-то элемента может не быть в
knowledgebase вовсе — это принципиально другая ситуация, чем
установленный, но не развёрнутый контент:

* не установлено — контент есть в БЗ, его достаточно установить;
* отсутствует в БЗ — контент не поставлен клиенту (нет лицензии на
  пакет, старая версия экспертизы, вырезанный срез), установить его
  нечем, нужна поставка.

До этого модуля разница терялась: правило без развёртывания и правило,
которого нет вовсе, одинаково давали ``install_status: false``, а
табличный список, отсутствующий в БЗ, ронял отчёт с ``KeyError``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

INSTALLED = "installed"
NOT_INSTALLED = "not_installed"
ABSENT_RULE = "absent_rule"
ABSENT_PACK = "absent_pack"
ABSENT_TABLE = "absent_table"

LABELS = {
    INSTALLED: "установлено",
    NOT_INSTALLED: "не установлено",
    ABSENT_RULE: "ОТСУТСТВУЕТ В БАЗЕ ЗНАНИЙ",
    ABSENT_PACK: "ПАКЕТ ОТСУТСТВУЕТ В БАЗЕ ЗНАНИЙ",
    ABSENT_TABLE: "ОТСУТСТВУЕТ В БАЗЕ ЗНАНИЙ",
}

_ABSENT = frozenset({ABSENT_RULE, ABSENT_PACK, ABSENT_TABLE})


def is_absent(status: str) -> bool:
    """Элемента нет в базе знаний (а не «есть, но не установлен»)."""
    return status in _ABSENT


def rule_presence(kb_installed: Mapping, pack: str, rule: str) -> str:
    """Статус правила корреляции относительно базы знаний клиента.

    ``kb_installed`` — структура из KB_struct.json: пакет -> список
    объектов с SystemName и DeploymentStatuses.
    """
    items = kb_installed.get(pack)
    if items is None:
        return ABSENT_PACK
    for item in items:
        if item.get("SystemName") == rule:
            return INSTALLED if (item.get("DeploymentStatuses") or {}) else NOT_INSTALLED
    return ABSENT_RULE


def table_presence(known_tables: Iterable[str], table_name: str) -> str:
    """Статус табличного списка: есть ли он в БЗ клиента вообще."""
    return ABSENT_TABLE if table_name not in set(known_tables) else INSTALLED


def absent_expertise(kb_installed: Mapping, event_policies: Mapping) -> dict:
    """Что из configs/event_policies.json отсутствует в БЗ клиента."""
    packages: set[str] = set()
    rules: dict[str, set[str]] = {}
    for policy in event_policies.values():
        for packs in policy.values():
            for pack, pack_rules in packs.items():
                if pack not in kb_installed:
                    packages.add(pack)
                    continue
                for rule in pack_rules:
                    if rule_presence(kb_installed, pack, rule) == ABSENT_RULE:
                        rules.setdefault(pack, set()).add(rule)
    return {
        "packages": sorted(packages),
        "rules": {pack: sorted(names) for pack, names in sorted(rules.items())},
    }


def build_absence_report(
    kb_installed: Mapping,
    event_policies: Mapping,
    table_lists_required: Iterable[str],
    table_lists_known: Iterable[str],
) -> dict:
    """Сводка «чего нет в базе знаний клиента» для отчёта и веба."""
    expertise = absent_expertise(kb_installed, event_policies)
    known = set(table_lists_known)
    tables = sorted({name for name in table_lists_required if name not in known})
    rules_total = sum(len(names) for names in expertise["rules"].values())
    return {
        "packages": expertise["packages"],
        "rules": expertise["rules"],
        "table_lists": tables,
        "totals": {
            "packages": len(expertise["packages"]),
            "rules": rules_total,
            "table_lists": len(tables),
        },
    }
