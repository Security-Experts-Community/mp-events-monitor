"""Табличные списки: какие из пустых действительно требуют заполнения.

Отчёт помечает пустой список как «is empty and needs fill», когда правило
корреляции на него ссылается. Но не всякий пустой список — проблема:

* **списки исключений** (blacklist/whitelist общего назначения) пустые по
  умолчанию и наполняются только тогда, когда у заказчика появляются
  исключения. Пустой такой список означает «исключений нет», а не
  «забыли заполнить»;
* **списки, заменённые asset-grid**: данные приходят из Asset Management,
  руками их не ведут.

До этого модуля исключением был только второй случай, а первый работал
случайно: ``Common_whitelist_value`` не попадал в ``table_mapping``, и
претензий по нему не возникало. У ``Common_blacklist_value``,
``Common_blacklist_regex`` и ``Common_IP_Subnet_Whitelist`` ссылки из
правил есть — и оператор получал три ложных требования на каждом прогоне
(репорт 15.09).
"""

from __future__ import annotations

from collections.abc import Iterable

# Пустые по природе: заполняются только при появлении исключений
OPTIONAL_TABLE_LISTS = frozenset(
    {
        "Common_blacklist_regex",
        "Common_blacklist_value",
        "Common_whitelist_regex",
        "Common_whitelist_value",
        "Common_whitelist_for_labeling",
        "Common_whitelist_for_labeling_regex",
        "Common_IP_Subnet_Whitelist",
    }
)


def needs_manual_fill(
    table_name: str, replaced_by_assetgrid: Iterable[str] = ()
) -> bool:
    """Нужно ли требовать ручного заполнения этого пустого списка."""
    if table_name in OPTIONAL_TABLE_LISTS:
        return False
    return table_name not in set(replaced_by_assetgrid)
