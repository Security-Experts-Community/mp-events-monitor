"""Доменная логика анализа покрытия (этап 1: перенос из lib/xlsx_out.py и Nomos.py).

Функции перенесены с СОХРАНЕНИЕМ СЕМАНТИКИ прототипа бит-в-бит — критерий
этапа 1: `tools/golden.py compare` = «СОВПАЛО». Известные странности
прототипа сохранены намеренно и помечены `# BUGCOMPAT`; их исправление —
этап 2 с пересъёмкой эталона.

Содержимое:
- status_master      <- lib/xlsx_out._status_master  (вычисление STATUS)
- check_edr          <- lib/xlsx_out.check_edr
- accumulate_policy_hits <- чистая часть lib/xlsx_out.create_asset_dict
- classify_policy_events <- lib/xlsx_out.pol_stat_mat (классификация)
- merge_asset_statistics <- Nomos.MaxPatrolEventsMonitor.asset_analyzer
- detect_host_collisions <- Nomos.unified_report (мёртвый print -> фича N17)
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from nomos.compat import UTC


def status_master(
    full_simple_attrs: list,
    small_attrs: list,
    mandatory_policies: list | None = None,
) -> tuple[str, list[str]]:
    """Вычисляет STATUS актива. Перенос _status_master без изменений логики.

    full_simple_attrs — позиционный контракт прототипа (9 элементов):
      [0] event_src.host, [3] host.@audittime, [4] ScanningInfo.Status,
      [7] полностью выполненные политики, [8] частично выполненные.
    Именованная модель приходит на замену на этапе 2 (N15).
    """
    simple_pol_st_os = False
    simple_audit_st = False
    empty_policies: list[str] = []

    if small_attrs and "Audit Events Hack" in small_attrs:
        small_attrs.remove("Audit Events Hack")
    if len(full_simple_attrs) < 9:
        return "not 8", empty_policies  # BUGCOMPAT: имя статуса историческое
    if not full_simple_attrs[0]:
        simple_audit_st = True
    elif full_simple_attrs[4] == "UpToDate":
        simple_audit_st = True
    elif (
        full_simple_attrs[4] == "NotDefined" or full_simple_attrs[4] is None
    ) and full_simple_attrs[3]:
        audit_date = datetime.strptime(full_simple_attrs[3], "%Y-%m-%dT%H:%M:%S%z")
        if (datetime.now(UTC) - audit_date).days < 28:
            simple_audit_st = True
    if small_attrs:
        if full_simple_attrs[7]:
            if (
                small_attrs[0].find("w os Win") != -1
                and full_simple_attrs[7][0].find("w os Win") != -1
            ):
                simple_pol_st_os = True
                for pol in small_attrs:
                    if pol.find("w os Win") != -1:
                        if pol not in full_simple_attrs[7]:
                            simple_pol_st_os = False
                            empty = True
                            for not_all_with_msgid in full_simple_attrs[8]:
                                if not_all_with_msgid.find(pol) != -1:
                                    empty = False
                            if empty:
                                empty_policies.append(pol)
                    else:
                        break
            elif small_attrs[0].find(" os ") == 1:
                for pol in full_simple_attrs[7]:
                    if pol.find(" os ") == 1:
                        simple_pol_st_os = True
                        break
        if mandatory_policies:
            mandatory_policies_copy = mandatory_policies.copy()
            if (
                "sa pt edr win" in mandatory_policies_copy
                and "sa pt edr unix" in mandatory_policies_copy
            ):
                # EDR-пара: достаточно одного из двух агентов
                if "sa pt edr win" in full_simple_attrs[7]:
                    mandatory_policies_copy.remove("sa pt edr unix")
                elif "sa pt edr unix" in full_simple_attrs[7]:
                    mandatory_policies_copy.remove("sa pt edr win")
            for mandatory in mandatory_policies_copy:
                if mandatory not in full_simple_attrs[7]:
                    empty = True
                    for not_all_with_msgid in full_simple_attrs[8]:
                        if not_all_with_msgid.find(mandatory) != -1:
                            empty = False
                    if empty:
                        simple_pol_st_os = False
                        empty_policies.append(mandatory)

        if full_simple_attrs[8]:
            simple_pol_st_os = False
    else:
        simple_pol_st_os = True

    list_to_return = []
    if simple_pol_st_os and simple_audit_st:
        list_to_return.append("ok")
    else:
        if not simple_audit_st:
            list_to_return.append("no audit")
        if not simple_pol_st_os:
            list_to_return.append("no os events")
    return ", ".join(list_to_return), empty_policies


def check_edr(asset: dict, small_policies: dict) -> str:
    """edr_statuses: 'No policies' | 'Good EDR events' | 'No EDR events'."""
    if (
        "sa pt edr win" in small_policies.keys()
        or "sa pt edr unix" in small_policies.keys()
    ):
        if "policies" in asset.keys() and (
            "sa pt edr win" in asset["policies"]
            or "sa pt edr unix" in asset["policies"]
        ):
            return "Good EDR events"
        else:
            return "No EDR events"
    else:
        return "No policies"


def accumulate_policy_hits(
    policies: list[dict], small_policies: dict, asset_dict: dict
) -> dict:
    """Дополняет asset_dict счётчиками host_ids всех политик.

    ВАЖНО: asset_dict приходит предзаполненным активами из PDQL
    (записи вида {"asset_info": ...}) — функция ДОПОЛНЯЕТ их полями
    policies/names, не затирая (семантика прототипа).

    Чистая часть create_asset_dict (запись строк XLSX осталась в рендере).
    Формат результата идентичен прототипу:
    {asset_id: {"policies": {имя: {"full_info": {номер: count},
                                    "sum_count": N, "satisfaction": "PART"|"YES"}},
                "names": [event_src.host, ...],
                "audit_info": [...]  # только при Audit Events Hack}}
    """
    for policy in policies:
        if policy["name"] == "Audit Events Hack":
            continue
        for host in policy["host_ids"].keys():
            value = {
                "full_info": {str(policy["number"]): policy["host_ids"][host]["count"]},
                "sum_count": policy["host_ids"][host]["count"],
                "satisfaction": "PART",
            }
            if host not in asset_dict:
                asset_dict.update(
                    {
                        host: {
                            "policies": {policy["name"]: value},
                            "names": list(policy["host_ids"][host]["event_src.host"]),
                        }
                    }
                )
            elif "policies" not in asset_dict[host].keys():
                asset_dict[host].update(
                    {
                        "policies": {policy["name"]: value},
                        "names": list(policy["host_ids"][host]["event_src.host"]),
                    }
                )
            else:
                if policy["name"] not in asset_dict[host]["policies"]:
                    asset_dict[host]["policies"].update({policy["name"]: value})
                    for host_name in policy["host_ids"][host]["event_src.host"]:
                        if host_name not in asset_dict[host]["names"]:
                            asset_dict[host]["names"].append(host_name)
                else:
                    asset_dict[host]["policies"][policy["name"]]["full_info"].update(
                        {str(policy["number"]): policy["host_ids"][host]["count"]}
                    )
                    asset_dict[host]["policies"][policy["name"]]["sum_count"] += policy[
                        "host_ids"
                    ][host]["count"]
            if len(
                asset_dict[host]["policies"][policy["name"]]["full_info"].keys()
            ) == len(small_policies[policy["name"]].keys()):
                asset_dict[host]["policies"][policy["name"]]["satisfaction"] = "YES"
            for host_name in policy["host_ids"][host]["event_src.host"]:
                if host_name not in asset_dict[host]["names"]:
                    asset_dict[host]["names"].append(host_name)
    if policies and policies[-1]["name"] == "Audit Events Hack":
        for host in policies[-1]["host_ids"].keys():
            # BUGCOMPAT: прототип падал бы KeyError, если актив виден только
            # в Audit Hack; сохраняем прямое обращение как в оригинале.
            asset_dict[host].update(
                {"audit_info": policies[-1]["host_ids"][host]["event_src.host"]}
            )
    return asset_dict


def classify_policy_events(
    policy_filters_stat: dict[str, int],
) -> Literal["all", "not all", "no"]:
    """Классификация политики по заполненности её фильтров.

    Чистая часть pol_stat_mat: 'all' — события по всем фильтрам политики,
    'not all' — по части, 'no' — ни по одному.
    """
    pol_stat = "no"
    all_pol = True
    for filter_query in policy_filters_stat:
        if policy_filters_stat[filter_query]:
            pol_stat = "all"
        else:
            all_pol = False
    if pol_stat == "all" and not all_pol:
        pol_stat = "not all"
    return pol_stat  # type: ignore[return-value]


def merge_asset_statistics(
    all_assets: dict, asset_id: str, asset_stat: dict, assets_filter: str
) -> None:
    """Слияние статистики актива между фильтрами (перенос asset_analyzer).

    Правила прототипа: 'ok' уступает любому проблемному статусу;
    'no audit' + 'no os events' из разных фильтров комбинируются;
    None-поля дозаполняются; списки политик объединяются без дублей.
    """
    if asset_id not in all_assets.keys():
        all_assets[asset_id] = {
            "STATUS": asset_stat["STATUS"],
            "reports": [assets_filter],
        }
        all_assets[asset_id].update(asset_stat)
    else:
        for stat_field, stat_value in all_assets[asset_id].items():
            if stat_field == "STATUS":
                if stat_value != asset_stat["STATUS"]:
                    if stat_value == "ok":
                        all_assets[asset_id][stat_field] = asset_stat["STATUS"]
                    elif (
                        stat_value == "no audit"
                        and asset_stat["STATUS"] == "no os events"
                    ):
                        all_assets[asset_id][stat_field] = "no audit, no os events"
                    elif (
                        stat_value == "no os events"
                        and asset_stat["STATUS"] == "no audit"
                    ):
                        all_assets[asset_id][stat_field] = "no audit, no os events"
            elif type(stat_value) is not list:
                if (
                    stat_value is None
                    and stat_field in asset_stat.keys()
                    and asset_stat[stat_field]
                ):
                    all_assets[asset_id][stat_field] = asset_stat[stat_field]
            elif stat_field == "reports":
                all_assets[asset_id][stat_field].append(assets_filter)
            elif type(stat_value) is list and asset_stat.get(stat_field):
                for policy in asset_stat[stat_field]:
                    if policy not in stat_value:
                        all_assets[asset_id][stat_field].append(policy)


def flatten_grid_value(value) -> str:
    """Отображаемое значение ячейки PDQL-грида (правила перенесены из
    lib/xlsx_out._asset_info_to_list, инцидент 15.07: веб показывал
    [object Object] там, где Excel — читаемые значения).

    dict: name -> data (data[0] при totalCount==1, иначе str(data)) ->
    displayName -> primaryType -> value; список -> str(список);
    None -> ""; остальное -> как есть строкой.
    """
    if value is None:
        return ""
    if isinstance(value, dict):
        if "name" in value:
            return str(value["name"] or "")
        if "data" in value:
            if value.get("totalCount") == 1:
                return str(value["data"][0])
            return str(value["data"])
        if "displayName" in value:
            return str(value["displayName"] or "")
        if "primaryType" in value:
            return str(value["primaryType"] or "")
        if "value" in value:
            v = value["value"]
            return "" if v is None else str(v)
        return str(value)
    if isinstance(value, list):
        return str(value)
    return str(value)


def status_counter_field(status: str) -> str:
    """Поле WriterStatistic, которое инкрементирует данный STATUS.

    Семантика критериев отчёта (подтверждена оператором, инцидент со
    скриншотом от 06.07.2026):
      «Процент актуального аудита»  = ok + no_os_events
        (аудит актуален, если в статусе НЕТ 'no audit');
      «Хостов с полным покрытием»   = ok + no_audit
        (события полны, если в статусе НЕТ 'no os events').
    Комбинированный 'no audit, no os events' не входит ни в один критерий.

    Исправляет N25: в прототипе ветки сравнивали с 'os events'/'audit'
    и не срабатывали никогда — оба критерия показывали только ok.
    """
    if status == "ok":
        return "ok"
    if status == "no os events":
        return "no_os_events"
    if status == "no audit":
        return "no_audit"
    return "no_audit_no_os_event"


def detect_host_collisions(e_hosts_checker: dict[str, list[str]]) -> list[dict]:
    """N17: коллизии 'один event_src.host у нескольких asset_id'.

    В прототипе результат считался и выбрасывался (закомментированный print);
    теперь — полноценная часть отчёта.
    """
    return [
        {"event_src_host": host, "asset_ids": ids}
        for host, ids in sorted(e_hosts_checker.items())
        if len(ids) > 1
    ]
