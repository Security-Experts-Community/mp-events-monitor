"""План предстоящих запросов прогона (отладочный флаг ``dump_queries``).

Запрос оператора: до опроса SIEM видеть, что именно будет спрошено —
какой asset-фильтр берётся, каким PDQL выбираются активы, какие политики
событий к нему применимы и какими текстами запросов они проверяются.

План считается ТОЙ ЖЕ функцией отбора, что и боевой прогон
(``EventPolicies.filter_policies``), но на отдельном экземпляре
``EventPolicies`` — общий объект прогона не трогаем, иначе план подменил
бы политики выполняющемуся фильтру.

Запись фильтра делается в два приёма: план (до опроса) и результат
выборки активов — список UUID, которые вернул PDQL. На втором шаге
тексты запросов достраиваются до исполнимых: в filter(...) добавляется
``in_list([...], event_src.asset)`` с фактическими активами, так что
запрос можно скопировать в интерфейс MaxPatrol как есть.

Вывод двойной: строки в журнал (видно в консоли и в бандле) и файл
``out/query_fixtures.json``, который дописывается ПО МЕРЕ прохождения
фильтров — прогон длинный, план по уже начатым фильтрам должен быть
доступен сразу, а не в конце.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from nomos.compat import UTC

logger = logging.getLogger("nomos.queryplan")

PLAN_FILE_NAME = "query_fixtures.json"
_LOG_IDS_LIMIT = 50  # в журнал — первые N UUID, полный список в файле

_lock = threading.Lock()
_plans: dict[str, dict] = {}  # ключ — путь к файлу плана

# Синтетическая политика прототипа при audit_hack (lib/events.py)
_AUDIT_HACK_POLICY = {
    "name": "Audit Events Hack",
    "number": 0,
    "filter": 'id = "PT_Positive_Technologies_MaxPatrol_customevent_collector_job_start"',
    "full_filter": (
        'filter(id = "PT_Positive_Technologies_MaxPatrol_customevent_collector_job_start") | '
        "select(time, event_src.host, dst.host, dst.asset, object.name) | sort(time desc) | "
        "group(key: [dst.asset, object.name], agg: COUNT(*) as Cnt) | sort(Cnt desc) "
        "| limit(100000)"
    ),
}
# Прототип ищет Audit Hack по dst.asset, остальные политики — по event_src.asset
_ASSET_FIELDS = {_AUDIT_HACK_POLICY["name"]: "dst.asset"}
_DEFAULT_ASSET_FIELD = "event_src.asset"


def pdql_text(pdql: Any) -> str:
    """PDQL фильтра как строка (в конфиге допустим список строк)."""
    if isinstance(pdql, list):
        return "".join(str(part) for part in pdql)
    return "" if pdql is None else str(pdql)


def build_filter_plan(
    settings,
    filter_name: str,
    pdql: Any,
    blacklist=None,
    whitelist=None,
    specific=None,
    mandatory=None,
    group=None,
    comment=None,
    audit_hack: bool = False,
) -> dict:
    """Считает, что будет спрошено по одному asset-фильтру."""
    from lib.policies_checker import EventPolicies

    policies = EventPolicies(Path(settings.event_policies_file), logger)
    policies.filter_policies(blacklist, whitelist, specific, mandatory)

    queries_by_policy: dict[str, list[dict]] = {}
    for query in policies.rebuilt_policies:
        queries_by_policy.setdefault(query["name"], []).append(
            {
                "number": query["number"],
                "filter": query["filter"],
                "full_filter": query["full_filter"],
            }
        )
    if audit_hack:
        queries_by_policy.setdefault(_AUDIT_HACK_POLICY["name"], []).append(
            {
                "number": _AUDIT_HACK_POLICY["number"],
                "filter": _AUDIT_HACK_POLICY["filter"],
                "full_filter": _AUDIT_HACK_POLICY["full_filter"],
            }
        )

    mandatory_names = set(policies.mandatory_policies or [])
    # Пакеты экспертизы намеренно не попадают в план: к предстоящим
    # запросам они отношения не имеют, а читать мешают (репорт 10.09).
    planned_policies = [
        {
            "name": name,
            "mandatory": name in mandatory_names,
            "queries": sorted(queries, key=lambda q: q["number"]),
        }
        for name, queries in sorted(queries_by_policy.items())
    ]
    return {
        "filter": filter_name,
        "planned_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "pdql": pdql_text(pdql),
        "group": group,
        "comment": comment,
        "time_delta_hours": getattr(settings, "time_delta_hours", None),
        "policies_total": len(planned_policies),
        "queries_total": sum(len(p["queries"]) for p in planned_policies),
        "policies": planned_policies,
    }


def log_filter_plan(plan: dict, log: logging.Logger | None = None) -> None:
    """Печатает план в журнал (консоль + nomos.log + JSONL бандла)."""
    log = log or logger
    log.info(
        f"[план] буду выполнять фильтр '{plan['filter']}': "
        f"политик {plan['policies_total']}, запросов к событиям "
        f"{plan['queries_total']}, окно {plan['time_delta_hours']} ч"
    )
    log.info(f"[план] PDQL выборки активов: {plan['pdql']}")
    for policy in plan["policies"]:
        mark = " (обязательная)" if policy["mandatory"] else ""
        log.info(f"[план]   политика '{policy['name']}'{mark}:")
        for query in policy["queries"]:
            log.info(f"[план]     №{query['number'] + 1} {query['filter']}")


def record_filter_plan(settings, plan: dict, log: logging.Logger | None = None) -> Path:
    """Дописывает план фильтра в out/query_fixtures.json и логирует его.

    Файл перезаписывается целиком после каждого фильтра (через временный
    файл): прогон может быть прерван в любой момент, а уже накопленный
    план должен остаться валидным JSON.
    """
    log_filter_plan(plan, log)
    out_folder = Path(getattr(settings, "out_folder", Path("out")))
    path = out_folder / PLAN_FILE_NAME
    key = str(path)
    with _lock:
        document = _document(key, path, settings)
        previous = _pop_entry(document, plan["filter"])
        if previous:
            # Повторный план того же фильтра не должен терять уже
            # записанный результат выборки активов
            for field in ("assets_total", "asset_ids", "no_asset_rows",
                          "assets_at", "batch_size"):
                if field in previous:
                    plan[field] = previous[field]
            if previous.get("asset_ids"):
                _apply_assets_to_entry(plan, previous["asset_ids"])
        document["filters"].append(plan)
        _write_atomic(path, document)
    log.info(f"[план] дописан в {path}") if log else None
    return path


def plan_asset_worker(worker, log: logging.Logger | None = None) -> Path:
    """Точка врезки в прогон: план по уже подготовленному AssetWorker.

    Берём нормализованные им поля (mandatory-политики к этому моменту уже
    добавлены в blacklist), поэтому план совпадает с тем, что построит
    EventsWorker этого фильтра.
    """
    settings = worker.settings
    plan = build_filter_plan(
        settings,
        filter_name=worker.filter_name,
        pdql=worker.pdql,
        blacklist=worker.default_politics_blacklist,
        whitelist=worker.default_politics_whitelist,
        specific=worker.specific_politics,
        mandatory=worker.mandatory_policies,
        group=_jsonable(getattr(worker, "group", None)),
        comment=_jsonable(getattr(worker, "comment", None)),
        audit_hack=bool(getattr(settings, "audit_hack", False))
        and not bool(getattr(settings, "dl_mode", False)),
    )
    return record_filter_plan(settings, plan, log or getattr(worker, "logger", None))


def asset_list_literal(asset_ids: list[str]) -> str:
    """Список активов как литерал PDQL: значения экранируются как в JSON."""
    return "[" + ",".join(json.dumps(str(a), ensure_ascii=False) for a in asset_ids) + "]"


def with_assets(full_filter: str, asset_ids: list[str], field: str) -> str:
    """Добавляет в filter(...) проверку принадлежности активу.

    Условие дописывается В КОНЕЦ выражения первого filter(...):
        filter(<условие> and in_list([...], event_src.asset)) | select(...)
    Скобки считаются, поэтому вложенные filter(...)/функции не ломаются.
    """
    if not full_filter or not asset_ids:
        return full_filter
    prefix = "filter("
    if not full_filter.startswith(prefix):
        return full_filter
    depth, close = 1, None
    for position in range(len(prefix), len(full_filter)):
        char = full_filter[position]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                close = position
                break
    if close is None:
        return full_filter
    condition = full_filter[len(prefix) : close]
    check = f"in_list({asset_list_literal(asset_ids)}, {field})"
    joined = f"{condition} and {check}" if condition.strip() else check
    return f"{prefix}{joined}{full_filter[close:]}"


def _apply_assets_to_entry(entry: dict, asset_ids: list[str]) -> None:
    """Проставляет активы во все запросы записи фильтра."""
    for policy in entry.get("policies") or []:
        field = _ASSET_FIELDS.get(policy.get("name"), _DEFAULT_ASSET_FIELD)
        for query in policy.get("queries") or []:
            base = query.get("base_filter") or query.get("full_filter")
            if not base:
                continue
            query["base_filter"] = base
            query["full_filter"] = with_assets(base, asset_ids, field)


def update_filter_assets(
    settings,
    filter_name: str,
    asset_ids: list[str],
    no_asset_rows: int | None = None,
    log: logging.Logger | None = None,
) -> Path:
    """Дописывает в запись фильтра UUID активов, которые вернул PDQL."""
    log = log or logger
    out_folder = Path(getattr(settings, "out_folder", Path("out")))
    path = out_folder / PLAN_FILE_NAME
    key = str(path)
    with _lock:
        document = _document(key, path, settings)
        entry = _pop_entry(document, filter_name) or {
            "filter": filter_name,
            "planned_at": None,
        }
        entry["assets_at"] = datetime.now(UTC).isoformat(timespec="seconds")
        entry["assets_total"] = len(asset_ids)
        entry["asset_ids"] = asset_ids
        if no_asset_rows is not None:
            entry["no_asset_rows"] = no_asset_rows
        batch_size = getattr(settings, "max_uuids_in_siem_query", None)
        if batch_size:
            entry["batch_size"] = batch_size
        # Тексты запросов становятся исполнимыми: с фактическими активами
        _apply_assets_to_entry(entry, asset_ids)
        document["filters"].append(entry)
        _write_atomic(path, document)
    shown = ", ".join(asset_ids[:_LOG_IDS_LIMIT])
    tail = (
        f" …и ещё {len(asset_ids) - _LOG_IDS_LIMIT} (полный список в {PLAN_FILE_NAME})"
        if len(asset_ids) > _LOG_IDS_LIMIT
        else ""
    )
    log.info(
        f"[план] фильтр '{filter_name}': PDQL вернул активов "
        f"{len(asset_ids)}: {shown}{tail}"
        if asset_ids
        else f"[план] фильтр '{filter_name}': PDQL не вернул ни одного актива"
    )
    batch_size = getattr(settings, "max_uuids_in_siem_query", None)
    if batch_size and len(asset_ids) > batch_size:
        batches = -(-len(asset_ids) // batch_size)
        log.info(
            f"[план] в прогоне активы уйдут пачками по {batch_size} "
            f"({batches} запроса(ов) на каждый под-фильтр); в плане запрос "
            f"приведён со всей выборкой сразу"
        )
    return path


def record_filter_assets(worker, log: logging.Logger | None = None) -> Path:
    """Точка врезки: результат выборки активов уже отработавшего фильтра."""
    asset_ids = [str(a) for a in (getattr(worker, "_asset_ids", None) or [])]
    return update_filter_assets(
        worker.settings,
        worker.filter_name,
        asset_ids,
        getattr(worker, "_no_asset_rows", None),
        log or getattr(worker, "logger", None),
    )


def reset_plan_cache() -> None:
    """Сброс накопителя (новый прогон в том же процессе — веб-режим)."""
    with _lock:
        _plans.clear()


def _document(key: str, path: Path, settings) -> dict:
    """Накопленный документ плана: из памяти, с диска или новый."""
    document = _plans.get(key)
    if document is None:
        document = _load_existing(path) or {
            "generated": datetime.now(UTC).isoformat(timespec="seconds"),
            "host": getattr(settings, "mpx_host", None),
            "mode": getattr(settings, "mode", None),
            "time_delta_hours": getattr(settings, "time_delta_hours", None),
            "event_policies_file": str(getattr(settings, "event_policies_file", "")),
            "filters": [],
        }
        _plans[key] = document
    return document


def _pop_entry(document: dict, filter_name: str) -> dict | None:
    """Изымает запись фильтра из документа (порядок — по времени записи)."""
    found = None
    kept = []
    for item in document["filters"]:
        if item.get("filter") == filter_name and found is None:
            found = item
        else:
            kept.append(item)
    document["filters"] = kept
    return found


def _jsonable(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return str(value)


def _load_existing(path: Path) -> dict | None:
    """Продолжение прерванного прогона в ту же папку out/."""
    try:
        with path.open("r", encoding="utf-8") as f:
            document = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if isinstance(document, dict) and isinstance(document.get("filters"), list):
        return document
    return None


def _write_atomic(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(document, f, ensure_ascii=False, indent=2)
    tmp.replace(path)
