"""Сборка UnifiedReport из структур прототипа (этап 1, шаг 1.3 частично).

Пока legacy-конвейер продолжает оперировать словарями (ради эквивалентности
эталону), этот модуль строит из них типизированный отчёт — будущий payload
REST API и источник веб-таблиц. Итог пишется в out/unified_report.json.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from nomos.domain.analysis import detect_host_collisions, flatten_grid_value
from nomos.domain.models import (
    AssetReport,
    AssetStatus,
    FilterOutcome,
    HostCollision,
    NoAssetRecord,
    PolicyResult,
    Satisfaction,
    UnifiedReport,
)

logger = logging.getLogger("nomos.domain.report")

# Служебные поля statistic, не являющиеся PDQL-атрибутами актива
_NON_ATTR_KEYS = {"STATUS", "reports", "empty policies"}
_POLICY_LIST_KEYS = ("полные политики", "частичные политики")


def asset_report_from_legacy(asset_id: str, stat: dict[str, Any]) -> AssetReport:
    """AssetReport из строки объединённого all_assets прототипа."""
    attrs: dict[str, Any] = {}
    # Реальные имена ключей прототипа (по файлам стенда 14.07):
    # good_policy — полные, "not all_policy" — частичные.
    full_pols: list[str] = list(stat.get("good_policy") or [])
    part_pols: list[str] = list(stat.get("not all_policy") or [])
    _known_list_keys = {"good_policy", "not all_policy"}
    for key, value in stat.items():
        if key in _NON_ATTR_KEYS or key in _known_list_keys:
            continue
        if isinstance(value, list) and key not in ("reports",):
            # позиционный фолбэк для нестандартных attrs_simple
            if not full_pols:
                full_pols = value
            elif not part_pols:
                part_pols = value
            continue
        attrs[key] = value
    policies: dict[str, PolicyResult] = {}
    for name in full_pols:
        policies[name] = PolicyResult(satisfaction=Satisfaction.YES)
    for entry in part_pols:
        # формат прототипа: 'имя политики: N из M'
        name = entry.split(":")[0].strip() if ":" in entry else entry
        policies.setdefault(name, PolicyResult(satisfaction=Satisfaction.PART))
    hosts = stat.get("event_src.host")
    return AssetReport(
        asset_id=asset_id,
        attrs=attrs,
        status=AssetStatus.from_legacy(
            stat.get("STATUS") or "", stat.get("empty policies") or []
        ),
        policies=policies,
        event_src_hosts=hosts.split(" / ") if isinstance(hosts, str) else [],
        reports=list(stat.get("reports") or []),
    )


def _enrich_asset_policies(
    asset: AssetReport,
    rich: dict | None,
    checked_by_filter: dict[str, dict],
    meta_by_filter: dict[str, dict] | None = None,
) -> None:
    """Обогащение политик актива данными, которые терялись при слиянии:

    1. hits по под-фильтрам (COUNT из !asset_dict, включая частичные);
    2. expected_filters и пакеты экспертизы (мета checked_policies);
    3. НЕВЫПОЛНЕННЫЕ политики (satisfaction=NONE): проверялись по
       asset-фильтрам актива, но событий не дали — раньше их не было
       видно вовсе (репорт оператора от 14.07: «не вижу невыполненные,
       данные по фильтрам и связанные пакеты»).
    """
    # Мета: объединение по всем asset-фильтрам актива.
    # Из checked_policies берём число под-фильтров; из !policies_meta —
    # тексты под-фильтров и пакеты->правила (для полного соответствия Excel).
    meta_by_filter = meta_by_filter or {}
    checked: dict[str, dict] = {}
    for report_name in asset.reports:
        for pol_name, meta in (checked_by_filter.get(report_name) or {}).items():
            known = checked.setdefault(
                pol_name,
                {"filters_total": 0, "subfilters": [], "packages": {}},
            )
            known["filters_total"] = max(
                known["filters_total"], int(meta.get("filters_total") or 0)
            )
        for pol_name, rich_meta in (meta_by_filter.get(report_name) or {}).items():
            known = checked.setdefault(
                pol_name,
                {"filters_total": 0, "subfilters": [], "packages": {}},
            )
            subfilters = rich_meta.get("filters") or []
            if len(subfilters) > len(known["subfilters"]):
                known["subfilters"] = list(subfilters)
            known["filters_total"] = max(known["filters_total"], len(subfilters))
            for pkg, rules in (rich_meta.get("packages") or {}).items():
                existing = known["packages"].setdefault(pkg, [])
                for rule in rules:
                    if rule not in existing:
                        existing.append(rule)

    # Обогащение выполненных/частичных из rich (hits по под-фильтрам)
    for pol_name, rich_pol in (rich or {}).items():
        hits = {str(k): int(v) for k, v in (rich_pol.get("hits") or {}).items()}
        result = asset.policies.get(pol_name)
        if result is None:
            satisfaction = (
                Satisfaction.YES
                if rich_pol.get("satisfaction") == "YES"
                else Satisfaction.PART
            )
            result = PolicyResult(satisfaction=satisfaction)
            asset.policies[pol_name] = result
        result.hits = hits
        result.sum_count = sum(hits.values())

    # Мета + невыполненные
    for pol_name, meta in checked.items():
        result = asset.policies.get(pol_name)
        if result is None:
            result = PolicyResult(satisfaction=Satisfaction.NONE)
            asset.policies[pol_name] = result
        result.expected_filters = meta["filters_total"] or None
        result.subfilters = meta["subfilters"]
        result.packages = {k: meta["packages"][k] for k in sorted(meta["packages"])}


def build_unified_report(
    host: str,
    time_delta_hours: int,
    all_assets: dict[str, dict],
    all_no_asset: list[dict],
    bad_assets: dict,
    e_hosts_checker: dict[str, list[str]],
    filters_statistic: dict[str, dict] | None = None,
    run_id: str | None = None,
    rich_policies: dict[str, dict] | None = None,
    checked_policies_by_filter: dict[str, dict] | None = None,
    policies_meta_by_filter: dict[str, dict] | None = None,
) -> UnifiedReport:
    report = UnifiedReport(
        host=host, time_delta_hours=time_delta_hours, run_id=run_id
    )
    rich_policies = rich_policies or {}
    checked_policies_by_filter = checked_policies_by_filter or {}
    policies_meta_by_filter = policies_meta_by_filter or {}
    for asset_id, stat in all_assets.items():
        asset = asset_report_from_legacy(asset_id, stat)
        _enrich_asset_policies(
            asset, rich_policies.get(asset_id), checked_policies_by_filter,
            policies_meta_by_filter,
        )
        report.assets[asset_id] = asset
    for record in all_no_asset:
        rec = dict(record)
        report_name = rec.pop("report", "")
        # Значения PDQL-грида сплющиваются в отображаемые строки —
        # как это делает Excel (_asset_info_to_list); служебные ключи вон.
        attrs = {
            key: flatten_grid_value(value)
            for key, value in rec.items()
            if key not in ("$assetGridGroupKey", "asset_info_is_answer_again")
        }
        report.no_assets.append(NoAssetRecord(report=report_name, attrs=attrs))
    report.bad_assets = dict(bad_assets or {})
    report.collisions = [
        HostCollision(**c) for c in detect_host_collisions(e_hosts_checker)
    ]
    for name, fstat in (filters_statistic or {}).items():
        report.filters.append(
            FilterOutcome(
                name=name,
                failed="FAILED" in fstat,
                fail_reason=fstat.get("FAILED"),
                assets=int(fstat.get("asset") or 0),
                no_assets=int(fstat.get("no_asset") or 0),
                degraded_windows_hours=dict(
                    fstat.get("degraded_windows_hours") or {}
                ),
            )
        )
    return report


def dump_unified_report(report: UnifiedReport, out_folder: Path) -> Path:
    path = Path(out_folder) / "unified_report.json"
    path.write_text(
        json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"машиночитаемый отчёт: {path.resolve()}")
    return path
