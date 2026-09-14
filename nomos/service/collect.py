"""Сборка объединённого результата из пофильтровых файлов out/.

Вынесено из Nomos.unified_report (этап 2), чтобы один и тот же код
собирал и финальный отчёт, и ПРОМЕЖУТОЧНЫЕ срезы по мере выполнения
фильтров (репорт оператора: «сводный отчёт должен наполняться данными
по мере выполнения, а не только в конце»). Функция только читает файлы
и строит модель — ни XLSX, ни статистику прогона не трогает.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from nomos.domain.analysis import merge_asset_statistics
from nomos.domain.models import UnifiedReport
from nomos.domain.report import build_unified_report

logger = logging.getLogger("nomos.service.collect")


def filter_folder_name(assets_filter: str) -> str:
    """Правило прототипа: имя фильтра -> имя папки в out/."""
    return re.sub("[^a-zA-Zа-яА-я_ 0-9-]", "_", assets_filter)


def collect_unified(
    out_folder: Path,
    assets_filters: list[str],
    bad_assets: dict,
    filters_statistic: dict | None,
    host: str,
    time_delta_hours: int,
    run_id: str | None = None,
) -> tuple[dict, list, UnifiedReport]:
    """Читает результаты перечисленных фильтров и строит UnifiedReport.

    Возвращает (all_assets, all_no_asset, unified) — первые два нужны
    XLSX-рендеру финального отчёта, модель — БД и веб-интерфейсу.
    Фильтры, чьи файлы ещё не готовы, молча пропускаются — это штатно
    для промежуточных срезов.
    """
    all_assets: dict = {}
    e_hosts_checker: dict = {}
    all_no_asset: list = []
    rich_policies: dict = {}
    checked_policies_by_filter: dict = {}
    policies_meta_by_filter: dict = {}

    for assets_filter in assets_filters:
        if assets_filter == "comments":
            continue
        folder = Path(out_folder) / filter_folder_name(assets_filter)
        asset_dict = _load_json(folder / "!asset_dict.json") or {}
        aw_stat = _load_json(folder / "AssetWorker_stat.json") or {}
        if aw_stat.get("checked_policies"):
            checked_policies_by_filter[assets_filter] = aw_stat["checked_policies"]
        # Тексты под-фильтров и правила по пакетам (для карточки актива —
        # полное соответствие Excel, инцидент 15.07)
        policies_meta = _load_json(folder / "!policies_meta.json") or {}
        if policies_meta:
            policies_meta_by_filter[assets_filter] = policies_meta
        for asset_id, asset_info in asset_dict.items():
            statistic = asset_info.get("statistic") or {}
            if statistic.get("event_src.host"):
                for e_host in statistic["event_src.host"].split(" / "):
                    ids = e_hosts_checker.setdefault(e_host, [])
                    if asset_id not in ids:
                        ids.append(asset_id)
            for pol_name, pol in (asset_info.get("policies") or {}).items():
                rich_pol = rich_policies.setdefault(asset_id, {}).setdefault(
                    pol_name,
                    {"hits": {}, "satisfaction": pol.get("satisfaction")},
                )
                for num, count in (pol.get("full_info") or {}).items():
                    # актив в нескольких asset-фильтрах: max, а не сумма
                    if count > rich_pol["hits"].get(num, -1):
                        rich_pol["hits"][num] = count
                if pol.get("satisfaction") == "YES":
                    rich_pol["satisfaction"] = "YES"
            merge_asset_statistics(all_assets, asset_id, statistic, assets_filter)
        no_asset_list = _load_json(folder / "!take_no_asset_ids.json") or []
        for no_asset in no_asset_list:
            all_no_asset.append({"report": assets_filter, **no_asset})

    unified = build_unified_report(
        host=host,
        time_delta_hours=time_delta_hours,
        all_assets=all_assets,
        all_no_asset=all_no_asset,
        bad_assets=bad_assets,
        e_hosts_checker=e_hosts_checker,
        filters_statistic=filters_statistic,
        run_id=run_id,
        rich_policies=rich_policies,
        checked_policies_by_filter=checked_policies_by_filter,
        policies_meta_by_filter=policies_meta_by_filter,
    )
    return all_assets, all_no_asset, unified


def _load_json(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        logger.warning(f"Can't open {path.absolute()}")
        return None
