"""Доменный слой Nomos: модели результата и логика анализа покрытия.

Появился на этапе 1 рефакторинга: бизнес-логика вынесена из модуля
генерации XLSX (lib/xlsx_out.py) и оркестратора (Nomos.py), рендер Excel
стал одним из потребителей. См. docstrings analysis.py про BUGCOMPAT.
"""

from nomos.domain.analysis import (
    accumulate_policy_hits,
    check_edr,
    classify_policy_events,
    detect_host_collisions,
    flatten_grid_value,
    merge_asset_statistics,
    status_master,
)
from nomos.domain.models import (
    AssetReport,
    AssetStatus,
    FilterOutcome,
    HostCollision,
    NoAssetRecord,
    PolicyHit,
    PolicyResult,
    Satisfaction,
    StatusFlag,
    UnifiedReport,
)
from nomos.domain.report import build_unified_report, dump_unified_report

__all__ = [
    "status_master", "check_edr", "accumulate_policy_hits",
    "classify_policy_events", "merge_asset_statistics", "detect_host_collisions",
    "flatten_grid_value",
    "AssetReport", "AssetStatus", "PolicyHit", "PolicyResult", "Satisfaction",
    "StatusFlag", "NoAssetRecord", "HostCollision", "FilterOutcome",
    "UnifiedReport", "build_unified_report", "dump_unified_report",
]
