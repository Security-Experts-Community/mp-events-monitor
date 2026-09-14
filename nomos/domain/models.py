"""Доменные модели результата аудита (этап 1 рефакторинга).

Контракт между ядром, будущей БД (этап 3), REST API (этап 4) и таблицами
веб-интерфейса (этап 5). XLSX-экспорт — лишь один из потребителей.

Модели выведены из фактических структур прототипа (см. код-ревью, §2.1):
`host_ids` из EventsWorker.take_events, `asset_dict[*]["statistic"]` из
MonitorXlsxWriter, слияние из Nomos.asset_analyzer.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from nomos.compat import StrEnum


class Satisfaction(StrEnum):
    """Насколько политика выполнена по активу (N16: раньше — строки)."""

    YES = "YES"  # события есть по всем фильтрам политики
    PART = "PART"  # события есть не по всем фильтрам
    NONE = "NONE"  # событий нет вовсе


class StatusFlag(StrEnum):
    """Атомарные флаги проблем актива (N18: раньше — подстроки)."""

    NO_AUDIT = "no audit"
    NO_OS_EVENTS = "no os events"
    BAD_PDQL = "not 8"  # исторический маркер прототипа: <9 атрибутов PDQL


class AssetStatus(BaseModel):
    """Статус покрытия актива. Сериализация совместима с прототипом."""

    flags: set[StatusFlag] = Field(default_factory=set)
    empty_policies: list[str] = Field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.flags

    def as_legacy(self) -> str:
        """Строка формата прототипа: 'ok' / 'no audit' /
        'no os events' / 'no audit, no os events' / 'not 8'."""
        if StatusFlag.BAD_PDQL in self.flags:
            return StatusFlag.BAD_PDQL.value
        if not self.flags:
            return "ok"
        ordered = [f for f in (StatusFlag.NO_AUDIT, StatusFlag.NO_OS_EVENTS) if f in self.flags]
        return ", ".join(f.value for f in ordered)

    @classmethod
    def from_legacy(cls, status: str, empty_policies: list[str] | None = None) -> AssetStatus:
        flags: set[StatusFlag] = set()
        if status == StatusFlag.BAD_PDQL.value:
            flags.add(StatusFlag.BAD_PDQL)
        else:
            if StatusFlag.NO_AUDIT.value in status:
                flags.add(StatusFlag.NO_AUDIT)
            if StatusFlag.NO_OS_EVENTS.value in status:
                flags.add(StatusFlag.NO_OS_EVENTS)
        return cls(flags=flags, empty_policies=empty_policies or [])


class PolicyHit(BaseModel):
    """Счётчик событий по активу для одного фильтра политики
    (элемент host_ids из take_events)."""

    count: int
    event_src_hosts: list[str] = Field(default_factory=list)


class PolicyResult(BaseModel):
    """Итог политики по активу (из create_asset_dict)."""

    satisfaction: Satisfaction
    hits: dict[str, int] = Field(
        default_factory=dict,
        description="номер фильтра политики -> COUNT (full_info прототипа)",
    )
    sum_count: int = 0
    expected_filters: int | None = Field(
        default=None, description="сколько под-фильтров у политики всего"
    )
    subfilters: list[str] = Field(
        default_factory=list,
        description="тексты под-фильтров (порядок = № фильтра)",
    )
    packages: dict[str, list[str]] = Field(
        default_factory=dict,
        description="пакет экспертизы -> список правил (как в Excel)",
    )
    effective_window_hours: int | None = Field(
        default=None,
        description="N6: фактическое окно анализа, если ретраи его сжали",
    )

    @property
    def completeness(self) -> float | None:
        """Доля выполненных фильтров, если известно их общее число."""
        return None  # заполняется на уровне отчёта, где известен total


class AssetReport(BaseModel):
    """Актив в отчёте: атрибуты + статус + разбор по политикам."""

    asset_id: str
    attrs: dict[str, Any] = Field(
        default_factory=dict,
        description="атрибуты PDQL по именам (N15: вместо позиций 0..8)",
    )
    status: AssetStatus = Field(default_factory=AssetStatus)
    policies: dict[str, PolicyResult] = Field(default_factory=dict)
    event_src_hosts: list[str] = Field(default_factory=list)
    reports: list[str] = Field(
        default_factory=list, description="в каких asset-фильтрах встретился"
    )


class NoAssetRecord(BaseModel):
    """Строка событий, для которой не нашлось актива."""

    report: str
    attrs: dict[str, Any] = Field(default_factory=dict)


class HostCollision(BaseModel):
    """N17: один event_src.host виден у нескольких asset_id."""

    event_src_host: str
    asset_ids: list[str]


class FilterOutcome(BaseModel):
    """Итог выполнения одного asset-фильтра в прогоне."""

    name: str
    failed: bool = False
    fail_reason: str | None = None
    assets: int = 0
    no_assets: int = 0
    degraded_windows_hours: dict[str, int] = Field(default_factory=dict)


class UnifiedReport(BaseModel):
    """Объединённый результат прогона — будущий payload REST API (этап 4)
    и источник таблиц веб-интерфейса (этап 5)."""

    host: str
    time_delta_hours: int
    run_id: str | None = None
    assets: dict[str, AssetReport] = Field(default_factory=dict)
    no_assets: list[NoAssetRecord] = Field(default_factory=list)
    bad_assets: dict[str, Any] = Field(default_factory=dict)
    collisions: list[HostCollision] = Field(default_factory=list)
    filters: list[FilterOutcome] = Field(default_factory=list)
