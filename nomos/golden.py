"""Золотой эталон: снятие снимка результатов и сравнение с ним.

Снимок кладётся в ``golden/`` (в git не попадает — данные стенда).
Сравниваются НОРМАЛИЗОВАННЫЕ данные: ключи и списки сортируются,
метки времени и счётчики событий (COUNT меняется между прогонами)
исключаются. Цель — подтверждать, что рефакторинг не изменил
СОСТАВ активов и их СТАТУСЫ.

Вызывается автоматически в конце прогона (см. ``nomos.autofinish``)
или вручную через ``tools/golden.py``.
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

logger = logging.getLogger("nomos.golden")

# Поля, меняющиеся от прогона к прогону — из сравнения исключаем.
VOLATILE_KEYS = {
    "COUNT",
    "time",
    "dur_audit",
    "host.@audittime",
    "@audittime",
    "run_id",
    "elapsed",
    "degraded_windows_hours",  # N6: зависит от здоровья стенда в момент прогона
}


def _normalize(node):
    if isinstance(node, dict):
        return {
            key: _normalize(value)
            for key, value in sorted(node.items())
            if key not in VOLATILE_KEYS
        }
    if isinstance(node, list):
        normalized = [_normalize(item) for item in node]
        try:
            return sorted(normalized, key=_sort_key)
        except TypeError:
            return normalized
    return node


def _sort_key(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _iter_result_files(base: Path):
    yield from sorted(base.rglob("!asset_dict.json"))
    yield from sorted(base.rglob("!take_no_asset_ids.json"))
    yield from sorted(base.rglob("AssetWorker_stat.json"))


def capture_golden(
    out_dir: Path | str = Path("out"),
    golden_dir: Path | str = Path("golden"),
    overwrite: bool = False,
) -> int:
    """Снимает эталон из out_dir. Возвращает число файлов снимка.

    По умолчанию НЕ перезаписывает существующий эталон (защита от
    случайной подмены базы сравнения повторным прогоном с
    record_fixtures=true) — для пересъёмки удалите golden/ или
    вызовите с overwrite=True.
    """
    out_dir, golden_dir = Path(out_dir), Path(golden_dir)
    if not out_dir.is_dir():
        raise FileNotFoundError(f"{out_dir} не найден — нечего снимать")
    if golden_dir.exists():
        if not overwrite:
            logger.warning(
                f"эталон {golden_dir.resolve()} уже существует — пропускаю съёмку. "
                f"Для пересъёмки удалите папку golden/"
            )
            return 0
        shutil.rmtree(golden_dir)
    count = 0
    for src in _iter_result_files(out_dir):
        rel = src.relative_to(out_dir)
        dst = golden_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        data = json.loads(src.read_text(encoding="utf-8"))
        dst.write_text(
            json.dumps(_normalize(data), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        count += 1
    logger.info(f"эталон снят: {count} файлов в {golden_dir.resolve()}")
    return count


def _statuses(asset_dict: dict) -> dict[str, str]:
    return {
        asset_id: (info.get("statistic") or {}).get("STATUS", "<нет>")
        for asset_id, info in asset_dict.items()
    }


def compare_golden(
    out_dir: Path | str = Path("out"),
    golden_dir: Path | str = Path("golden"),
) -> int:
    """Сравнивает out_dir с эталоном. Возвращает число расхождений.

    Каждое расхождение логируется (уровень WARNING) — попадает в
    nomos.log и JSONL, то есть в диагностический бандл.
    """
    out_dir, golden_dir = Path(out_dir), Path(golden_dir)
    if not golden_dir.is_dir():
        raise FileNotFoundError(f"{golden_dir} не найден — сначала снимите эталон")
    problems = 0
    for golden_file in _iter_result_files(golden_dir):
        rel = golden_file.relative_to(golden_dir)
        current_file = out_dir / rel
        if not current_file.exists():
            logger.warning(f"[НЕТ ФАЙЛА] {rel}")
            problems += 1
            continue
        golden = json.loads(golden_file.read_text(encoding="utf-8"))
        current = _normalize(json.loads(current_file.read_text(encoding="utf-8")))
        if golden == current:
            logger.info(f"[OK]        {rel}")
            continue
        problems += 1
        logger.warning(f"[РАСХОЖД.]  {rel}")
        if golden_file.name == "!asset_dict.json":
            g_status, c_status = _statuses(golden), _statuses(current)
            only_golden = sorted(set(g_status) - set(c_status))
            only_current = sorted(set(c_status) - set(g_status))
            changed = [
                (aid, g_status[aid], c_status[aid])
                for aid in g_status
                if aid in c_status and g_status[aid] != c_status[aid]
            ]
            if only_golden:
                logger.warning(
                    f"    активы пропали ({len(only_golden)}): {only_golden[:3]}..."
                )
            if only_current:
                logger.warning(
                    f"    активы добавились ({len(only_current)}): {only_current[:3]}..."
                )
            for aid, old, new in changed[:10]:
                logger.warning(f"    STATUS {aid}: '{old}' -> '{new}'")
            if len(changed) > 10:
                logger.warning(f"    ... и ещё {len(changed) - 10} смен статуса")
    if problems == 0:
        logger.info("сверка с эталоном: СОВПАЛО")
    else:
        logger.warning(f"сверка с эталоном: {problems} расхождений")
    return problems
