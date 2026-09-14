"""Автозавершение прогона: эталон, сверка, диагностический бандл.

Вызывается из ``Nomos.py`` в блоке finally — то есть отрабатывает и при
успехе, и при падении прогона (при падении бандл нужнее всего).

Поведение при ``debug_dump=true`` или ``record_fixtures=true``:

1. Прогон успешен и ``golden/`` НЕ существует и record_fixtures=true
   → снять эталон (первый, «золотой» прогон).
2. Прогон успешен и ``golden/`` существует
   → автоматически сверить результат с эталоном, вердикт в лог.
3. Всегда (успех или падение) → собрать nomos_debug_{run_id}.zip.

Итого оператору нужна одна команда:
    python Nomos.py record_fixtures=true debug_dump=true
"""

from __future__ import annotations

import logging
from pathlib import Path

from nomos.debug_bundle import make_debug_bundle
from nomos.golden import capture_golden, compare_golden

logger = logging.getLogger("nomos.autofinish")

# Контекст текущего прогона: ставится из init_debug_harness сразу после
# загрузки Settings — ДО первого запроса к MaxPatrol. Благодаря этому
# finalize_last_run() может собрать бандл, даже если прогон упал ещё
# в конструкторе (аутентификация, проверка политик).
_context: dict = {"settings": None, "run_id": None, "finalized": False}


def set_run_context(settings, run_id: str) -> None:
    _context.update({"settings": settings, "run_id": run_id, "finalized": False})


def finalize_last_run(failed: bool = False) -> None:
    """Идемпотентное автозавершение по сохранённому контексту.

    No-op, если обвязка не успела стартовать (упали на валидации Settings —
    журналов ещё нет, собирать нечего) или finalize уже выполнялся.
    """
    if _context["finalized"] or _context["run_id"] is None:
        return
    _context["finalized"] = True
    finalize_run(_context["settings"], _context["run_id"], failed=failed)


def finalize_run(settings, run_id: str, failed: bool = False) -> None:
    """Пост-обработка прогона. Никогда не роняет процесс сама."""
    debug_dump = bool(getattr(settings, "debug_dump", False))
    record_fixtures = bool(getattr(settings, "record_fixtures", False))
    if not (debug_dump or record_fixtures):
        return

    out_dir = Path(getattr(settings, "out_folder", Path("out")))
    golden_dir = Path("golden")

    if failed:
        logger.warning("прогон завершился ошибкой — эталон не трогаю, собираю бандл")
    else:
        try:
            if golden_dir.is_dir():
                compare_golden(out_dir=out_dir, golden_dir=golden_dir)
            elif record_fixtures:
                capture_golden(out_dir=out_dir, golden_dir=golden_dir)
            else:
                logger.info(
                    "эталона нет; для его съёмки запустите прогон с record_fixtures=true"
                )
        except Exception:
            logger.exception("эталон: ошибка снятия/сверки (прогон не затронут)")

    try:
        bundle = make_debug_bundle(run_id=run_id, out_dir=out_dir)
        print(f"\nДиагностический бандл готов: {bundle.resolve()}")
        print("Секреты замаскированы — файл можно передавать.")
        if record_fixtures:
            fixtures = Path("fixtures") / run_id
            if fixtures.is_dir():
                print(f"Фикстуры для offline-регрессии: {fixtures.resolve()} (передать отдельно)")
    except Exception:
        logger.exception("не удалось собрать диагностический бандл")
