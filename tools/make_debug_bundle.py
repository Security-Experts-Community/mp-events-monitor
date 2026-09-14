#!/usr/bin/env python3
"""Собрать диагностический бандл после ручного прогона.

Использование (из корня проекта):
    python tools/make_debug_bundle.py            # последний прогон
    python tools/make_debug_bundle.py <run_id>   # конкретный прогон

Полученный nomos_debug_<run_id>.zip передаётся для разбора.
Все секреты в архиве замаскированы.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from envcheck import configure_console  # noqa: E402
from nomos.debug_bundle import make_debug_bundle  # noqa: E402
from nomos.runlog import init_run_logging  # noqa: E402

configure_console()

if __name__ == "__main__":
    init_run_logging(level="INFO", run_id="bundle-tool")
    run_id = sys.argv[1] if len(sys.argv) > 1 else None
    path = make_debug_bundle(run_id=run_id)
    print(f"\nГотово: {path.resolve()}\nЭтот файл можно передавать — секреты замаскированы.")
