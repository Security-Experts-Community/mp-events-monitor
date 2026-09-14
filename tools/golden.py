#!/usr/bin/env python3
"""CLI золотого эталона (логика — в nomos/golden.py).

При прогонах с debug_dump=true / record_fixtures=true эталон снимается
и сверяется АВТОМАТИЧЕСКИ — этот инструмент нужен только для ручных
операций:

    python tools/golden.py capture             # снять (если golden/ нет)
    python tools/golden.py capture --force     # пересъёмка
    python tools/golden.py compare             # сверить out/ с эталоном
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from envcheck import configure_console  # noqa: E402
from nomos.golden import capture_golden, compare_golden  # noqa: E402
from nomos.runlog import init_run_logging  # noqa: E402

configure_console()

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] not in ("capture", "compare"):
        sys.exit(__doc__)
    init_run_logging(level="INFO", run_id="golden-tool")
    if args[0] == "capture":
        capture_golden(overwrite="--force" in args)
    else:
        sys.exit(1 if compare_golden() else 0)
