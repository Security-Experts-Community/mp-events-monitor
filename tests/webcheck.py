"""Признак наличия веб-интерфейса в сборке.

Проект поставляется в двух видах: полный (веб + CLI) и CLI-only — для
обратной совместимости и стендов, где веб не поможет. Тесты, которые
проверяют веб, в CLI-сборке пропускаются, а не падают.
"""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

WEB_PRESENT = (ROOT / "nomos" / "web" / "server.py").exists()

requires_web = pytest.mark.skipif(
    not WEB_PRESENT, reason="CLI-сборка: веб-интерфейс не входит в поставку"
)
