"""Полнота зависимостей и поддерживаемые версии Python.

Репорт оператора 14.09: на машине, где проект никогда не запускали,
установка и запуск падали. Часть причин — внешняя (Python 3.14, сломанный
pip), но часть была на нашей стороне: объявленные зависимости не
совпадали с тем, что проект реально импортирует.
"""

import ast
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from webcheck import WEB_PRESENT  # noqa: E402

import envcheck  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

try:  # Python 3.11+
    import tomllib  # novermin
except ImportError:  # Python 3.10 — простого разбора хватает: файл наш
    tomllib = None


def _pyproject() -> dict:
    """Секции зависимостей pyproject без внешнего парсера TOML."""
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    if tomllib is not None:
        return tomllib.loads(text)
    project: dict = {"dependencies": [], "optional-dependencies": {}}
    section = None
    for raw in text.splitlines():
        line = raw.split("#")[0].strip()
        if not line:
            continue
        if line.startswith("["):
            section = line.strip("[]")
            continue
        if "=" in line and line.endswith("["):
            name = line.split("=")[0].strip()
            if section == "project" and name == "dependencies":
                current = project["dependencies"]
            elif section == "project.optional-dependencies":
                current = project["optional-dependencies"].setdefault(name, [])
            else:
                current = None
            continue
        if line.startswith('"') and current is not None:
            current.append(line.strip(',').strip('"'))
        elif section == "project" and line.startswith("requires-python"):
            project["requires-python"] = line.split("=", 1)[1].strip().strip('"')
        elif "=" in line and line.endswith("]") and section == "project.optional-dependencies":
            name, value = line.split("=", 1)
            project["optional-dependencies"][name.strip()] = [
                item.strip().strip('"') for item in value.strip("[] ").split(",") if item.strip()
            ]
    return {"project": project}
SKIP_DIRS = {".git", "__pycache__", ".venv", "build", "dist", ".pytest_cache"}

# Импортируются под try/except или лениво, по явному включению режима.
# Объявлены в extras pyproject: dl, telemetry, dev.
OPTIONAL_IMPORTS = {
    "pandas", "loguru", "sqlalchemy", "datalake_client",  # dl
    "pyminizip",  # telemetry
    "pytest", "colorama", "httpx", "ruff",  # dev
}

# Имя пакета на PyPI отличается от имени модуля
MODULE_TO_PACKAGE = {
    "yaml": "pyyaml",
    "pydantic_settings": "pydantic-settings",
    "datalake_client": "datalake-client",
}


# Стандартные не во всех поддерживаемых версиях: на 3.10 их нет в
# sys.stdlib_module_names, но пакетами они от этого не становятся
CONDITIONAL_STDLIB = {"tomllib"}


def _third_party_imports() -> dict[str, set[str]]:
    stdlib = set(sys.stdlib_module_names) | CONDITIONAL_STDLIB
    local = {"nomos", "Nomos", "lib", "tools", "envcheck", "webcheck", "conftest", "procs"}
    # прототип импортирует соседние модули lib/ без пакета
    local |= {p.stem for p in (ROOT / "lib").glob("*.py")}
    found: dict[str, set[str]] = {}
    for path in ROOT.rglob("*.py"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                names = [node.module.split(".")[0]]
            else:
                continue
            for name in names:
                if name not in stdlib and name not in local:
                    found.setdefault(name, set()).add(path.name)
    return found


def _declared_packages() -> set[str]:
    data = _pyproject()
    project = data["project"]
    specs = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        specs.extend(extra)
    return {re.split(r"[<>=!\[ ]", spec)[0].strip().lower() for spec in specs}


class TestDeclaredDependencies:
    def test_every_import_is_declared(self):
        """Ни один импорт не должен быть «забыт» в зависимостях."""
        declared = _declared_packages()
        if not WEB_PRESENT:
            # В CLI-срезе веб-пакетов нет, но импорт fastapi остался в тесте
            # под @requires_web — он пропускается, а не выполняется
            declared |= {"fastapi", "uvicorn"}
        missing = {}
        for module, files in _third_party_imports().items():
            package = MODULE_TO_PACKAGE.get(module, module).lower()
            if package not in declared:
                missing[module] = sorted(files)
        assert not missing, f"нет в pyproject: {missing}"

    def test_optional_imports_live_in_extras(self):
        """Тяжёлое и внутреннее не должно попадать в обязательные."""
        data = _pyproject()
        required = {
            re.split(r"[<>=!\[ ]", spec)[0].strip().lower()
            for spec in data["project"]["dependencies"]
        }
        for module in OPTIONAL_IMPORTS:
            package = MODULE_TO_PACKAGE.get(module, module).lower()
            assert package not in required, f"{package} не нужен обычному прогону"

    def test_requirements_matches_pyproject(self):
        """requirements.txt — то же, что обязательные зависимости.

        В CLI-поставке веб-пакетов нет ни в requirements, ни в коде.
        """
        data = _pyproject()
        expected = {
            re.split(r"[<>=!\[ ]", spec)[0].strip().lower()
            for spec in data["project"]["dependencies"]
        }
        if not WEB_PRESENT:
            expected -= {"fastapi", "uvicorn"}
        lines = [
            line.strip()
            for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        actual = {re.split(r"[<>=!\[ ]", line)[0].strip().lower() for line in lines}
        assert actual == expected, (
            f"лишние: {sorted(actual - expected)}, "
            f"недостающие: {sorted(expected - actual)}"
        )

    def test_datalake_libraries_are_all_declared(self):
        """Прототип объявлял только pandas — модуль тянет ещё три пакета."""
        source = (ROOT / "lib/events_dl.py").read_text(encoding="utf-8")
        data = _pyproject()
        dl = {
            re.split(r"[<>=!\[ ]", spec)[0].strip().lower()
            for spec in data["project"]["optional-dependencies"]["dl"]
        }
        for module in ("pandas", "loguru", "sqlalchemy", "datalake_client"):
            if module in source:
                assert MODULE_TO_PACKAGE.get(module, module).lower() in dl, module


class TestPythonVersionGuard:
    """Запускаться на том Python, который есть у заказчика."""

    @pytest.mark.parametrize("version", [(3, 10), (3, 12), (3, 13), (3, 15), (4, 0)])
    def test_new_versions_are_not_blocked(self, version):
        """Верхней границы нет: запрещать свежий Python мы не вправе."""
        assert envcheck.python_is_supported(version)

    @pytest.mark.parametrize("version", [(2, 7), (3, 8), (3, 9)])
    def test_too_old_is_blocked(self, version):
        assert not envcheck.python_is_supported(version)

    @pytest.mark.parametrize("version", [(3, 10), (3, 12), (3, 14)])
    def test_tested_range(self, version):
        assert envcheck.python_is_tested(version)

    @pytest.mark.parametrize("version", [(3, 9), (3, 15), (4, 0)])
    def test_outside_tested_range(self, version):
        assert not envcheck.python_is_tested(version)

    def test_untested_version_warns_but_runs(self):
        message = envcheck.describe_untested((3, 15))
        assert "3.15" in message and "_pydantic_core" in message
        assert "не запустится" not in message, "новую версию не запрещаем"

    def test_old_version_message_is_actionable(self):
        message = envcheck.describe_unsupported((3, 9))
        assert "3.9" in message and "3.10" in message
        assert "py -0p" in message

    def test_import_failure_advice_names_the_interpreter(self):
        """Главная причина на чистой машине: пакеты встали в другой Python."""
        message = envcheck.describe_import_failure((3, 14))
        assert sys.executable in message
        assert "_pydantic_core" in message

    def test_import_advice_hook_keeps_original_traceback(self):
        import io

        original = sys.excepthook
        printed = []
        sys.excepthook = lambda *a: printed.append("исходный трейсбек")
        try:
            envcheck.install_import_advice()
            stderr, sys.stderr = sys.stderr, io.StringIO()
            try:
                sys.excepthook(ImportError, ImportError("DLL load failed"), None)
                advice = sys.stderr.getvalue()
            finally:
                sys.stderr = stderr
        finally:
            sys.excepthook = original
        assert printed == ["исходный трейсбек"]
        assert "pip install" in advice

    def test_running_version_is_supported(self):
        assert envcheck.python_is_supported()

    def test_pyproject_declares_open_range(self):
        data = _pyproject()
        assert data["project"]["requires-python"] == ">=3.10"


class TestOldPythonCompatibility:
    """Код не должен использовать то, чего нет в 3.10."""

    def test_no_direct_utc_or_strenum_imports(self):
        offenders = []
        for path in ROOT.rglob("*.py"):
            if any(part in SKIP_DIRS for part in path.parts):
                continue
            if path.name == "compat.py":
                continue  # там они и заменяются
            source = path.read_text(encoding="utf-8")
            if re.search(r"from datetime import [^\n]*\bUTC\b", source):
                offenders.append(f"{path.name}: datetime.UTC (нет в 3.10)")
            if re.search(r"from enum import [^\n]*\bStrEnum\b", source):
                offenders.append(f"{path.name}: enum.StrEnum (нет в 3.10)")
        assert not offenders, offenders

    def test_compat_provides_replacements(self):
        from nomos.compat import UTC, StrEnum

        class Sample(StrEnum):
            VALUE = "значение"

        # у штатного StrEnum str() и f-строка дают значение, а не 'Sample.VALUE'
        assert str(Sample.VALUE) == "значение"
        assert f"{Sample.VALUE}" == "значение"
        assert Sample.VALUE == "значение"
        from datetime import datetime, timezone

        assert UTC is timezone.utc
        assert datetime.now(UTC).tzinfo is timezone.utc

    @pytest.mark.parametrize("entry", ["Nomos.py", "nomos_web.py"])
    def test_entry_points_check_before_third_party_imports(self, entry):
        path = ROOT / entry
        if not path.exists():  # CLI-поставка без веба
            pytest.skip(f"{entry} не входит в эту поставку")
        source = path.read_text(encoding="utf-8")
        guard_at = source.index("ensure_supported_python()")
        for third_party in ("import requests", "from pydantic", "from fastapi"):
            if third_party in source:
                assert guard_at < source.index(third_party), (
                    f"{entry}: {third_party} раньше проверки версии"
                )


def test_vermin_confirms_minimum_version():
    """Сквозная проверка совместимости с 3.10 (если vermin установлен).

    Ручные проверки ловят известные конструкции; vermin разбирает весь
    синтаксис и API целиком.
    """
    import shutil

    from procs import run as run_python

    if shutil.which("vermin") is None:
        pytest.skip("vermin не установлен (pip install -e .[dev])")
    targets = [
        str(ROOT / name)
        for name in ("Nomos.py", "lib", "nomos", "tools", "envcheck.py", "tests")
        if (ROOT / name).exists()
    ]
    result = run_python(["vermin", "-t=3.10", "--violations", *targets])
    assert result.returncode == 0, result.stdout[-3000:]
    assert "Minimum required versions: 3.10" in result.stdout, result.stdout[-2000:]
