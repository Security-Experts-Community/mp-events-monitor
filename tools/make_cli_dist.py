#!/usr/bin/env python3
"""Сборка CLI-поставки: то же самое, но без веб-интерфейса.

    python tools/make_cli_dist.py [--out dist/Nomos_cli.zip]

Зачем: обратная совместимость и стенды, где веб не поможет — запустил
``python Nomos.py``, получил xlsx-отчёты. Из поставки исключается всё,
что нужно только вебу (FastAPI/uvicorn, SQLite-хранилище прогонов,
сервисный слой веба, статика), поэтому ставить нужно меньше зависимостей.

Доменная логика, конфиги, отладочная обвязка и тесты остаются теми же
файлами — это не форк, а срез одного репозитория.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from envcheck import child_environment, configure_console  # noqa: E402

configure_console()

# Только веб: в CLI-поставке этих файлов нет
WEB_ONLY_PATHS = [
    "nomos/web",
    "nomos_web.py",
    "nomos/service/runner.py",  # CheckRun нужен вебу, CLI идёт через Nomos.py
    "nomos/storage.py",  # история прогонов — витрина веба
    "nomos.spec",
    "build_exe.ps1",
    # Пайплайн собирает и веб тоже — в срезе ему делать нечего
    ".gitlab-ci.yml",
]

WEB_ONLY_TESTS = [
    "tests/test_frontend_static.py",
    "tests/test_filter_reports.py",
    "tests/test_kb_visibility.py",
    "tests/test_pause_and_incremental.py",
    "tests/test_run_config.py",
    "tests/test_storage_and_api.py",
]

# Пакеты, которые нужны только вебу
WEB_ONLY_REQUIREMENTS = ("fastapi", "uvicorn", "starlette", "httpx")

SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", ".ruff_cache", "build", "dist",
             "out", "logs", "debug", "golden", "web_inputs", ".venv"}

CLI_README = """# Nomos CLI

Консольная поставка: без веб-интерфейса, только прогон и xlsx-отчёты.
Доменная логика, конфиги и отладочная обвязка — те же, что в полной
версии.

## Установка

Нужен **Python 3.10 или новее** — верхней границы нет, подойдёт тот, что
уже стоит на машине. Проверены 3.10, 3.12 и 3.14. На более свежих Nomos
предупредит, но запустится; если установка пакетов там упадёт (у
pydantic-core может не быть колёс), он подскажет, что делать.

```powershell
py -3.12 -m pip install -r requirements.txt
copy configs\\example.config.env configs\\.config.env   :: заполнить HOST и токен
```

Зависимостей меньше, чем в полной версии: FastAPI и uvicorn не нужны.

Если `pip` не находится — вызывайте через интерпретатор
(`py -3.12 -m pip`). Если pip в самой установке Python сломан
(`No module named 'pip._vendor...'`) — `py -3.12 -m ensurepip --upgrade`.

## Запуск

```bash
python Nomos.py                                  # режим по умолчанию
python Nomos.py mode=Assets_filters time_delta_hours=168
python Nomos.py --help                           # все параметры
```

Отчёты — в `out/<папка прогона>/*.xlsx`, журналы — в `logs/`.

## Отладка

Те же флаги, что и в полной версии:

```bash
python Nomos.py debug_dump=true          # сырые ответы API в debug/
python Nomos.py record_fixtures=true     # запись фикстур для offline-регрессии
python Nomos.py dump_queries=true        # план запросов + out/query_fixtures.json
python Nomos.py logging_level=DEBUG      # трассировка HTTP
```

Диагностический бандл для разбора: `python tools/make_debug_bundle.py`.

## Тесты

```bash
python -m pytest tests -q
```

Тесты веб-интерфейса в эту поставку не входят; остальные — те же.

## Единый exe одной командой

```powershell
powershell -ExecutionPolicy Bypass -File build_cli.ps1
```

или в Git Bash:

```bash
./build_cli.sh
```

Скрипт сам создаст `.venv`, поставит зависимости и PyInstaller, прогонит
тесты и соберёт `dist\\NomosCLI_<дата>.zip`: внутри `NomosCLI.exe` и
папка `configs\\` рядом с ним. Python на целевой машине не нужен,
остаётся создать `configs\\.config.env` из `example.config.env`.

Ключи передаются насквозь: `./build_cli.sh --no-test`,
`--out ФАЙЛ`. Собирать нужно на Windows — PyInstaller не
кросс-компилирует.
"""

CLI_GUARD_TEST = '''"""Проверки самой CLI-поставки (файл добавляется сборщиком).

Смысл поставки — работать без веб-зависимостей. Если сюда просочится
импорт fastapi или файл веба, это должно падать здесь, а не у оператора.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

WEB_PATHS = ["nomos/web", "nomos_web.py", "nomos/storage.py", "nomos/service/runner.py"]


def test_no_web_files():
    present = [p for p in WEB_PATHS if (ROOT / p).exists()]
    assert not present, f"в CLI-поставке остались файлы веба: {present}"


def test_no_web_requirements():
    text = (ROOT / "requirements.txt").read_text(encoding="utf-8").lower()
    for package in ("fastapi", "uvicorn"):
        assert package not in text, f"{package} не нужен консольной версии"


def test_pyproject_matches_the_slice():
    """pip install -e ".[dev]" не должен тянуть веб и искать nomos_web."""
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert '"fastapi' not in text and '"uvicorn' not in text
    assert "nomos_web" not in text


def test_nothing_imports_the_web():
    for path in ROOT.rglob("*.py"):
        if "tests" in path.parts or path.name == "make_cli_dist.py":
            continue
        source = path.read_text(encoding="utf-8")
        for forbidden in ("import fastapi", "from fastapi", "import uvicorn",
                          "nomos.web", "nomos.storage", "service.runner"):
            assert forbidden not in source, f"{path.name}: {forbidden}"


def test_entry_point_and_configs_in_place():
    assert (ROOT / "Nomos.py").is_file()
    assert (ROOT / "nomos_cli.spec").is_file()
    for name in ("assets_filters.json", "event_policies.json", "table_filters.json"):
        assert (ROOT / "configs" / name).is_file(), name


def test_cli_starts_and_reports_settings(tmp_path):
    """Nomos.py --help работает без установленных веб-пакетов."""
    import os
    import subprocess
    import sys

    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"  # на Windows вывод иначе в cp1251
    env["PYTHONUTF8"] = "1"
    result = subprocess.run(
        [sys.executable, "Nomos.py", "--help"],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
        errors="replace", env=env, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "mode" in result.stdout
'''


def _trim_pyproject(path: Path) -> None:
    """Убирает из метаданных то, чего в срезе нет.

    Иначе `pip install -e ".[dev]"` в поставке падает: setuptools ищет
    модуль nomos_web, а FastAPI с uvicorn тянутся как зависимости, хотя
    веб в срез не входит.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    result = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(("\"fastapi", "\"uvicorn")):
            continue
        if stripped.startswith("# Веб-слой"):
            continue
        if stripped.startswith("py-modules"):
            line = 'py-modules = ["Nomos"]'
        # область линта: файла веб-точки входа в срезе нет
        line = line.replace('"nomos_web.py", ', "")
        result.append(line)
    path.write_text("\n".join(result) + "\n", encoding="utf-8")


def _skip(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    if any(part in SKIP_DIRS for part in relative.parts):
        return True
    return relative.name == ".config.env"


def build(target: Path, keep_fixtures: bool = False) -> Path:
    staging = target.parent / "_cli_staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    copied = 0
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or _skip(path, ROOT):
            continue
        relative = path.relative_to(ROOT)
        if not keep_fixtures and relative.parts[0] == "fixtures":
            continue
        if any(str(relative).startswith(web) for web in WEB_ONLY_PATHS):
            continue
        if str(relative).replace("\\", "/") in WEB_ONLY_TESTS:
            continue
        destination = staging / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        copied += 1

    _trim_pyproject(staging / "pyproject.toml")

    # requirements без веб-пакетов
    requirements = (staging / "requirements.txt").read_text(encoding="utf-8")
    kept = [
        line
        for line in requirements.splitlines()
        if not any(line.lower().startswith(pkg) for pkg in WEB_ONLY_REQUIREMENTS)
        and "# Веб-интерфейс" not in line
    ]
    while kept and not kept[-1].strip():
        kept.pop()
    (staging / "requirements.txt").write_text("\n".join(kept) + "\n", encoding="utf-8")

    (staging / "README_CLI.md").write_text(CLI_README, encoding="utf-8")
    (staging / "tests" / "test_cli_distribution.py").write_text(
        CLI_GUARD_TEST, encoding="utf-8"
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                archive.write(path, f"nomos_cli/{path.relative_to(staging).as_posix()}")
    print(f"собрано файлов: {copied + 2}; архив: {target}")
    return staging


def verify(staging: Path) -> None:
    """Проверяет, что срез живой: тесты проходят прямо в нём.

    Дочернему Python явно задаём UTF-8 и читаем с errors="replace": на
    Windows он иначе пишет в кодировке локали, а родитель ждёт UTF-8 и
    падает с UnicodeDecodeError, так и не показав результат тестов
    (репорт оператора 15.09).
    """
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests", "-q"],
        cwd=staging,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=child_environment(),
    )
    output = (result.stdout or "") + (result.stderr or "")
    lines = [line for line in output.strip().splitlines() if line.strip()]
    if result.returncode == 0:
        print(lines[-1] if lines else "тесты прошли")
        return
    print("\n".join(lines[-25:]))
    raise SystemExit("тесты CLI-поставки не прошли")


def main() -> int:
    parser = argparse.ArgumentParser(description="Сборка CLI-поставки Nomos")
    parser.add_argument("--out", type=Path, default=ROOT / "dist" / "Nomos_cli.zip")
    parser.add_argument("--keep-fixtures", action="store_true",
                        help="включить fixtures/ (offline-регрессия)")
    parser.add_argument("--no-verify", action="store_true")
    args = parser.parse_args()

    staging = build(args.out, keep_fixtures=args.keep_fixtures)
    if not args.no_verify:
        verify(staging)
    manifest = {
        "archive": str(args.out),
        "excluded": WEB_ONLY_PATHS + WEB_ONLY_TESTS,
    }
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
