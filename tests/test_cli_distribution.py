"""Проверки самой CLI-поставки (файл добавляется сборщиком).

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
