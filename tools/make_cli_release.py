#!/usr/bin/env python3
"""Готовый архив: единый exe + папка конфигов.

    python tools/make_cli_release.py

Одна команда: тесты -> PyInstaller -> папка поставки -> zip. На выходе
``dist/NomosCLI_<версия>.zip``, внутри — исполняемый файл и ``configs/``
рядом с ним, как того требует программа.

Собирать нужно на той ОС, где будут запускать: PyInstaller не
кросс-компилирует, на Linux получится Linux-бинарник, на Windows — exe.

Секрет ``configs/.config.env`` в архив НЕ кладётся: его заполняют на
месте из ``example.config.env``.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from envcheck import child_environment, configure_console  # noqa: E402

configure_console()

SPEC = ROOT / "nomos_cli.spec"
EXE_NAME = "NomosCLI.exe" if sys.platform == "win32" else "NomosCLI"

READ_ME = """Nomos CLI
=========

Запуск (Python не нужен):

    {exe}                       обычный прогон
    {exe} --help                все параметры
    {exe} mode=Assets_filters time_delta_hours=168

Перед первым запуском заполните доступ к MaxPatrol:
скопируйте configs\\example.config.env в configs\\.config.env
и впишите MPX_HOST и токен.

Папка configs\\ должна лежать рядом с исполняемым файлом.
Отчёты появятся в out\\, журналы — в logs\\, там же рядом.

Отладка:
    {exe} dump_queries=true     план предстоящих запросов
    {exe} debug_dump=true       сырые ответы API в debug\\
    {exe} logging_level=DEBUG   трассировка HTTP

Собрано: {built}
"""


def _run(command: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Дочерний процесс с читаемым UTF-8 выводом (важно для Windows)."""
    return subprocess.run(
        command,
        cwd=cwd or ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=child_environment(),
    )


def check_pyinstaller() -> None:
    result = _run([sys.executable, "-c", "import PyInstaller"])
    if result.returncode != 0:
        raise SystemExit(
            "PyInstaller не установлен. Поставьте его в тот же интерпретатор:\n"
            f"  {sys.executable} -m pip install pyinstaller"
        )


def check_pytest() -> None:
    """Отличает «тесты упали» от «тесты нечем запускать»."""
    if _run([sys.executable, "-c", "import pytest"]).returncode == 0:
        return
    raise SystemExit(
        "pytest не установлен в этот интерпретатор — запускать тесты нечем.\n"
        f"  {sys.executable} -m pip install -e \".[dev]\"\n"
        "или соберите без прогона: python tools/make_cli_release.py --no-test"
    )


def run_tests() -> None:
    print("1/4 Тесты (собирать сломанное незачем)")
    check_pytest()
    result = _run([sys.executable, "-m", "pytest", "tests", "-q"])
    output = (result.stdout or "") + (result.stderr or "")
    lines = [line for line in output.strip().splitlines() if line.strip()]
    if result.returncode != 0:
        print("\n".join(lines[-25:]))
        raise SystemExit("тесты не прошли, сборка остановлена")
    print(f"    {lines[-1] if lines else 'пройдены'}")


def build_exe(work: Path) -> Path:
    print("2/4 PyInstaller")
    result = _run([
        sys.executable, "-m", "PyInstaller", str(SPEC), "--noconfirm",
        "--distpath", str(work / "pyinstaller"), "--workpath", str(work / "build"),
    ])
    exe = work / "pyinstaller" / EXE_NAME
    if result.returncode != 0 or not exe.exists():
        output = (result.stdout or "") + (result.stderr or "")
        print("\n".join(output.strip().splitlines()[-25:]))
        raise SystemExit("PyInstaller не собрал исполняемый файл")
    print(f"    {exe.name}: {exe.stat().st_size / 1024 / 1024:.1f} МБ")
    return exe


def assemble(exe: Path, target_dir: Path) -> Path:
    """Складывает папку поставки: исполняемый файл и конфиги рядом с ним."""
    print("3/4 Папка поставки")
    if target_dir.exists():
        shutil.rmtree(target_dir)
    (target_dir / "configs").mkdir(parents=True)
    shutil.copy2(exe, target_dir / exe.name)

    copied = 0
    for path in sorted((ROOT / "configs").iterdir()):
        if not path.is_file() or path.name == ".config.env":
            continue  # секрет заполняется на месте
        shutil.copy2(path, target_dir / "configs" / path.name)
        copied += 1
    (target_dir / "README.txt").write_text(
        READ_ME.format(exe=exe.name, built=datetime.now().strftime("%Y-%m-%d %H:%M")),
        encoding="utf-8",
    )
    print(f"    конфигов: {copied}")
    return target_dir


def pack(source_dir: Path, archive: Path) -> Path:
    print("4/4 Архив")
    archive.parent.mkdir(parents=True, exist_ok=True)
    if archive.exists():
        archive.unlink()
    # zipfile сам ставит флаг UTF-8 в именах — Проводник читает кириллицу
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in sorted(source_dir.rglob("*")):
            if path.is_file():
                zf.write(path, f"{source_dir.name}/{path.relative_to(source_dir).as_posix()}")
    return archive


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Собрать поставку: единый exe + папка конфигов"
    )
    parser.add_argument("--out", type=Path, default=None, metavar="ФАЙЛ",
                        help="куда положить архив (по умолчанию dist/NomosCLI_<дата>.zip)")
    parser.add_argument("--no-test", action="store_true", help="пропустить тесты")
    parser.add_argument("--keep-build", action="store_true",
                        help="не удалять временные файлы PyInstaller")
    args = parser.parse_args()

    if not SPEC.exists():
        raise SystemExit(f"нет файла сборки: {SPEC}")
    check_pyinstaller()
    if not args.no_test:
        run_tests()

    work = ROOT / "build" / "cli_release"
    work.mkdir(parents=True, exist_ok=True)
    exe = build_exe(work)

    stamp = datetime.now().strftime("%Y%m%d")
    archive = args.out or (ROOT / "dist" / f"NomosCLI_{stamp}.zip")
    release_dir = (ROOT / "dist" / archive.stem)
    assemble(exe, release_dir)
    pack(release_dir, archive)

    if not args.keep_build:
        shutil.rmtree(work, ignore_errors=True)

    size = archive.stat().st_size / 1024 / 1024
    print()
    print(f"Готово: {archive} ({size:.1f} МБ)")
    print(f"Внутри: {release_dir.name}/{exe.name} и {release_dir.name}/configs/")
    print("Перед передачей: создать configs/.config.env из example.config.env")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
