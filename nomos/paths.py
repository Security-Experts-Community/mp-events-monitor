"""Пути приложения: одинаково из репозитория и из собранного exe.

PyInstaller в режиме onefile распаковывает код во временную папку
(``sys._MEIPASS``), поэтому ``Path(__file__).parent`` указывает не туда,
где лежит exe. Разделяем два понятия:

* **ресурсы сборки** (статика веба) — внутри бандла, рядом с кодом;
* **рабочая папка** (``configs/``, ``out/``, ``logs/``, ``nomos.db``) —
  рядом с exe, чтобы оператор правил конфиги и забирал отчёты, не
  распаковывая ничего.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def is_frozen() -> bool:
    """Запущены из собранного PyInstaller исполняемого файла."""
    return bool(getattr(sys, "frozen", False))


def app_base_dir() -> Path:
    """Рабочая папка: где лежат configs/, out/, logs/, nomos.db.

    Для exe — папка самого exe; для исходников — корень репозитория.
    """
    if is_frozen():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def bundle_dir() -> Path:
    """Папка распакованных ресурсов сборки (или корень репозитория)."""
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass)
    return Path(__file__).resolve().parent.parent


def resource_path(relative: str | Path) -> Path:
    """Ресурс сборки по пути относительно корня проекта."""
    return bundle_dir() / relative


def use_app_base_dir() -> Path:
    """Делает рабочую папку текущей (относительные configs/ и out/).

    Вызывается только у собранного exe: запуск двойным кликом или из
    произвольной директории не должен менять, где искать конфиги.
    """
    base = app_base_dir()
    if is_frozen():
        os.chdir(base)
    return base
