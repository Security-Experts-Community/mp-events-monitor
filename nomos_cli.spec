# -*- mode: python ; coding: utf-8 -*-
"""Единый exe консольной версии.

Собирается не руками, а скриптом: python tools/make_cli_release.py —
он прогоняет тесты, зовёт PyInstaller и складывает папку поставки.
Напрямую тоже можно: pyinstaller nomos_cli.spec

Конфиги внутрь не запекаются: папка configs/ лежит рядом с exe, чтобы
правки экспертизы не требовали пересборки.
"""

import os

# Ресурс версии Windows: CI подставляет путь, локальная сборка обходится без него
VERSION_FILE = os.environ.get("NOMOS_VERSION_FILE") or None

a = Analysis(
    ["Nomos.py"],
    pathex=["."],
    binaries=[],
    datas=[],
    # Подгружается динамически, статический анализ не видит
    hiddenimports=["sqlite3", "xlsxwriter", "encodings.idna"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    # Веб-пакеты в консольной версии не нужны даже если стоят в окружении
    excludes=[
        "tkinter", "matplotlib", "numpy", "pandas", "pytest", "PIL",
        "fastapi", "uvicorn", "starlette",
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name=os.environ.get("NOMOS_EXE_NAME", "NomosCLI"),
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version=VERSION_FILE,
)
