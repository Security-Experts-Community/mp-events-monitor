"""Запуск дочернего Python в тестах.

На Windows дочерний процесс по умолчанию пишет в кодировке локали
(cp1251/cp866), а читаем мы как UTF-8 — получается UnicodeDecodeError
вместо результата (репорт оператора 15.09). Здесь это закрыто в одном
месте: и окружение ребёнка, и разбор его вывода.
"""

import os
import subprocess


def run(command, cwd=None, timeout=300):
    """subprocess.run с гарантированно читаемым UTF-8 выводом."""
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        timeout=timeout,
    )
