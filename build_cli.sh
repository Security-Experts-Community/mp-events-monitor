#!/usr/bin/env bash
# Сборка консольной поставки Nomos: venv -> зависимости -> exe -> архив
#
#   ./build_cli.sh
#   ./build_cli.sh --no-test
#
# Работает и в Git Bash на Windows, и на Linux/macOS. Всё делается в
# локальном .venv рядом со скриптом: глобальный Python не трогаем, и
# состав exe не зависит от того, что установлено в системе.

set -euo pipefail
cd "$(dirname "$0")"

export PYTHONIOENCODING=utf-8
export PYTHONUTF8=1

find_python() {
    # py -3.12 надёжнее python из PATH: там может оказаться алиас Store
    if command -v py >/dev/null 2>&1; then
        for version in 3.12 3.13 3.11 3.10; do
            if py -"$version" --version >/dev/null 2>&1; then
                echo "py -$version"
                return
            fi
        done
    fi
    for name in python3.12 python3.13 python3.11 python3.10 python3 python; do
        if command -v "$name" >/dev/null 2>&1; then
            echo "$name"
            return
        fi
    done
    echo "Python не найден. Поставьте Python 3.12 (python.org)" >&2
    exit 1
}

PYTHON=$(find_python)
echo "Интерпретатор: $PYTHON"

if [ ! -d .venv ]; then
    echo "Создаю .venv"
    $PYTHON -m venv .venv
fi

# Windows кладёт исполняемые файлы venv в Scripts/, остальные — в bin/
if [ -x .venv/Scripts/python.exe ]; then
    VENV_PYTHON=.venv/Scripts/python.exe
elif [ -x .venv/bin/python ]; then
    VENV_PYTHON=.venv/bin/python
else
    echo "не найден python внутри .venv" >&2
    exit 1
fi

echo "Ставлю зависимости"
"$VENV_PYTHON" -m pip install --upgrade pip --quiet
"$VENV_PYTHON" -m pip install -r requirements.txt --quiet
# Сборщик прогоняет тесты, значит нужен pytest и остальное из [dev]
"$VENV_PYTHON" -m pip install -e ".[dev]" --quiet
"$VENV_PYTHON" -m pip install pyinstaller --quiet

echo "Собираю поставку"
"$VENV_PYTHON" tools/make_cli_release.py "$@"

echo
echo "Архив лежит в dist/"
