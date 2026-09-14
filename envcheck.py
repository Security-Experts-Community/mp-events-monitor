"""Проверка окружения до импорта зависимостей.

Модуль намеренно написан на синтаксисе старых Python и не импортирует
ничего, кроме стандартной библиотеки: он обязан отработать раньше, чем
падёт первый `import pydantic`.

Политика версий (репорты оператора 14.09):

* **ниже 3.10 — останавливаемся.** Код использует синтаксис, которого
  там нет; запуск всё равно закончится SyntaxError, но уже без внятного
  объяснения.
* **3.10–3.14 — проверено,** тихо работаем.
* **новее — пробуем.** Запрещать нельзя: Nomos запускают на том Python,
  который уже стоит у заказчика. Но предупреждаем: сторонние пакеты
  (pydantic-core) могут ещё не иметь колёс под эту версию.

Отдельная беда — текст ошибки, когда колёс действительно нет:
`ImportError: DLL load failed while importing _pydantic_core`. По нему
невозможно догадаться, что дело в версии Python, поэтому при падении
импорта дописываем подсказку.
"""

import sys

MIN_VERSION = (3, 10)
LAST_TESTED = (3, 14)

_TOO_OLD = """
Nomos не запустится на Python {current}.

Нужен Python {minimum} или новее. Посмотрите, какие версии стоят:
  Windows:  py -0p
  Linux:    ls /usr/bin/python3*

и запускайте подходящей: py -3.12 Nomos.py
"""

_UNTESTED = (
    "Внимание: Python {current} новее проверенных ({tested}). "
    "Nomos должен работать, но если установка пакетов или запуск упадут "
    "с ошибкой про _pydantic_core, поставьте Python {tested} — колёс под "
    "свежие версии может ещё не быть.\n"
)

_IMPORT_ADVICE = """
Похоже, зависимости не встали в этот интерпретатор.

Проверьте, что пакеты ставились именно им:
  {executable} -m pip install -r requirements.txt

Если ошибка упоминает _pydantic_core или "DLL load failed", у
pydantic-core нет готовых колёс под Python {current}: поставьте Python
{tested} и повторите установку тем же интерпретатором.
"""


def _format(version):
    return "{}.{}".format(version[0], version[1])  # noqa: UP032


def python_is_supported(version=None):
    """Версия не ниже минимальной (сверху ограничения нет)."""
    version = tuple((version or sys.version_info[:2])[:2])
    return version >= MIN_VERSION


def python_is_tested(version=None):
    """Версия входит в проверенный диапазон."""
    version = tuple((version or sys.version_info[:2])[:2])
    return MIN_VERSION <= version <= LAST_TESTED


def describe_unsupported(version=None):
    """Объяснение для слишком старого Python."""
    version = version or sys.version_info[:2]
    return _TOO_OLD.format(current=_format(version), minimum=_format(MIN_VERSION))


def describe_untested(version=None):
    """Предупреждение для Python новее проверенных."""
    version = version or sys.version_info[:2]
    return _UNTESTED.format(current=_format(version), tested=_format(LAST_TESTED))


def describe_import_failure(version=None):
    """Подсказка, когда сторонние пакеты не импортируются."""
    version = version or sys.version_info[:2]
    return _IMPORT_ADVICE.format(
        executable=sys.executable,
        current=_format(version),
        tested=_format(LAST_TESTED),
    )


def install_import_advice():
    """Дописывает подсказку к трейсбеку неудачного импорта.

    Трейсбек остаётся целиком — он нужен для разбора; подсказка идёт
    после него, последней строкой, которую человек и прочитает.
    """
    previous = sys.excepthook

    def hook(kind, value, traceback):
        previous(kind, value, traceback)
        if issubclass(kind, ImportError):
            sys.stderr.write(describe_import_failure())
            sys.stderr.flush()

    sys.excepthook = hook


def ensure_supported_python():
    """Останавливает запуск только на слишком старом Python."""
    configure_console()
    install_import_advice()
    if not python_is_supported():
        sys.stderr.write(describe_unsupported())
        sys.stderr.flush()
        raise SystemExit(2)
    if not python_is_tested():
        sys.stderr.write(describe_untested())
        sys.stderr.flush()


def configure_console():
    """Заставляет наш вывод быть UTF-8 там, где Python выбрал кодировку ОС.

    В Git Bash (MINGW64) и при перенаправлении в файл Windows-Python
    берёт кодировку локали (cp1251/cp866), и русский текст превращается
    в «▒▒▒▒▒» (репорт оператора 15.09). У настоящей консоли cmd.exe
    кодировка уже utf-8, её не трогаем.

    errors="replace" — чтобы вывод программы никогда не ронял её саму.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        encoding = (getattr(stream, "encoding", "") or "").lower()
        if encoding.replace("-", "") == "utf8":
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            pass


def child_environment(env=None):
    """Окружение для дочернего Python: его вывод тоже должен быть UTF-8.

    Иначе родитель читает stdout как UTF-8, а ребёнок пишет в cp1251 —
    и чтение падает с UnicodeDecodeError ещё до того, как станет
    понятно, чем закончился запуск.
    """
    import os

    result = dict(os.environ if env is None else env)
    result["PYTHONIOENCODING"] = "utf-8"
    result["PYTHONUTF8"] = "1"
    return result
