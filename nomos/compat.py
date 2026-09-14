"""Совместимость со старыми версиями Python.

CLI должен запускаться на том Python, который уже стоит у заказчика, —
в том числе на 3.10. Две вещи из 3.11 мешали этому:

* ``datetime.UTC`` — псевдоним ``timezone.utc``, появился в 3.11;
* ``enum.StrEnum`` — тоже 3.11.

Обе заменяются без потери поведения, поэтому код проекта импортирует их
отсюда, а не из стандартной библиотеки напрямую.
"""

from datetime import timezone
from enum import Enum

UTC = timezone.utc

try:  # Python 3.11+
    from enum import StrEnum  # novermin
except ImportError:  # Python 3.10

    class StrEnum(str, Enum):  # noqa: UP042 — это и есть замена StrEnum
        """Замена enum.StrEnum для 3.10.

        Важны не только значения, но и поведение при выводе: у штатного
        StrEnum ``str(x)`` и ``f"{x}"`` дают значение, а у обычной пары
        ``str, Enum`` в 3.10 — ``'Класс.ИМЯ'``. Отчёты и JSON зависят от
        этого, поэтому оба метода берём у строки.
        """

        __str__ = str.__str__
        __format__ = str.__format__


__all__ = ["UTC", "StrEnum"]
