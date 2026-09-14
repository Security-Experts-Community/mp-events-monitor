"""Доменные исключения Nomos.

Замена `exit(1)` из библиотечного кода (находка N2 код-ревью): библиотека
сигнализирует исключениями, решение о завершении процесса принимает
точка входа. Для веб-версии это обязательное условие — воркер FastAPI
не должен умирать из-за одного кривого фильтра.
"""

from __future__ import annotations


class NomosError(Exception):
    """Базовое исключение проекта."""


class MPXAuthError(NomosError):
    """Аутентификация/авторизация в MaxPatrol не удалась или отозвана.

    Прерывает прогон целиком: без валидного токена продолжать нечего.
    """


class FilterExecutionError(NomosError):
    """Ошибка выполнения одного asset-фильтра.

    Прерывает только текущий фильтр; прогон продолжается со следующего.
    """

    def __init__(self, filter_name: str, reason: str):
        self.filter_name = filter_name
        self.reason = reason
        super().__init__(f"Фильтр '{filter_name}': {reason}")


class RunStopped(NomosError):
    """Прогон остановлен оператором (мягкая остановка на границе фильтра)."""
