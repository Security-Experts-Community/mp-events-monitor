"""Редакция секретов в логах, дампах и диагностических бандлах.

Правило проекта: ни один токен, пароль или cookie не должен попасть
в файлы, которые оператор передаёт для отладки. Все каналы вывода
(консоль, nomos.log, JSONL, debug-дампы, фикстуры, бандл) проходят
через redact().
"""

from __future__ import annotations

import re

MASK = "***REDACTED***"

# Порядок важен: сначала специфичные форматы, затем общие пары ключ-значение.
_PATTERNS: list[re.Pattern[str]] = [
    # Personal Access Token MaxPatrol
    re.compile(r"pat_[A-Za-z0-9]{16,}"),
    # Bearer / Basic в заголовках
    re.compile(r"(?i)(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}"),
    # JWT (три base64url-сегмента через точки)
    re.compile(r"eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}"),
]

# Пары ключ-значение в JSON / env / query: password=..., "access_token": "..."
_KV_KEYS = (
    "password",
    "passwd",
    "personal_token",
    "access_token",
    "refresh_token",
    "id_token",
    "token",
    "secret",
    "client_secret",
    "mpx_secret",
    "authorization",
    "cookie",
    "set-cookie",
    "proxy_password",
)
_KV_PATTERN = re.compile(
    r"(?i)(\"?(?:" + "|".join(_KV_KEYS) + r")\"?\s*[:=]\s*)"
    r"(\"[^\"]*\"|'[^']*'|[^\s,;&}\]]+)"
)


def _kv_replacement(match: re.Match) -> str:
    """Маска с сохранением кавычек значения: редакция уже
    сериализованного JSON не должна ломать его синтаксис."""
    value = match.group(2)
    if value.startswith('"') and value.endswith('"'):
        masked = f'"{MASK}"'
    elif value.startswith("'") and value.endswith("'"):
        masked = f"'{MASK}'"
    else:
        masked = MASK
    return match.group(1) + masked

_SENSITIVE_HEADERS = {"authorization", "cookie", "set-cookie", "x-api-key"}


def redact(text: str) -> str:
    """Маскирует секреты в произвольной строке."""
    if not text:
        return text
    for pattern in _PATTERNS:
        text = pattern.sub(MASK, text)
    text = _KV_PATTERN.sub(_kv_replacement, text)
    return text


def redact_headers(headers: dict) -> dict:
    """Копия заголовков с маскировкой чувствительных значений."""
    out = {}
    for key, value in dict(headers).items():
        if key.lower() in _SENSITIVE_HEADERS:
            out[key] = MASK
        else:
            out[key] = redact(str(value))
    return out


def redact_obj(obj):
    """Рекурсивная редакция JSON-совместимой структуры."""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if str(key).lower() in _KV_KEYS or str(key).lower() in _SENSITIVE_HEADERS:
                result[key] = MASK
            else:
                result[key] = redact_obj(value)
        return result
    if isinstance(obj, list):
        return [redact_obj(item) for item in obj]
    if isinstance(obj, str):
        return redact(obj)
    return obj


def redact_env_text(env_text: str) -> str:
    """Редакция .env-файла: значения секретных ключей маскируются построчно."""
    lines = []
    for line in env_text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip().lower()
            if any(marker in key for marker in ("token", "password", "secret")):
                lines.append(f"{line.split('=', 1)[0]}={MASK}")
                continue
        lines.append(redact(line))
    return "\n".join(lines)
