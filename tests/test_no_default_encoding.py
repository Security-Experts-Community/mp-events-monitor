"""Страж: текстовые open() без encoding= запрещены во всём проекте.

Причина — реальный инцидент 2026-07-06: на Windows дефолтная cp1251
уронила прогон на символе 'ʳ' в тексте формулы корреляции
(kb_checker.diff_formulas_to_file). Python на Windows подставляет
локальную кодировку, на Linux — utf-8; код обязан быть явным.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCAN = [ROOT / "lib", ROOT / "nomos", ROOT / "tools", ROOT / "Nomos.py"]
# Исключений нет: kb_config_generator приведён в порядок на шаге 2.5
EXCLUDE: set[str] = set()

_OPEN_RE = re.compile(r"\.open\(|(?<![\w.])open\(")


def _calls_without_encoding(path: Path):
    text = path.read_text(encoding="utf-8")
    for match in _OPEN_RE.finditer(text):
        start = match.end() - 1
        depth, i = 0, start
        while i < len(text):
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        call = text[start : i + 1]
        if "encoding" in call or "b\"" in call:
            continue
        if any(binary in call for binary in ('"rb"', "'rb'", '"wb"', "'wb'", '"ab"', "'ab'")):
            continue
        line = text[: match.start()].count("\n") + 1
        yield line, call.replace("\n", " ")[:80]


def test_all_text_opens_have_explicit_encoding():
    violations = []
    for target in SCAN:
        files = [target] if target.is_file() else sorted(target.rglob("*.py"))
        for path in files:
            if path.name in EXCLUDE:
                continue
            for line, call in _calls_without_encoding(path):
                violations.append(f"{path.relative_to(ROOT)}:{line}: open{call}")
    assert not violations, "open() без encoding:\n" + "\n".join(violations)
