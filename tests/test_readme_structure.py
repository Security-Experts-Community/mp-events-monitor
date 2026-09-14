"""Раздел «Структура репозитория» против реального дерева.

Репорт оператора 15.09: раздел не совпадал с репозиторием «уже давно» —
в нём не было ни веб-версии, ни доброй половины новых модулей, зато
значилось «19 тестов» при трёхстах с лишним. Документация, которой
нельзя верить, хуже отсутствующей, поэтому теперь расхождение ловится
тестом.
"""

import re
from pathlib import Path

import pytest
from webcheck import WEB_PRESENT

ROOT = Path(__file__).resolve().parent.parent

# Раздел описывает полный репозиторий; в CLI-срезе части файлов нет
pytestmark = pytest.mark.skipif(
    not WEB_PRESENT, reason="CLI-срез: структура описывает полный репозиторий"
)
README = ROOT / "README.md"

# Каталоги верхнего уровня, которых в дереве README быть не должно
NOT_IN_REPO = {"__pycache__", ".git", ".venv", "build", "dist", "out", "logs",
               "debug", "golden", "web_inputs", ".pytest_cache", ".ruff_cache",
               "nomos.egg-info", "node_modules"}


def _gitignored_names() -> set[str]:
    """Имена из .gitignore: этих файлов в репозитории нет, описывать нечего."""
    names = set()
    for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines():
        entry = line.split("#")[0].strip().rstrip("/")
        if entry and "*" not in entry:
            names.add(entry)
    return names


def _structure_block() -> str:
    text = README.read_text(encoding="utf-8")
    start = text.index("## Структура репозитория")
    opening = text.index("```", start) + 3
    return text[opening : text.index("```", opening)]


def _mentioned_paths() -> set[str]:
    """Пути из левой колонки блока (до комментария)."""
    paths = set()
    for line in _structure_block().splitlines():
        head = line.split("#")[0].strip()
        if not head:
            continue
        for token in re.split(r"[ ,/]\s*|\s+/\s+", head):
            token = token.strip().rstrip("/")
            # строки вида "runlog, redact," — это модули nomos/, не пути
            if not token or token.endswith(","):
                continue
            if "." in token or token in {"lib", "nomos", "configs", "tools",
                                         "tests", "fixtures", "docs"}:
                paths.add(token)
    return paths


class TestInternalDocs:
    """Рабочие документы не хранятся в репозитории и не упоминаются в README."""

    INTERNAL = [
        "AGENT.md", "DEVELOPMENT.md", "Nomos_Анализ_и_документация.md",
        "Nomos_Код-ревью.md", "Nomos_План_рефакторинга.md",
    ]

    @pytest.mark.parametrize("name", INTERNAL)
    def test_listed_in_gitignore(self, name):
        assert name in _gitignored_names(), name

    @pytest.mark.parametrize("name", INTERNAL)
    def test_not_mentioned_in_readme(self, name):
        assert name not in README.read_text(encoding="utf-8"), name

    def test_not_shipped_in_cli_slice(self):
        source = (ROOT / "tools/make_cli_dist.py").read_text(encoding="utf-8")
        for name in self.INTERNAL:
            assert name in source, f"{name} не исключён из поставки"


class TestDevelopmentTree:
    """В DEVELOPMENT.md своё дерево — оно устаревало так же."""

    def _block(self) -> str:
        text = (ROOT / "DEVELOPMENT.md").read_text(encoding="utf-8")
        opening = text.index("```") + 3
        return text[opening : text.index("```", opening)]

    def test_mentioned_modules_exist(self):
        missing = []
        for line in self._block().splitlines():
            head = line.split("#")[0].strip().rstrip("/")
            for token in (part.strip() for part in head.split(",")):
                if not token or "." not in token:
                    continue
                if not any((ROOT / prefix / token).exists()
                           for prefix in ("", "nomos", "tools")):
                    missing.append(token)
        assert not missing, f"в DEVELOPMENT.md есть, в репозитории нет: {missing}"

    def test_test_count_is_not_stale(self):
        block = self._block()
        stated = [int(n) for n in re.findall(r"~?(\d+)\s+тест", block)]
        files = len(list((ROOT / "tests").glob("test_*.py")))
        assert stated, "количество тестов не указано"
        assert max(stated) > files, f"указано {stated}, тестовых файлов {files}"


class TestReadmeMatchesTree:
    def test_every_mentioned_path_exists(self):
        missing = []
        for name in sorted(_mentioned_paths()):
            if name.startswith("*") or "…" in name:
                continue
            candidates = [ROOT / name, ROOT / "nomos" / name,
                          ROOT / "tools" / name, ROOT / "docs" / name]
            if not any(path.exists() for path in candidates):
                missing.append(name)
        assert not missing, f"в README есть, в репозитории нет: {missing}"

    def test_every_top_level_entry_is_documented(self):
        block = _structure_block()
        undocumented = []
        ignored = _gitignored_names()
        for path in sorted(ROOT.iterdir()):
            name = path.name
            if name in NOT_IN_REPO or name in ignored or name.startswith(".git"):
                continue
            if name.startswith(".") and name not in {".gitlab-ci.yml"}:
                continue
            if name not in block:
                undocumented.append(name)
        assert not undocumented, (
            f"есть в репозитории, но не описано в README: {undocumented}"
        )

    @pytest.mark.parametrize(
        "entry",
        ["nomos_web.py", "envcheck.py", "domain/", "service/", "web/",
         "queryplan.py", "kb_presence.py", "tablelists.py", "make_cli_dist.py",
         "build_cli.sh", "nomos_cli.spec", ".gitlab-ci.yml"],
    )
    def test_key_pieces_are_named(self, entry):
        """То, чего в старом разделе не было вовсе."""
        assert entry.rstrip("/") in _structure_block(), entry

    def test_test_count_is_not_stale(self):
        """В разделе значилось «19 тестов» при трёхстах с лишним."""
        block = _structure_block()
        numbers = [int(n) for n in re.findall(r"~?(\d+)\s+тест", block)]
        assert numbers, "количество тестов в разделе не указано"
        actual = len(list((ROOT / "tests").glob("test_*.py")))
        for stated in numbers:
            # округлённое «~350» против реального числа файлов-тестов:
            # сверяем порядок, а не точное совпадение
            assert stated > actual, f"указано {stated}, а тестовых файлов {actual}"
            assert stated < 10000, stated
