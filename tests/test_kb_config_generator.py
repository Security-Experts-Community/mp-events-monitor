"""Регрессии офлайн-генератора конфигов (шаг 2.5, репорт оператора 10.09).

Симптомы репорта:
  ✗ Файл политик не найден: configs\\event_policies_old.json
  ✗ FileNotFoundError: 'configs\\\\packages_names.json'
Две причины: (1) пути к configs считались от текущей директории, а запуск
был из tools/; корень knowledgebase был захардкожен строкой Windows;
(2) вход анализа политик — event_policies_old.json — удалён на этапе 0.1
как «мёртвый артефакт», хотя это исходные запросы 74 политик.
"""

import importlib.util
import json
import shutil
import sys
from pathlib import Path

import pytest
from procs import run as run_python

ROOT = Path(__file__).resolve().parent.parent
TOOL = ROOT / "tools" / "kb_config_generator.py"


def _load_tool():
    spec = importlib.util.spec_from_file_location("kb_config_generator", TOOL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def tool():
    return _load_tool()


class TestPaths:
    """Пути не зависят от cwd: инструмент запускают и из tools/."""

    def test_configs_dir_is_repo_relative(self, tool):
        assert tool.CONFIGS_DIR == ROOT / "configs"
        assert tool.REPO_ROOT == ROOT

    def test_all_outputs_live_in_repo_configs(self, tool):
        outputs = [
            tool.OUTPUT_TABLE_FILTERS,
            tool.OUTPUT_TABLE_MAPPING,
            tool.OUTPUT_EVENT_POLICIES,
            tool.OUTPUT_PACKAGES_NAMES,
            tool.OUTPUT_SUBRULES,
            tool.POLICY_QUERIES,
        ]
        for path in outputs:
            assert path.is_absolute(), path
            assert path.parent == ROOT / "configs", path

    def test_kb_root_is_overridable(self, tool, tmp_path):
        try:
            tool.set_kb_root(tmp_path)
            assert tool.BASE_PACKAGES == tmp_path / "packages"
            assert tool.EXCLUDE_CFG == tmp_path / "_extra" / "slices.yaml"
        finally:
            tool.set_kb_root(tool.DEFAULT_KB_ROOT)

    def test_no_windows_paths_outside_default(self):
        """Хардкод Windows-путей допустим только как дефолт и в пояснениях."""
        source = TOOL.read_text(encoding="utf-8")
        offenders = [
            line.strip()
            for line in source.splitlines()
            if "D:\\Work" in line
            and "DEFAULT_KB_ROOT" not in line
            and not line.strip().startswith("#")
            and "\\\\" not in line  # текст докстринги с экранированием
        ]
        assert not offenders, offenders


class TestPolicyQueriesInput:
    """Вход анализа политик обязан лежать в репозитории."""

    def test_source_file_present_and_sane(self):
        path = ROOT / "configs" / "policy_queries.json"
        assert path.exists(), (
            "configs/policy_queries.json — исходные запросы политик, вход "
            "kb_config_generator; без него --policies-only не работает"
        )
        data = json.loads(path.read_text(encoding="utf-8"))
        assert len(data) == 74
        assert all("queries" in policy for policy in data.values())

    def test_legacy_name_is_fallback(self, tool, tmp_path, monkeypatch):
        monkeypatch.setattr(tool, "POLICY_QUERIES", tmp_path / "policy_queries.json")
        monkeypatch.setattr(
            tool, "LEGACY_POLICY_QUERIES", tmp_path / "event_policies_old.json"
        )
        assert tool.policy_queries_path().name == "policy_queries.json"
        (tmp_path / "event_policies_old.json").write_text("{}", encoding="utf-8")
        assert tool.policy_queries_path().name == "event_policies_old.json"


def _sandbox_repo(tmp_path: Path) -> Path:
    """Копия инструмента и конфигов: настоящие configs/ не трогаем."""
    repo = tmp_path / "repo"
    (repo / "tools").mkdir(parents=True)
    (repo / "configs").mkdir()
    shutil.copy(TOOL, repo / "tools" / TOOL.name)
    shutil.copy(ROOT / "envcheck.py", repo / "envcheck.py")  # настройка консоли
    for name in ("policy_queries.json", "packages_names.json"):
        shutil.copy(ROOT / "configs" / name, repo / "configs" / name)
    return repo


def _fake_kb(tmp_path: Path) -> Path:
    kb = tmp_path / "knowledgebase"
    rule = kb / "packages" / "demo_pack" / "correlation_rules" / "Demo_Rule"
    excluded = kb / "packages" / "excluded_pack" / "correlation_rules" / "Bad_Rule"
    table = kb / "packages" / "demo_pack" / "tabular_lists" / "List_Demo"
    for folder in (rule, excluded, table, kb / "_extra"):
        folder.mkdir(parents=True)
    (kb / "_extra" / "slices.yaml").write_text(
        "KnowledgebaseSlices:\n  SIEM-Public:\n    Excludes:\n      Files:\n"
        "        - packages/excluded_pack\n",
        encoding="utf-8",
    )
    (table / "table.tl").write_text("fillType: Registry\nname: List_Demo\n", encoding="utf-8")
    (rule / "rule.co").write_text('event Demo: key: table_list("List_Demo")\n', encoding="utf-8")
    (rule / "metainfo.yaml").write_text(
        "ContentRelations:\n  Uses:\n    SIEMKB:\n      Auto:\n"
        "        CorrelationRules:\n          dep1: Subrule_Demo\n",
        encoding="utf-8",
    )
    (excluded / "rule.co").write_text("event Bad: key: 1\n", encoding="utf-8")
    (excluded / "metainfo.yaml").write_text("ContentRelations: {}\n", encoding="utf-8")
    return kb


class TestCli:
    def test_missing_kb_root_reports_instead_of_traceback(self, tmp_path):
        repo = _sandbox_repo(tmp_path)
        result = run_python(
            [sys.executable, "kb_config_generator.py", "--kb-root", str(tmp_path / "нет")],
            cwd=repo / "tools",
        )
        assert result.returncode == 1
        assert "Traceback" not in result.stdout + result.stderr
        assert "--kb-root" in result.stdout

    def test_runs_from_tools_dir_and_writes_into_repo_configs(self, tmp_path):
        """Главная регрессия: запуск из tools/ (как у оператора) работает."""
        repo = _sandbox_repo(tmp_path)
        kb = _fake_kb(tmp_path)
        result = run_python(
            [sys.executable, "kb_config_generator.py", "--kb-root", str(kb)],
            cwd=repo / "tools",
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Traceback" not in result.stdout + result.stderr
        assert not (repo / "tools" / "configs").exists(), "конфиги ушли не туда"
        for name in ("table_filters.json", "table_mapping.json",
                     "event_policies.json", "subrules.json"):
            assert (repo / "configs" / name).exists(), name
        subrules = json.loads(
            (repo / "configs" / "subrules.json").read_text(encoding="utf-8")
        )
        # Имя правила и пакета берутся из структуры пути (раньше split("\\"))
        assert subrules == {"Subrule_Demo": {"demo_pack": ["Demo_Rule"]}}
