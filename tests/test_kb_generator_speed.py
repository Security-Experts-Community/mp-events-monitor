"""Эквивалентность индекса исходному перебору (ускорение анализа политик).

Репорт оператора 10.09: 141 с на политику, ~3 часа на прогон — на каждый
из 136 развёрнутых запросов делалось два полных обхода knowledgebase.
Индекс строится один раз; здесь проверяется, что ответы совпадают с
эталонной «медленной» реализацией прототипа на случайном репозитории.
"""

import importlib.util
import json
import os
import random
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
TOOL = ROOT / "tools" / "kb_config_generator.py"

FIELDS = ["event_src.subsys", "msgid", "event_src.vendor", "action", "status"]
VALUES = ["security", "system", "4624", "4625", "5145", "microsoft", "cisco",
          "start", "success", "failure", None, True]


def _load_tool():
    spec = importlib.util.spec_from_file_location("kb_config_generator_speed", TOOL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _reference_norms(root: Path, query: dict, check_match) -> list[str]:
    """Исходная реализация: перебор всех *.js обходом дерева."""
    found = []
    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            if not filename.endswith(".js"):
                continue
            data = json.loads((Path(dirpath) / filename).read_text(encoding="utf-8"))
            if data and check_match(query, data):
                if data.get("id"):
                    found.append(data["id"])
    return sorted(set(found))


def _reference_packs(root: Path, norms: list[str], get_relative_package_path):
    """Исходная реализация поиска пакетов по правилам нормализации."""
    packs, deps = [], {}
    for dirpath, _, filenames in os.walk(root):
        if not any(f.endswith(".co") for f in filenames):
            continue
        meta = Path(dirpath) / "metainfo.yaml"
        if not meta.is_file():
            continue
        rule_meta = yaml.safe_load(meta.read_text(encoding="utf-8")) or {}
        try:
            nf_dict = rule_meta["ContentRelations"]["Uses"]["SIEMKB"]["Auto"][
                "NormalizationRules"
            ]
        except (KeyError, TypeError):
            continue
        for nf_value in nf_dict.values():
            if nf_value in norms:
                package = get_relative_package_path(meta)
                if package:
                    packs.append(package)
                    deps.setdefault(package, []).append(meta.parent.name)
    return sorted(set(packs)), {k: sorted(set(v)) for k, v in deps.items()}


@pytest.fixture(scope="module")
def kb(tmp_path_factory):
    """Случайный, но воспроизводимый репозиторий knowledgebase."""
    rnd = random.Random(20260910)
    root = tmp_path_factory.mktemp("kb") / "packages"
    norm_ids = []
    for pkg in range(6):
        pkg_dir = root / f"pack_{pkg}"
        for norm in range(12):
            norm_dir = pkg_dir / "normalization_formulas" / f"nf_{pkg}_{norm}"
            norm_dir.mkdir(parents=True)
            data = {"id": f"nf_{pkg}_{norm}"}
            for field in rnd.sample(FIELDS, rnd.randint(1, 4)):
                data[field] = rnd.choice([v for v in VALUES if v is not None])
            norm_ids.append(data["id"])
            (norm_dir / "formula.js").write_text(
                json.dumps(data, ensure_ascii=False), encoding="utf-8"
            )
        for rule in range(8):
            rule_dir = pkg_dir / "correlation_rules" / f"rule_{pkg}_{rule}"
            rule_dir.mkdir(parents=True)
            (rule_dir / "rule.co").write_text("event X: key: 1\n", encoding="utf-8")
            uses = {
                f"dep{i}": nf
                for i, nf in enumerate(rnd.sample(norm_ids, rnd.randint(1, 3)))
            }
            (rule_dir / "metainfo.yaml").write_text(
                yaml.safe_dump(
                    {
                        "ContentRelations": {
                            "Uses": {"SIEMKB": {"Auto": {"NormalizationRules": uses}}}
                        }
                    }
                ),
                encoding="utf-8",
            )
    return root


@pytest.fixture(scope="module")
def tool_on_kb(kb):
    tool = _load_tool()
    tool.set_kb_root(kb.parent)
    tool.build_policy_index()
    return tool


def _queries():
    rnd = random.Random(4242)
    queries = []
    for _ in range(40):
        query = {}
        for field in rnd.sample(FIELDS, rnd.randint(1, 3)):
            choice = rnd.choice(VALUES)
            if rnd.random() < 0.3:
                choice = [c for c in rnd.sample(VALUES[:9], 2)]
            query[field] = choice
        queries.append(query)
    queries.append({})  # пустой запрос: прототип считал совпадением всё
    return queries


@pytest.mark.parametrize("query", _queries())
def test_norms_match_reference(tool_on_kb, kb, query):
    fast = sorted(tool_on_kb.find_matching_js_files_parallel(kb, query))
    slow = _reference_norms(kb, query, tool_on_kb.check_match)
    assert fast == slow, query


def test_packs_match_reference(tool_on_kb, kb):
    for query in _queries()[:15]:
        norms = tool_on_kb.find_matching_js_files_parallel(kb, query)
        packs, deps = tool_on_kb.find_correlation_packs_parallel(kb, norms)
        ref_packs, ref_deps = _reference_packs(
            kb, norms, tool_on_kb.get_relative_package_path
        )
        assert sorted(set(packs)) == ref_packs, query
        assert {k: sorted(set(v)) for k, v in deps.items()} == ref_deps, query


def test_repository_is_walked_once(tool_on_kb, kb, monkeypatch):
    """Главная регрессия: индекс готов — обходов файловой системы больше нет."""
    walks = []
    real_walk = os.walk
    monkeypatch.setattr(os, "walk", lambda *a, **kw: (walks.append(a), real_walk(*a, **kw))[1])
    for query in _queries()[:10]:
        norms = tool_on_kb.find_matching_js_files_parallel(kb, query)
        tool_on_kb.find_correlation_packs_parallel(kb, norms)
    assert walks == [], f"файловая система обойдена {len(walks)} раз(а)"
