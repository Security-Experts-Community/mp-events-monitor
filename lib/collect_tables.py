import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import yaml

BASE_KB_ROOT = Path(r"D:\Work\repo\knowledgebase")
BASE_PACKAGES = BASE_KB_ROOT / "packages"
EXCLUDE_CFG = Path(r"D:\Work\repo\knowledgebase\_extra\build_on_server\slices.yaml")


def _load_excludes(cfg_path: Path) -> set[Path]:
    if not cfg_path.is_file():
        sys.stderr.write(f"Warning: {cfg_path} not found → no excludes.\n")
        return set()
    try:
        with cfg_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as exc:
        sys.stderr.write(f"Error parsing {cfg_path}: {exc}\n")
        return set()
    try:
        file_list = cfg["KnowledgebaseSlices"]["SIEM-Public"]["Excludes"]["Files"]
    except Exception:
        sys.stderr.write("Warning: Excludes list not found in config.\n")
        return set()
    excludes = set()
    for entry in file_list or []:
        rel = Path(entry).as_posix().strip("/")
        if rel:
            excludes.add(BASE_KB_ROOT / rel)
    return excludes


EXCLUDE_PATHS = _load_excludes(EXCLUDE_CFG)


def _is_excluded(some_path: Path, exclude_paths: set[Path]) -> bool:
    for excl in exclude_paths:
        if some_path.is_relative_to(excl):
            return True
    return False


def _is_registry_table(tl_path: Path) -> bool:
    """
    Возвращает True ⇔ в YAML‑файле table.tl
        • есть ключ ``fillType`` со значением ``Registry``  И
        • выполнено хотя бы одно из условий:
            1) в структуре присутствует ключ  defaults → PT
            2) путь к файлу содержит подстроку ``whitelist`` или ``blacklist``
    """
    # 1️⃣  Читаем YAML‑файл
    try:
        with tl_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as exc:  # если файл не читается считаем, что он не подходит
        sys.stderr.write(f"Warning: cannot read {tl_path}: {exc}\n")
        return False

    # 2️⃣  Основная проверка fillType
    if not (isinstance(data, dict) and data.get("fillType") == "Registry"):
        return False

    # 3️⃣  Альтернативные условия
    has_defaults_pt = (
        isinstance(data, dict)
        and isinstance(data.get("defaults"), dict)
        and "PT" in data["defaults"]
    )
    path_contains_flag = any(
        word in tl_path.as_posix().lower() for word in ("whitelist", "blacklist")
    )

    return not has_defaults_pt or path_contains_flag


def _collect_all_registry_tables(
    packages_root: Path, exclude_paths: set[Path]
) -> dict[str, set[str]]:
    tables_by_pkg = defaultdict(set)
    for tl_path in packages_root.glob("*/tabular_lists/*/table.tl"):
        if _is_excluded(tl_path, exclude_paths):
            continue
        if not _is_registry_table(tl_path):
            continue
        package_name = tl_path.parts[-4]
        table_name = tl_path.parts[-2]
        tables_by_pkg[package_name].add(table_name)
    return tables_by_pkg


ALL_TABLES_BY_PKG = _collect_all_registry_tables(BASE_PACKAGES, EXCLUDE_PATHS)


def _find_tables_in_testconds(
    packages_root: Path, exclude_paths: set[Path], tables_by_pkg: dict[str, set[str]]
) -> dict[str, set[str]]:
    all_names = [name for names in tables_by_pkg.values() for name in names]
    if not all_names:
        return defaultdict(set)
    pattern = re.compile(r"\b(?:" + "|".join(map(re.escape, all_names)) + r")\b")
    referenced = defaultdict(set)
    for tc_path in packages_root.rglob("test_conds_*.tc"):
        if _is_excluded(tc_path, exclude_paths):
            continue
        try:
            text = tc_path.read_text(encoding="utf-8")
        except Exception as exc:
            sys.stderr.write(f"Warning: Cannot read {tc_path}: {exc}\n")
            continue
        for match in pattern.finditer(text):
            found_name = match.group(0)
            for pkg, names in tables_by_pkg.items():
                if found_name in names:
                    referenced[pkg].add(found_name)
                    break
    return referenced


REFERENCED_TABLES = _find_tables_in_testconds(
    BASE_PACKAGES, EXCLUDE_PATHS, ALL_TABLES_BY_PKG
)


def _prepare_output(ref_tables: dict[str, set[str]]) -> dict[str, list[str]]:
    ordered = {}
    for pkg in sorted(ref_tables):
        if ref_tables[pkg]:
            ordered[pkg] = sorted(ref_tables[pkg])
    return ordered


FINAL_RESULT = _prepare_output(REFERENCED_TABLES)

print(json.dumps(FINAL_RESULT, ensure_ascii=False, indent=2))

with open("configs\\table_filters.json", "w", encoding="utf-8") as f:
    json.dump(FINAL_RESULT, f, ensure_ascii=False, indent=2)
