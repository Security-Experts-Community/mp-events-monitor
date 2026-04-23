#!/usr/bin/env python3
"""
Объединенный скрипт для анализа knowledge base:
1. Поиск registry таблиц и их использование в test_conds
2. Поиск таблиц в .co файлах и построение маппинга
3. Анализ политик событий и correlation packs
"""

import concurrent.futures
import itertools
import json
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from tqdm import tqdm

# ===================== КОНСТАНТЫ =====================
BASE_KB_ROOT = Path(r"D:\Work\repo\knowledgebase")
BASE_PACKAGES = BASE_KB_ROOT / "packages"
EXCLUDE_CFG = BASE_KB_ROOT / "_extra" / "slices.yaml"

# Выходные файлы
OUTPUT_TABLE_FILTERS = Path("configs/table_filters.json")
OUTPUT_TABLE_MAPPING = Path("configs/table_mapping.json")
OUTPUT_EVENT_POLICIES = Path("configs/event_policies.json")
OUTPUT_EVENT_POLICIES_OLD = Path("configs/event_policies_old.json")
OUTPUT_PACKAGES_NAMES = Path("configs/packages_names.json")

# Пакеты, которые не должны попадать в финальный результат
PACKS_ABOUT_MANY_SOFTS = {"bruteforce", "profiling", "remote_work"}

# ===================== ГЛОБАЛЬНЫЕ КЭШИ =====================
_EXCLUDE_PATHS_CACHE: Optional[set[Path]] = None
_TABLE_PATH_CACHE: Dict[str, Path] = {}
_YAML_PARSE_CACHE: Dict[str, Any] = {}
_JSON_PARSE_CACHE: Dict[str, Any] = {}

# Блокировки для потокобезопасности
results_lock = Lock()
cache_lock = Lock()

# ===================== УТИЛИТЫ =====================


def sort_lists_in_structure(data):
    if isinstance(data, dict):
        return {key: sort_lists_in_structure(value) for key, value in data.items()}
    elif isinstance(data, list):
        sorted_list = sorted(data)
        return [sort_lists_in_structure(item) for item in sorted_list]
    else:
        return data


def setup_logging():
    """Настройка цветного логирования"""
    try:
        from colorama import Fore, Style, init

        init()
        return Fore, Style
    except ImportError:
        # Заглушки если colorama не установлена
        class Dummy:
            def __getattr__(self, name):
                return ""

        return Dummy(), Dummy()


Fore, Style = setup_logging()


def print_header(text: str):
    """Печать заголовка"""
    print(f"\n{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")


def print_success(text: str):
    """Печать успешного сообщения"""
    print(f"{Fore.GREEN}✓ {text}{Style.RESET_ALL}")


def print_warning(text: str):
    """Печать предупреждения"""
    print(f"{Fore.YELLOW}⚠ {text}{Style.RESET_ALL}")


def print_error(text: str):
    """Печать ошибки"""
    print(f"{Fore.RED}✗ {text}{Style.RESET_ALL}")


def safe_yaml_load(file_path: Path) -> Optional[dict]:
    """Безопасная загрузка YAML с кэшированием"""
    path_str = str(file_path)

    # Проверяем кэш
    if path_str in _YAML_PARSE_CACHE:
        return _YAML_PARSE_CACHE[path_str]

    if not file_path.exists():
        return None

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        # Сохраняем в кэш
        with cache_lock:
            _YAML_PARSE_CACHE[path_str] = data

        return data
    except Exception:
        return None


def safe_json_load(file_path: Path) -> Optional[dict]:
    """Безопасная загрузка JSON с кэшированием"""
    path_str = str(file_path)

    # Проверяем кэш
    if path_str in _JSON_PARSE_CACHE:
        return _JSON_PARSE_CACHE[path_str]

    if not file_path.exists():
        return None

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        # Сохраняем в кэш
        with cache_lock:
            _JSON_PARSE_CACHE[path_str] = data

        return data
    except Exception:
        return None


def clear_caches():
    """Очистка всех кэшей"""
    global _EXCLUDE_PATHS_CACHE, _TABLE_PATH_CACHE, _YAML_PARSE_CACHE, _JSON_PARSE_CACHE
    _EXCLUDE_PATHS_CACHE = None
    _TABLE_PATH_CACHE.clear()
    _YAML_PARSE_CACHE.clear()
    _JSON_PARSE_CACHE.clear()


# ===================== ФУНКЦИИ ДЛЯ РАБОТЫ С ИСКЛЮЧЕНИЯМИ =====================


def load_excludes(cfg_path: Path = EXCLUDE_CFG) -> set[Path]:
    """Загружает список исключённых путей из YAML-конфига."""
    global _EXCLUDE_PATHS_CACHE
    if _EXCLUDE_PATHS_CACHE is not None:
        return _EXCLUDE_PATHS_CACHE

    if not cfg_path.is_file():
        print_warning(f"{cfg_path} не найден → нет исключений")
        _EXCLUDE_PATHS_CACHE = set()
        return _EXCLUDE_PATHS_CACHE

    cfg = safe_yaml_load(cfg_path)
    if not cfg:
        _EXCLUDE_PATHS_CACHE = set()
        return _EXCLUDE_PATHS_CACHE

    try:
        file_list = cfg["KnowledgebaseSlices"]["SIEM-Public"]["Excludes"]["Files"]
    except Exception:
        print_warning("Список исключений не найден в конфиге")
        _EXCLUDE_PATHS_CACHE = set()
        return _EXCLUDE_PATHS_CACHE

    excludes = set()
    for entry in file_list or []:
        rel = Path(entry).as_posix().strip("/")
        if rel:
            excludes.add(BASE_KB_ROOT / rel)

    _EXCLUDE_PATHS_CACHE = excludes
    print_success(f"Загружено {len(excludes)} исключённых путей")
    return excludes


def is_excluded(some_path: Path, exclude_paths: Optional[set[Path]] = None) -> bool:
    """Проверяет, исключён ли путь."""
    if exclude_paths is None:
        exclude_paths = load_excludes()

    # Быстрая проверка через as_posix для сравнения
    path_str = some_path.as_posix()
    for excl in exclude_paths:
        if path_str.startswith(excl.as_posix()):
            return True
    return False


def get_relative_package_path(full_path: Path) -> str:
    """Получает относительный путь пакета от packages"""
    try:
        parts = full_path.parts
        packages_idx = parts.index("packages")
        if packages_idx + 1 < len(parts):
            return parts[packages_idx + 1]
    except (ValueError, IndexError):
        pass
    return ""


# ===================== ФУНКЦИИ ДЛЯ АНАЛИЗА ТАБЛИЦ (Скрипт 1) =====================


@lru_cache(maxsize=2048)
def is_registry_table(tl_path: str) -> bool:
    """
    Оптимизированная проверка registry таблицы с кэшированием.
    Принимает строку для возможности кэширования.
    """
    path = Path(tl_path)
    data = safe_yaml_load(path)

    if not data or not isinstance(data, dict):
        return False

    if data.get("fillType") != "Registry":
        return False

    has_defaults_pt = (
        isinstance(data.get("defaults"), dict) and "PT" in data["defaults"]
    )
    path_contains_flag = any(
        word in path.as_posix().lower() for word in ("whitelist", "blacklist")
    )

    return not has_defaults_pt or path_contains_flag


@lru_cache(maxsize=2048)
def check_fill_type_strict(tl_path: str) -> bool:
    """
    Проверка fillType для Registry/AssetGrid с пустым defaults.
    Принимает строку для кэширования.
    """
    path = Path(tl_path)
    data = safe_yaml_load(path)

    if not data or not isinstance(data, dict):
        return False

    fill_type = data.get("fillType")
    if fill_type not in ("Registry", "AssetGrid"):
        return False

    defaults = data.get("defaults")
    # Если defaults есть - должен быть пустым словарём
    if defaults is not None:
        return isinstance(defaults, dict) and len(defaults) == 0
    return True


def build_table_path_cache() -> Dict[str, Path]:
    """Предварительно строит кэш путей ко всем таблицам."""
    global _TABLE_PATH_CACHE
    _TABLE_PATH_CACHE.clear()

    exclude_paths = load_excludes()

    for tl_path in tqdm(
        list(BASE_PACKAGES.rglob("*/tabular_lists/*/table.tl")),
        desc="Построение кэша таблиц",
        unit="файлов",
    ):
        if is_excluded(tl_path, exclude_paths):
            continue
        table_name = tl_path.parts[-2]
        _TABLE_PATH_CACHE[table_name] = tl_path

    print_success(f"Построен кэш для {len(_TABLE_PATH_CACHE)} таблиц")
    return _TABLE_PATH_CACHE


def find_table_path(table_name: str) -> Optional[Path]:
    """Находит путь к таблице по её имени, используя кэш."""
    return _TABLE_PATH_CACHE.get(table_name)


def collect_all_registry_tables() -> Dict[str, Set[str]]:
    """Собирает все registry таблицы по пакетам."""
    exclude_paths = load_excludes()
    tables_by_pkg = defaultdict(set)

    for tl_path in tqdm(
        list(BASE_PACKAGES.rglob("*/tabular_lists/*/table.tl")),
        desc="Сбор registry таблиц",
        unit="файлов",
    ):
        if is_excluded(tl_path, exclude_paths):
            continue

        if is_registry_table(str(tl_path)):
            package_name = tl_path.parts[-4]
            table_name = tl_path.parts[-2]
            tables_by_pkg[package_name].add(table_name)

            # Кэшируем путь к таблице
            _TABLE_PATH_CACHE[table_name] = tl_path

    total_tables = sum(len(v) for v in tables_by_pkg.values())
    print_success(f"Найдено registry таблиц: {total_tables}")

    return dict(tables_by_pkg)


def find_tables_in_testconds(tables_by_pkg: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """Находит использование таблиц в test_conds файлах."""
    exclude_paths = load_excludes()

    # Оптимизация: собираем все имена таблиц для одного regex
    all_names = [name for names in tables_by_pkg.values() for name in names]
    if not all_names:
        return defaultdict(set)

    # Компилируем один regex для всех имён
    pattern = re.compile(r"\b(?:" + "|".join(map(re.escape, all_names)) + r")\b")
    referenced = defaultdict(set)

    # Собираем все test_conds файлы
    tc_files = list(BASE_PACKAGES.rglob("test_conds_*.tc"))

    for tc_path in tqdm(tc_files, desc="Поиск в test_conds", unit="файлов"):
        if is_excluded(tc_path, exclude_paths):
            continue

        try:
            text = tc_path.read_text(encoding="utf-8")
        except Exception:
            continue

        for match in pattern.finditer(text):
            found_name = match.group(0)
            for pkg, names in tables_by_pkg.items():
                if found_name in names:
                    referenced[pkg].add(found_name)
                    break

    total_referenced = sum(len(v) for v in referenced.values())
    print_success(f"Найдено использований в test_conds: {total_referenced}")

    return dict(referenced)


def process_co_file(co_file_path: str, exclude_paths: set) -> Optional[tuple]:
    """
    Обрабатывает один .co файл (для потоков).
    """
    try:
        co_path = Path(co_file_path)
        root = co_path.parent
        yaml_path = root / "metainfo.yaml"

        if not yaml_path.exists():
            return None

        # Проверка исключений
        if is_excluded(yaml_path, exclude_paths):
            return None

        # Чтение YAML
        data = safe_yaml_load(yaml_path)
        if not data or not isinstance(data, dict):
            return None

        content_auto_name = data.get("ContentAutoName")
        if not content_auto_name:
            return None

        # Извлечение TabularLists
        tabular_lists = None

        # Пробуем разные варианты структуры
        try:
            # Новый формат
            content_relations = data.get("content_relations", {})
            if isinstance(content_relations, dict):
                uses = content_relations.get("uses", {})
                if isinstance(uses, dict):
                    siemkb = uses.get("siemkb", {})
                    if isinstance(siemkb, dict):
                        auto_section = siemkb.get("auto", {})
                        if isinstance(auto_section, dict):
                            tabular_lists = auto_section.get("tabular_lists")

            # Старый формат
            if tabular_lists is None:
                auto_section = (
                    data.get("ContentRelations", {})
                    .get("Uses", {})
                    .get("SIEMKB", {})
                    .get("Auto", {})
                )
                if isinstance(auto_section, dict):
                    tabular_lists = auto_section.get("TabularLists")

        except (AttributeError, TypeError):
            pass

        if not isinstance(tabular_lists, dict):
            return None

        # Формирование результата
        result_list = []
        for value in tabular_lists.values():
            if isinstance(value, str):
                table_path = find_table_path(value)
                if table_path and check_fill_type_strict(str(table_path)):
                    result_list.append({value: "No_manual_changes"})

        if result_list:
            return (content_auto_name, result_list)

    except Exception:
        pass

    return None


def extract_co_data(max_workers: int = 16) -> Dict[str, List]:
    """
    Многопоточное извлечение данных из .co файлов.
    """
    exclude_paths = load_excludes()

    # Собираем все .co файлы
    print("Сканирование .co файлов...")
    co_files = []
    for root, _, files in os.walk(str(BASE_PACKAGES)):
        for file in files:
            if file.endswith(".co"):
                co_files.append(str(Path(root) / file))

    print(f"Найдено {len(co_files)} .co файлов")

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_co_file, co_file, exclude_paths): co_file
            for co_file in co_files
        }

        for future in tqdm(
            as_completed(future_to_file),
            total=len(co_files),
            desc="Обработка .co файлов",
            unit="файл",
        ):
            try:
                result = future.result(timeout=10)
                if result:
                    content_name, result_list = result
                    with results_lock:
                        results[content_name] = result_list
            except Exception:
                pass

    print_success(f"Найдено записей в .co файлах: {len(results)}")
    return results


# ===================== ФУНКЦИИ ДЛЯ АНАЛИЗА ПОЛИТИК (Скрипт 2) =====================


def dict_to_query_string(query_dict: dict) -> str:
    """Преобразует словарь в строку запроса"""
    conditions = []

    for key, value in query_dict.items():
        if value is None:
            conditions.append(f"not {key}")
        elif value is True:
            conditions.append(f"{key}")
        elif isinstance(value, list):
            if value:
                conditions.append(f'{key} = "{value[0]}"')
            else:
                conditions.append(f"not {key}")
        else:
            conditions.append(f'{key} = "{value}"')

    return " and ".join(conditions)


def transform_queries(data: dict) -> dict:
    """Трансформирует запросы, разворачивая списки"""
    transformed = {}
    for key, value in data.items():
        new_queries = []
        for query in value["queries"]:
            # Находим все поля, которые являются списками
            list_fields = {}
            single_fields = {}

            for field, field_value in query.items():
                if isinstance(field_value, list):
                    list_fields[field] = field_value
                else:
                    single_fields[field] = field_value

            # Если нет полей-списков, просто добавляем запрос как есть
            if not list_fields:
                new_queries.append(query.copy())
                continue

            # Создаем все комбинации значений из полей-списков
            field_names = list(list_fields.keys())
            field_values = list(list_fields.values())

            for combination in itertools.product(*field_values):
                new_query = single_fields.copy()
                for i, field_name in enumerate(field_names):
                    new_query[field_name] = [combination[i]]
                new_queries.append(new_query)

        transformed[key] = {"queries": new_queries}
    return transformed


def localize_pack(pack: str, loc_dict: dict) -> str:
    """Локализует имя пакета"""
    for item in loc_dict.get("categories", []):
        if item["id"] == pack:
            return item["name"]
    return pack


def check_match(dict1: dict, dict2: dict) -> bool:
    """Проверяет соответствие словаря шаблону"""
    for key, value in dict1.items():
        if key not in dict2:
            return False
        if value is not None:
            if isinstance(value, list):
                if not any(
                    str(item).lower() == str(dict2[key]).lower() for item in value
                ):
                    return False
            else:
                if isinstance(value, bool):
                    continue
                if str(value).lower() != str(dict2[key]).lower():
                    return False
    return True


def find_matching_js_files_parallel(root_dir: Path, dictionary: dict) -> List[str]:
    """Многопоточный поиск совпадающих JS файлов"""
    js_files = []

    # Сбор всех JS файлов
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".js"):
                js_files.append(Path(dirpath) / filename)

    matching_paths = []
    lock = Lock()

    def process_file(filepath: Path):
        try:
            data = safe_json_load(filepath)
            if data and check_match(dictionary, data):
                result = data.get("id")
                if result:
                    with lock:
                        matching_paths.append(result)
        except Exception:
            pass

    # Многопоточная обработка
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            tqdm(
                executor.map(process_file, js_files),
                total=len(js_files),
                desc="Поиск в JS файлах",
                leave=False,
            )
        )

    return list(set(matching_paths))


def find_correlation_packs_parallel(
    root_dir: Path, nf_list: List[str]
) -> Tuple[List[str], dict]:
    """Многопоточный поиск correlation packs"""
    co_files = []
    dependent_corrs = {}

    # Сбор всех .co файлов
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".co"):
                filepath = Path(dirpath) / "metainfo.yaml"
                if filepath.exists():
                    co_files.append(filepath)

    packs = []
    lock = Lock()

    def process_co_file(filepath: Path):
        try:
            rule_meta = safe_yaml_load(filepath)
            if not rule_meta:
                return

            curr_rule = filepath.parent.name

            if "ContentRelations" in rule_meta:
                try:
                    nf_dict = rule_meta["ContentRelations"]["Uses"]["SIEMKB"]["Auto"][
                        "NormalizationRules"
                    ]
                    for nf_value in nf_dict.values():
                        if nf_value in nf_list:
                            package = get_relative_package_path(filepath)
                            if package:
                                with lock:
                                    packs.append(package)
                                    if package in dependent_corrs:
                                        dependent_corrs[package].append(curr_rule)
                                    else:
                                        dependent_corrs[package] = [curr_rule]
                except (KeyError, TypeError):
                    pass
        except Exception:
            pass

    # Многопоточная обработка
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            tqdm(
                executor.map(process_co_file, co_files),
                total=len(co_files),
                desc="Поиск correlation packs",
                leave=False,
            )
        )

    return list(set(packs)), dependent_corrs


def process_policy_item(args: tuple) -> Tuple[str, List[str], dict]:
    """Обработка одного элемента политики для многопоточного выполнения"""
    policy_key, policy_data, root_directory = args

    if policy_data.get("queries"):
        all_packs = []
        all_deps = {}

        for item in policy_data["queries"]:
            norms = find_matching_js_files_parallel(root_directory, item)
            packs, deps_for_item = find_correlation_packs_parallel(
                root_directory, norms
            )
            all_packs.extend(packs)
            all_deps[dict_to_query_string(item)] = {
                key: list(set(value)) for key, value in deps_for_item.items()
            }

        return policy_key, list(set(all_packs)), all_deps

    return policy_key, [], {}


def analyze_policies() -> None:
    """Анализ политик событий"""
    print_header("Анализ политик событий")

    # Загрузка политик
    policies_path = OUTPUT_EVENT_POLICIES_OLD
    if not policies_path.exists():
        print_error(f"Файл политик не найден: {policies_path}")
        return

    policies = safe_json_load(policies_path)
    if not policies:
        print_error("Не удалось загрузить политики")
        return

    # Трансформация запросов
    print("Трансформация запросов...")
    policies_transformed = transform_queries(policies)
    for key in policies:
        policies[key]["queries"] = policies_transformed[key]["queries"]

    # Сохраняем трансформированные политики
    with open(OUTPUT_EVENT_POLICIES_OLD, "w", encoding="utf-8") as f:
        json.dump(policies, f, indent=4, ensure_ascii=False)

    # Загрузка имен пакетов
    packs_names = safe_json_load(OUTPUT_PACKAGES_NAMES) or {}

    # Загрузка черного списка
    file_blacklist = []
    cfg = safe_yaml_load(EXCLUDE_CFG)
    if cfg:
        try:
            blacklist = cfg["KnowledgebaseSlices"]["SIEM-Public"]["Excludes"]["Files"]
            file_blacklist = [item.replace("packages/", "") for item in blacklist]
        except Exception:
            pass

    print(f"Загружено {len(file_blacklist)} исключённых путей")

    total_policies = len(policies)
    print(f"Всего политик: {total_policies}")

    # Подготовка аргументов для многопоточной обработки
    policy_args = [(key, policies[key], BASE_PACKAGES) for key in policies.keys()]

    separate_dict = {}

    # Многопоточная обработка политик
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(process_policy_item, arg) for arg in policy_args]

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Обработка политик",
            unit="политика",
        ):
            try:
                policy_key, packs, found_deps = future.result(timeout=60)
                policies[policy_key]["KB_packs"] = packs
                separate_dict[policy_key] = found_deps
            except Exception as e:
                print_error(f"Ошибка обработки политики: {e}")

    # Фильтрация и локализация пакетов
    for policy_key in policies:
        if "KB_packs" in policies[policy_key]:
            temp = policies[policy_key]["KB_packs"]
            policies[policy_key]["KB_packs"] = [
                item
                for item in temp
                if item not in file_blacklist and item not in PACKS_ABOUT_MANY_SOFTS
            ]
            for i in range(len(policies[policy_key]["KB_packs"])):
                policies[policy_key]["KB_packs"][i] = localize_pack(
                    policies[policy_key]["KB_packs"][i], packs_names
                )

    # Сохранение результатов
    OUTPUT_EVENT_POLICIES_OLD.parent.mkdir(exist_ok=True)

    with open(OUTPUT_EVENT_POLICIES_OLD, "w", encoding="utf-8") as f:
        json.dump(policies, f, indent=4, ensure_ascii=False)

    with open(OUTPUT_EVENT_POLICIES, "w", encoding="utf-8") as f:
        json.dump(separate_dict, f, indent=4, ensure_ascii=False)

    print_success(
        f"Результаты сохранены в {OUTPUT_EVENT_POLICIES_OLD} и {OUTPUT_EVENT_POLICIES}"
    )


# ===================== ОСНОВНАЯ ФУНКЦИЯ =====================


def run_table_analysis():
    """Запуск анализа таблиц (часть 1)"""
    print_header("Часть 1: Поиск registry таблиц")

    tables_by_pkg = collect_all_registry_tables()
    referenced_tables = find_tables_in_testconds(tables_by_pkg)

    # Подготовка и сохранение результата
    final_result = {}
    for pkg in sorted(referenced_tables):
        if referenced_tables[pkg]:
            final_result[pkg] = sorted(referenced_tables[pkg])

    OUTPUT_TABLE_FILTERS.parent.mkdir(exist_ok=True)
    with open(OUTPUT_TABLE_FILTERS, "w", encoding="utf-8") as f:
        json.dump(final_result, f, ensure_ascii=False, indent=2)

    print_success(f"Результат сохранён в {OUTPUT_TABLE_FILTERS}")
    return final_result


def run_co_analysis():
    """Запуск анализа .co файлов (часть 2)"""
    print_header("Часть 2: Поиск таблиц в .co файлах")

    # Убедимся, что кэш таблиц построен
    if not _TABLE_PATH_CACHE:
        build_table_path_cache()

    mapping_result = extract_co_data(max_workers=16)

    OUTPUT_TABLE_MAPPING.parent.mkdir(exist_ok=True)
    with open(OUTPUT_TABLE_MAPPING, "w", encoding="utf-8") as f:
        json.dump(mapping_result, f, indent=4, ensure_ascii=False)

    print_success(f"Результат сохранён в {OUTPUT_TABLE_MAPPING}")
    return mapping_result


def get_subrules():
    with open(r"D:\Work\repo\knowledgebase\_extra\slices.yaml") as exclude_file:
        cfg = yaml.safe_load(exclude_file)
        file_blacklist = cfg["KnowledgebaseSlices"]["SIEM-Public"]["Excludes"]["Files"]
        for i in range(len(file_blacklist)):
            file_blacklist[i] = file_blacklist[i].replace("packages/", "")

    with open("configs\\packages_names.json", "r", encoding="utf-8") as f_packs:
        packs_names = json.load(f_packs)

    print(file_blacklist)

    meta_corr = []

    for dirpath, dirnames, filenames in os.walk(
        "D:\\Work\\repo\\knowledgebase\\packages"
    ):
        for filename in filenames:
            if filename.endswith(".co"):
                need_insert = True
                for bad_pack in file_blacklist:
                    if bad_pack in dirpath:
                        need_insert = False
                if need_insert:
                    filepath = os.path.join(dirpath, "metainfo.yaml")
                    if os.path.exists(filepath):
                        meta_corr.append(filepath)

    subrules_to_rules = {}
    for item in meta_corr:
        curr_rule = item.split("\\")[-2]
        curr_pack = item.split("\\")[-4]
        with open(item, "r", encoding="utf-8") as f:
            rule_meta = yaml.safe_load(f)

        if "ContentRelations" in rule_meta:
            try:
                dependencies = rule_meta["ContentRelations"]["Uses"]["SIEMKB"]["Auto"][
                    "CorrelationRules"
                ]
                # print(rule_meta["ContentRelations"]["Uses"]["SIEMKB"]["Auto"]["CorrelationRules"])
                for subrule in dependencies:
                    tmp = dependencies[subrule]
                    if tmp not in subrules_to_rules.keys():
                        subrules_to_rules[tmp] = {curr_pack: [curr_rule]}
                    else:
                        if curr_pack in subrules_to_rules[tmp].keys():
                            subrules_to_rules[tmp][curr_pack].append(curr_rule)
                        else:
                            subrules_to_rules[tmp][curr_pack] = [curr_rule]
            except:
                pass

    with open("configs\\subrules.json", "w", encoding="utf-8") as f_out:
        f_out.write(json.dumps(subrules_to_rules, indent=4, ensure_ascii=False))

    with open("configs\\event_policies.json", "r", encoding="utf-8") as f_in:
        queries = json.load(f_in)

    queries_copy = queries

    for item in queries:
        for filter in queries[item]:
            for pack in queries[item][filter]:
                for rule in queries[item][filter][pack]:
                    if rule in subrules_to_rules.keys():
                        for pack_name in subrules_to_rules[rule]:
                            if pack_name == pack:
                                for corr in subrules_to_rules[rule][pack_name]:
                                    if corr not in queries_copy[item][filter][pack]:
                                        queries_copy[item][filter][pack].append(corr)

    queries_copy = {}
    for item in queries:
        queries_copy[item] = queries[item].copy()
        queries_copy[item] = {}

        for filter in queries[item]:
            queries_copy[item][filter] = {}
            print(file_blacklist)

            for pack in queries[item][filter]:
                new_pack = localize_pack(pack, packs_names)
                queries_copy[item][filter][new_pack] = queries[item][filter][pack]

            for bad in file_blacklist:
                if bad in queries_copy[item][filter].keys():
                    del queries_copy[item][filter][bad]

    queries_copy = sort_lists_in_structure(queries_copy)

    with open("configs\\event_policies.json", "w", encoding="utf-8") as f_out_2:
        queries = f_out_2.write(json.dumps(queries_copy, indent=4, ensure_ascii=False))


def main():
    """Основная функция"""
    start_time = time.time()

    print_header("ОБЪЕДИНЕННЫЙ АНАЛИЗ KNOWLEDGE BASE")

    # Парсинг аргументов командной строки
    run_all = True
    debug_mode = False

    if len(sys.argv) > 1:
        if sys.argv[1] == "--debug":
            debug_mode = True
            print_warning("Запуск в отладочном режиме (однопоточном)")
        elif sys.argv[1] == "--tables-only":
            run_all = False
            print("Запуск только анализа таблиц")
        elif sys.argv[1] == "--co-only":
            run_all = False
            print("Запуск только анализа .co файлов")
        elif sys.argv[1] == "--policies-only":
            run_all = False
            print("Запуск только анализа политик")

    try:
        # Выполнение анализа
        if run_all or sys.argv[1] == "--tables-only":
            run_table_analysis()

        if run_all or sys.argv[1] == "--co-only":
            # Для .co анализа нужен кэш таблиц
            if not _TABLE_PATH_CACHE:
                build_table_path_cache()
            run_co_analysis()

        if run_all or sys.argv[1] == "--policies-only":
            analyze_policies()
            get_subrules()

        elapsed = time.time() - start_time
        print_header(f"АНАЛИЗ ЗАВЕРШЕН ЗА {elapsed:.2f} СЕКУНД")

    except KeyboardInterrupt:
        print_warning("\nАнализ прерван пользователем")
    except Exception as e:
        print_error(f"Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
