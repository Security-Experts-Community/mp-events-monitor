#!/usr/bin/env python3
"""
Объединенный скрипт для анализа knowledge base:
1. Поиск registry таблиц и их использование в test_conds
2. Поиск таблиц в .co файлах и построение маппинга
3. Анализ политик событий и correlation packs
"""

import argparse
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
from typing import Any

import yaml
from tqdm import tqdm

# ===================== КОНСТАНТЫ =====================
# Пути к конфигам НЕ зависят от текущей директории: инструмент запускают
# и из корня репозитория, и из tools/ (инцидент оператора 10.09:
# "No such file or directory: 'configs\\packages_names.json'").
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

try:  # Git Bash/перенаправление: иначе русский вывод ломается
    from envcheck import configure_console

    configure_console()
except ImportError:  # скрипт скопировали отдельно от репозитория
    pass

CONFIGS_DIR = REPO_ROOT / "configs"

# Корень локального репозитория knowledgebase. Приоритет источников:
# --kb-root  >  переменная окружения NOMOS_KB_ROOT  >  исторический путь.
DEFAULT_KB_ROOT = Path(os.environ.get("NOMOS_KB_ROOT") or r"D:\Work\repo\knowledgebase")
BASE_KB_ROOT = DEFAULT_KB_ROOT
BASE_PACKAGES = BASE_KB_ROOT / "packages"
EXCLUDE_CFG = BASE_KB_ROOT / "_extra" / "slices.yaml"


def set_kb_root(root) -> None:
    """Перенастраивает корень knowledgebase (вызывается из main до анализа)."""
    global BASE_KB_ROOT, BASE_PACKAGES, EXCLUDE_CFG
    BASE_KB_ROOT = Path(root).expanduser()
    BASE_PACKAGES = BASE_KB_ROOT / "packages"
    EXCLUDE_CFG = BASE_KB_ROOT / "_extra" / "slices.yaml"


# ВХОД анализа политик: исходные запросы к событиям (правит человек).
# Историческое имя event_policies_old.json читалось как "старая версия",
# из-за чего файл был удалён на этапе 0.1 вместе с мёртвыми артефактами;
# восстановлен из git под честным именем, старое поддержано как фолбэк.
POLICY_QUERIES = CONFIGS_DIR / "policy_queries.json"
LEGACY_POLICY_QUERIES = CONFIGS_DIR / "event_policies_old.json"

# Выходные файлы
OUTPUT_TABLE_FILTERS = CONFIGS_DIR / "table_filters.json"
OUTPUT_TABLE_MAPPING = CONFIGS_DIR / "table_mapping.json"
OUTPUT_EVENT_POLICIES = CONFIGS_DIR / "event_policies.json"
OUTPUT_PACKAGES_NAMES = CONFIGS_DIR / "packages_names.json"
OUTPUT_SUBRULES = CONFIGS_DIR / "subrules.json"


def policy_queries_path() -> Path:
    """Файл исходных запросов: новое имя, при его отсутствии — старое."""
    if not POLICY_QUERIES.exists() and LEGACY_POLICY_QUERIES.exists():
        return LEGACY_POLICY_QUERIES
    return POLICY_QUERIES

# Пакеты, которые не должны попадать в финальный результат
PACKS_ABOUT_MANY_SOFTS = {"bruteforce", "profiling", "remote_work"}

# ===================== ГЛОБАЛЬНЫЕ КЭШИ =====================
_EXCLUDE_PATHS_CACHE: set[Path] | None = None
_TABLE_PATH_CACHE: dict[str, Path] = {}
_YAML_PARSE_CACHE: dict[str, Any] = {}
_JSON_PARSE_CACHE: dict[str, Any] = {}

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


def safe_yaml_load(file_path: Path) -> dict | None:
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


def safe_json_load(file_path: Path) -> dict | None:
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


def load_excludes(cfg_path: Path | None = None) -> set[Path]:
    """Загружает список исключённых путей из YAML-конфига."""
    global _EXCLUDE_PATHS_CACHE
    # None, а не EXCLUDE_CFG в дефолте: значение по умолчанию связалось бы
    # на импорте — до того, как main() применит --kb-root.
    if cfg_path is None:
        cfg_path = EXCLUDE_CFG
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


def is_excluded(some_path: Path, exclude_paths: set[Path] | None = None) -> bool:
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


def build_table_path_cache() -> dict[str, Path]:
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


def find_table_path(table_name: str) -> Path | None:
    """Находит путь к таблице по её имени, используя кэш."""
    return _TABLE_PATH_CACHE.get(table_name)


def collect_all_registry_tables() -> dict[str, set[str]]:
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


def find_tables_in_testconds(tables_by_pkg: dict[str, set[str]]) -> dict[str, set[str]]:
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


def process_co_file(co_file_path: str, exclude_paths: set) -> tuple | None:
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


def extract_co_data(max_workers: int = 16) -> dict[str, list]:
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


# ===================== ИНДЕКС KNOWLEDGEBASE =====================
# Раньше на КАЖДЫЙ развёрнутый запрос политики (их 136) делалось два полных
# обхода репозитория knowledgebase, каждый — со своим пулом потоков и
# вложенным прогресс-баром: ~70 с на запрос, ~3 часа на прогон (репорт
# оператора 10.09). Репозиторий читается один раз, дальше запросы
# обслуживаются из памяти. Результат побитово тот же: кандидаты
# отбираются индексом, но подтверждаются тем же check_match.


class KbIndex:
    """Однократный слепок knowledgebase для анализа политик."""

    def __init__(self, root: Path, fields: set[str] | None):
        self.root = str(root)
        self.fields = fields
        # правила нормализации (*.js): обрезанные данные + их id
        self.norm_data: list[dict] = []
        self.norm_ids: list[str] = []
        # инвертированные индексы для отбора кандидатов
        self.by_field: dict[str, set[int]] = {}
        self.by_field_value: dict[tuple[str, str], set[int]] = {}
        # правило нормализации -> [(пакет, правило корреляции)]
        self.norm_to_rules: dict[str, list[tuple[str, str]]] = {}
        # metainfo.yaml правил корреляции -> связанные сабрулы (get_subrules)
        self.rule_relations: list[tuple[Path, dict]] = []


_POLICY_INDEX: KbIndex | None = None


def _read_json(path: Path):
    """Чтение без общего кэша: индекс хранит только нужные поля."""
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _read_yaml(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception:
        return None


def _collect_kb_files() -> tuple[list[Path], list[Path]]:
    """Единственный обход: *.js и metainfo.yaml папок с .co-правилами."""
    js_files: list[Path] = []
    meta_files: list[Path] = []
    for dirpath, _, filenames in os.walk(BASE_PACKAGES):
        folder = Path(dirpath)
        has_co = False
        for filename in filenames:
            if filename.endswith(".js"):
                js_files.append(folder / filename)
            elif filename.endswith(".co"):
                has_co = True
        if has_co and "metainfo.yaml" in filenames:
            meta_files.append(folder / "metainfo.yaml")
    return js_files, meta_files


def build_policy_index(fields: set[str] | None = None) -> KbIndex:
    """Строит (или возвращает готовый) индекс knowledgebase."""
    global _POLICY_INDEX
    ready = _POLICY_INDEX
    if ready is not None and ready.root == str(BASE_PACKAGES):
        # fields=None у вызывающего = «сгодится любой готовый индекс»
        if fields is None or ready.fields is None or fields <= ready.fields:
            return ready

    index = KbIndex(BASE_PACKAGES, fields)
    print("Индексация репозитория knowledgebase (однократно)...")
    js_files, meta_files = _collect_kb_files()
    print(f"Найдено правил нормализации: {len(js_files)}, правил корреляции: {len(meta_files)}")

    # Чтение файлов — операция дисковая, потоки здесь работают
    with ThreadPoolExecutor(max_workers=8) as executor:
        js_data = list(
            tqdm(
                executor.map(_read_json, js_files),
                total=len(js_files),
                desc="Правила нормализации",
                unit="файл",
            )
        )
        meta_data = list(
            tqdm(
                executor.map(_read_yaml, meta_files),
                total=len(meta_files),
                desc="Правила корреляции",
                unit="файл",
            )
        )

    for data in js_data:
        if not isinstance(data, dict):
            continue
        ident = data.get("id")
        if not ident:
            continue  # прототип отбрасывал записи без id
        trimmed = (
            data if fields is None else {f: data[f] for f in fields if f in data}
        )
        position = len(index.norm_ids)
        index.norm_ids.append(ident)
        index.norm_data.append(trimmed)
        for field, value in trimmed.items():
            index.by_field.setdefault(field, set()).add(position)
            index.by_field_value.setdefault(
                (field, str(value).lower()), set()
            ).add(position)

    for meta_path, meta in zip(meta_files, meta_data, strict=True):
        if not isinstance(meta, dict):
            continue
        relations = meta.get("ContentRelations")
        if not isinstance(relations, dict):
            continue
        auto = relations.get("Uses", {})
        auto = auto.get("SIEMKB", {}) if isinstance(auto, dict) else {}
        auto = auto.get("Auto", {}) if isinstance(auto, dict) else {}
        if not isinstance(auto, dict):
            continue
        norm_rules = auto.get("NormalizationRules")
        if isinstance(norm_rules, dict):
            package = get_relative_package_path(meta_path)
            if package:
                rule = meta_path.parent.name
                for norm_id in norm_rules.values():
                    index.norm_to_rules.setdefault(norm_id, []).append((package, rule))
        corr_rules = auto.get("CorrelationRules")
        if isinstance(corr_rules, dict):
            index.rule_relations.append((meta_path, corr_rules))

    print_success(
        f"Индекс построен: {len(index.norm_ids)} правил нормализации, "
        f"{len(index.norm_to_rules)} связей с пакетами"
    )
    _POLICY_INDEX = index
    return index


def find_matching_js_files_parallel(root_dir: Path, dictionary: dict) -> list[str]:
    """Правила нормализации, подходящие под запрос политики (по индексу)."""
    index = build_policy_index()
    candidates: set[int] | None = None
    for key, value in dictionary.items():
        present = index.by_field.get(key)
        if not present:
            return []  # поля нет ни в одном правиле -> check_match дал бы False
        if value is None or isinstance(value, bool):
            # прототип требовал только наличия ключа
            found = present
        elif isinstance(value, list):
            found = set()
            for item in value:
                found |= index.by_field_value.get((key, str(item).lower()), set())
        else:
            found = index.by_field_value.get((key, str(value).lower()), set())
        if not found:
            return []
        if candidates is None or len(found) < len(candidates):
            candidates = found
    if candidates is None:
        candidates = set(range(len(index.norm_ids)))  # пустой запрос
    return list(
        {
            index.norm_ids[i]
            for i in candidates
            if check_match(dictionary, index.norm_data[i])
        }
    )


def find_correlation_packs_parallel(
    root_dir: Path, nf_list: list[str]
) -> tuple[list[str], dict]:
    """Пакеты и правила корреляции, зависящие от этих правил нормализации."""
    index = build_policy_index()
    packs: list[str] = []
    dependent_corrs: dict[str, list[str]] = {}
    for norm_id in nf_list:
        for package, rule in index.norm_to_rules.get(norm_id, ()):
            packs.append(package)
            dependent_corrs.setdefault(package, []).append(rule)
    return list(set(packs)), dependent_corrs


def process_policy_item(args: tuple) -> tuple[str, list[str], dict]:
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
    policies_path = policy_queries_path()
    if not policies_path.exists():
        print_error(f"Файл исходных запросов политик не найден: {policies_path}")
        print_warning(
            "Восстановить из истории git: git show "
            "837f37d:configs/event_policies_old.json > configs/policy_queries.json"
        )
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
    with policies_path.open("w", encoding="utf-8") as f:
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

    # Индекс строится один раз на все политики; после него обработка одной
    # политики — операции со словарями, пул потоков только мешал бы (GIL).
    query_fields = {
        field
        for policy in policies.values()
        for query in policy.get("queries") or []
        for field in query
    }
    build_policy_index(query_fields)

    separate_dict = {}

    for key in tqdm(policies, desc="Обработка политик", unit="политика"):
        try:
            policy_key, packs, found_deps = process_policy_item(
                (key, policies[key], BASE_PACKAGES)
            )
            policies[policy_key]["KB_packs"] = packs
            separate_dict[policy_key] = found_deps
        except Exception as e:
            print_error(f"Ошибка обработки политики {key}: {e}")

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
    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)

    with policies_path.open("w", encoding="utf-8") as f:
        json.dump(policies, f, indent=4, ensure_ascii=False)

    with OUTPUT_EVENT_POLICIES.open("w", encoding="utf-8") as f:
        json.dump(separate_dict, f, indent=4, ensure_ascii=False)

    print_success(f"Результаты сохранены в {policies_path} и {OUTPUT_EVENT_POLICIES}")


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

    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_TABLE_FILTERS.open("w", encoding="utf-8") as f:
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

    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_TABLE_MAPPING.open("w", encoding="utf-8") as f:
        json.dump(mapping_result, f, indent=4, ensure_ascii=False)

    print_success(f"Результат сохранён в {OUTPUT_TABLE_MAPPING}")
    return mapping_result


def get_subrules():
    """Сабрулы -> правила пакетов; локализация имён пакетов в event_policies.

    Раньше пути к knowledgebase и configs были захардкожены строками Windows
    (D:\\Work\\repo\\knowledgebase, "configs\\packages_names.json"), а имя
    правила/пакета вычислялось split("\\") — на Linux и при запуске не из
    корня репозитория это не работало вовсе.
    """
    cfg = safe_yaml_load(EXCLUDE_CFG) or {}
    try:
        file_blacklist = [
            item.replace("packages/", "")
            for item in cfg["KnowledgebaseSlices"]["SIEM-Public"]["Excludes"]["Files"]
        ]
    except (KeyError, TypeError):
        print_warning(f"Список исключений не найден в {EXCLUDE_CFG}")
        file_blacklist = []
    print(f"Исключённых пакетов: {len(file_blacklist)}")

    packs_names = safe_json_load(OUTPUT_PACKAGES_NAMES)
    if packs_names is None:
        print_error(f"Не найден файл локализации пакетов: {OUTPUT_PACKAGES_NAMES}")
        return
    queries = safe_json_load(OUTPUT_EVENT_POLICIES)
    if queries is None:
        print_error(f"Не найден {OUTPUT_EVENT_POLICIES} (сначала analyze_policies)")
        return

    # Правила корреляции берём из индекса: репозиторий уже прочитан
    index = build_policy_index()
    subrules_to_rules: dict[str, dict[str, list[str]]] = {}
    for meta_path, dependencies in index.rule_relations:
        if any(bad and bad in meta_path.parent.as_posix() for bad in file_blacklist):
            continue
        # <пакет>/correlation_rules/<правило>/metainfo.yaml
        curr_rule = meta_path.parent.name
        curr_pack = meta_path.parents[2].name
        for subrule in dependencies:
            name = dependencies[subrule]
            pack_rules = subrules_to_rules.setdefault(name, {}).setdefault(
                curr_pack, []
            )
            if curr_rule not in pack_rules:
                pack_rules.append(curr_rule)

    with OUTPUT_SUBRULES.open("w", encoding="utf-8") as f_out:
        json.dump(subrules_to_rules, f_out, indent=4, ensure_ascii=False)
    print_success(f"Результат сохранён в {OUTPUT_SUBRULES}")

    # Правило, использующее сабрул, добавляется в тот же пакет.
    # BUGCOMPAT: список правил дополняется ПО ХОДУ итерации по нему же —
    # так раскрытие получается транзитивным (сабрул сабрула). Поведение
    # прототипа сохранено намеренно: от него зависят готовые конфиги.
    for policy in queries.values():
        for pack_rules in policy.values():
            for pack, rules in pack_rules.items():
                for rule in rules:
                    for pack_name, corr_rules in subrules_to_rules.get(rule, {}).items():
                        if pack_name != pack:
                            continue
                        for corr in corr_rules:
                            if corr not in rules:
                                rules.append(corr)

    # Локализация имён пакетов + удаление исключённых
    localized: dict = {}
    for policy_name, policy in queries.items():
        localized[policy_name] = {}
        for query, packs in policy.items():
            localized[policy_name][query] = {
                localize_pack(pack, packs_names): rules for pack, rules in packs.items()
            }
            for bad in file_blacklist:
                localized[policy_name][query].pop(bad, None)

    localized = sort_lists_in_structure(localized)
    with OUTPUT_EVENT_POLICIES.open("w", encoding="utf-8") as f_out:
        json.dump(localized, f_out, indent=4, ensure_ascii=False)
    print_success(f"Результат сохранён в {OUTPUT_EVENT_POLICIES}")


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description="Генератор конфигов Nomos из локального репозитория knowledgebase "
        "(офлайн dev-инструмент, в веб-приложение не входит)",
    )
    parser.add_argument(
        "--kb-root",
        type=Path,
        default=None,
        metavar="ПУТЬ",
        help=f"корень репозитория knowledgebase (по умолчанию {DEFAULT_KB_ROOT}; "
        "также читается из переменной окружения NOMOS_KB_ROOT)",
    )
    parser.add_argument(
        "--debug", action="store_true", help="однопоточный режим для отладки"
    )
    only = parser.add_mutually_exclusive_group()
    only.add_argument("--tables-only", action="store_true", help="только table_filters")
    only.add_argument("--co-only", action="store_true", help="только table_mapping")
    only.add_argument(
        "--policies-only", action="store_true", help="только event_policies и subrules"
    )
    args = parser.parse_args()

    start_time = time.time()
    print_header("ОБЪЕДИНЕННЫЙ АНАЛИЗ KNOWLEDGE BASE")

    if args.kb_root is not None:
        set_kb_root(args.kb_root)
    if args.debug:
        print_warning("Запуск в отладочном режиме (однопоточном)")

    run_all = not (args.tables_only or args.co_only or args.policies_only)
    print(f"Репозиторий knowledgebase: {BASE_KB_ROOT}")
    print(f"Конфигурация Nomos:        {CONFIGS_DIR}")

    if not BASE_PACKAGES.is_dir():
        print_error(f"Пакеты knowledgebase не найдены: {BASE_PACKAGES}")
        print_warning(
            "Укажите корень репозитория: "
            "python tools/kb_config_generator.py --kb-root <путь> "
            "(или задайте переменную окружения NOMOS_KB_ROOT)"
        )
        return 1

    try:
        if run_all or args.tables_only:
            run_table_analysis()

        if run_all or args.co_only:
            # Для .co анализа нужен кэш таблиц
            if not _TABLE_PATH_CACHE:
                build_table_path_cache()
            run_co_analysis()

        if run_all or args.policies_only:
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
