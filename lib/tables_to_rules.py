import json
import os
from pathlib import Path

import yaml


def check_fill_type_strict(directory_path, target_folder_name):
    """
    Строгая версия с дополнительными проверками.
    """
    directory = Path(directory_path)

    if not directory.exists() or not directory.is_dir():
        return False

    found_folders = list(directory.rglob(target_folder_name))

    if not found_folders:
        return False

    target_folder = found_folders[0]

    if not target_folder.is_dir():
        return False

    table_file = target_folder / "table.tl"

    if not table_file.exists():
        return False

    try:
        with open(table_file, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        if not isinstance(data, dict):
            return False

        # Проверяем fillType
        fill_type_value = data.get("fillType")
        if fill_type_value not in ["Registry", "AssetGrid"]:
            return False

        # Проверяем defaults
        defaults_value = data.get("defaults")

        # Если defaults есть, проверяем что это пустой словарь
        if defaults_value is not None:
            return isinstance(defaults_value, dict) and len(defaults_value) == 0

        # Если defaults нет, условие выполняется
        return True

    except (yaml.YAMLError, Exception):
        return False


# Загружаем список исключений
def load_exclusions(file_path):
    with open(file_path, "r", encoding="utf-8") as exclude_file:
        cfg = yaml.safe_load(exclude_file)
        return cfg["KnowledgebaseSlices"]["SIEM-Public"]["Excludes"]["Files"]


# Функция для проверки, содержится ли одна из частей пути в исключениях
def is_path_excluded(full_path, exclusions):
    # Приводим к нижнему регистру и убираем слеши для сравнения
    normalized_path = full_path.lower().replace("\\", "/")
    for exclusion in exclusions:
        if exclusion.lower() in normalized_path:
            return True
    return False


def extract_co_data(root_directory):
    # Загружаем исключения
    exclusions = load_exclusions(r"D:\Work\repo\knowledgebase\_extra\slices.yaml")

    result = {}
    cache_for_tl = {}
    # Рекурсивный обход всех каталогов
    for root, dirs, files in os.walk(root_directory):
        # Проверяем каждый файл в текущей директории
        for file in files:
            if file.endswith(".co"):
                # Путь к файлу .co
                co_file_path = os.path.join(root, file)
                # Путь к соответствующему metainfo.yaml
                yaml_file_path = os.path.join(root, "metainfo.yaml")

                # Проверяем, существует ли файл metainfo.yaml
                if os.path.exists(yaml_file_path):
                    # Проверяем, не является ли путь исключённым
                    if is_path_excluded(yaml_file_path, exclusions):
                        continue  # Пропускаем этот файл

                    try:
                        # Читаем YAML файл
                        with open(yaml_file_path, "r", encoding="utf-8") as f:
                            data = yaml.safe_load(f)
                        # Извлекаем ContentAutoName
                        content_auto_name = data.get("ContentAutoName")
                        if not content_auto_name:
                            continue
                        # Извлекаем TabularLists
                        tabular_lists = None
                        # Проверяем структуру данных
                        if (
                            data
                            and isinstance(data, dict)
                            and "ContentRelations" in data
                            and isinstance(data["ContentRelations"], dict)
                            and "Uses" in data["ContentRelations"]
                            and isinstance(data["ContentRelations"]["Uses"], dict)
                            and "SIEMKB" in data["ContentRelations"]["Uses"]
                            and isinstance(
                                data["ContentRelations"]["Uses"]["SIEMKB"], dict
                            )
                            and "Auto" in data["ContentRelations"]["Uses"]["SIEMKB"]
                            and isinstance(
                                data["ContentRelations"]["Uses"]["SIEMKB"]["Auto"], dict
                            )
                        ):
                            auto_section = data["ContentRelations"]["Uses"]["SIEMKB"][
                                "Auto"
                            ]
                            tabular_lists = auto_section.get("TabularLists")
                        # Формируем результат в нужном формате
                        result_list = []
                        # Обрабатываем TabularLists - значение всегда "No_manual_changes"
                        if tabular_lists and isinstance(tabular_lists, dict):
                            for key, value in tabular_lists.items():
                                if value not in cache_for_tl.keys():
                                    state = check_fill_type_strict(
                                        root_directory, value
                                    )
                                    if state:
                                        result_list.append({value: "No_manual_changes"})
                                        cache_for_tl[value] = state
                                else:
                                    if cache_for_tl[value]:
                                        result_list.append({value: "No_manual_changes"})
                        result[content_auto_name] = result_list
                    except yaml.YAMLError as e:
                        print(f"Ошибка парсинга YAML в файле {yaml_file_path}: {e}")
                    except Exception as e:
                        print(f"Ошибка при обработке файла {yaml_file_path}: {e}")
    return result


result = extract_co_data(r"D:\Work\repo\knowledgebase\packages")
with open("configs/table_mapping.json", "w", encoding="utf-8") as f_out:
    f_out.write(json.dumps(result, indent=4))
