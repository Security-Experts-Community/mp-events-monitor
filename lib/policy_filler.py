import concurrent.futures
import itertools
import json
import os
import time
from threading import Lock

import yaml
from tqdm import tqdm


def dict_to_query_string(query_dict):
    conditions = []

    for key, value in query_dict.items():
        if value is None:
            conditions.append(f"not {key}")
        elif value == True:
            conditions.append(f"{key}")
        elif isinstance(value, list):
            if value:
                conditions.append(f'{key} = "{value[0]}"')
            else:
                conditions.append(f"not {key}")
        else:
            conditions.append(f'{key} = "{value}"')

    return " and ".join(conditions)


def transform_queries(data):
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

            # Генерируем все комбинации
            for combination in itertools.product(*field_values):
                new_query = single_fields.copy()

                # Добавляем по одному значению для каждого поля-списка
                for i, field_name in enumerate(field_names):
                    new_query[field_name] = [combination[i]]

                new_queries.append(new_query)

        transformed[key] = {"queries": new_queries}
    return transformed


def localize_pack(pack, loc_dict):
    for item in loc_dict["categories"]:
        if item["id"] == pack:
            return item["name"]

    return pack


def check_match(dict1, dict2):
    for key, value in dict1.items():
        if key not in dict2:
            return False
        if value is not None:
            if isinstance(value, list):
                if not any(item.lower() == dict2[key].lower() for item in value):
                    return False
            else:
                if isinstance(value, bool):
                    continue
                if value.lower() != dict2[key].lower():
                    return False
    return True


def find_matching_js_files(root_dir, dictionary):
    """Поиск совпадающих JS файлов (можно распараллелить)"""
    matching_paths = []
    js_files = []

    # Сначала соберем все JS файлы
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".js"):
                js_files.append(os.path.join(dirpath, filename))

    # Обработка файлов с прогресс-баром
    for filepath in tqdm(js_files, desc="Поиск в JS файлах", leave=False):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        if check_match(dictionary, data):
            result = data.get("id")
            if result:
                matching_paths.append(result)

    return list(set(matching_paths))


def find_matching_js_files_parallel(root_dir, dictionary):
    """Многопоточная версия поиска JS файлов"""
    js_files = []

    # Сбор всех JS файлов
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".js"):
                js_files.append(os.path.join(dirpath, filename))

    matching_paths = []
    lock = Lock()

    def process_file(filepath):
        nonlocal matching_paths
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if check_match(dictionary, data):
                result = data.get("id")
                # print("Я result! → " + result)
                if result:
                    with lock:
                        matching_paths.append(result)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # Многопоточная обработка
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        list(
            tqdm(
                executor.map(process_file, js_files),
                total=len(js_files),
                desc="Поиск JS файлов",
                leave=False,
            )
        )

    return list(set(matching_paths))


def find_correlation_packs(root_dir, nf_list):
    """Поиск correlation packs"""
    packs = []
    co_files = []

    # Сбор всех .co файлов
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".co"):
                filepath = os.path.join(dirpath, "metainfo.yaml")
                if os.path.exists(filepath):
                    co_files.append(filepath)

    # Обработка с прогресс-баром
    for filepath in tqdm(co_files, desc="Поиск correlation packs", leave=False):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                rule_meta = yaml.safe_load(f)

            if "ContentRelations" in rule_meta:
                try:
                    for formula in rule_meta["ContentRelations"]["Uses"]["SIEMKB"][
                        "Auto"
                    ]["NormalizationRules"]:
                        nf_value = rule_meta["ContentRelations"]["Uses"]["SIEMKB"][
                            "Auto"
                        ]["NormalizationRules"][formula]
                        if nf_value in nf_list:
                            parts = filepath.split("\\")
                            packages_index = parts.index("packages")
                            result = parts[packages_index + 1]
                            packs.append(result)
                except (KeyError, TypeError):
                    continue
        except (yaml.YAMLError, UnicodeDecodeError):
            continue

    return list(set(packs))


def find_correlation_packs_parallel(root_dir, nf_list):
    """Многопоточная версия поиска correlation packs"""
    co_files = []
    dependent_corrs = {}

    # Сбор всех .co файлов
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".co"):
                filepath = os.path.join(dirpath, "metainfo.yaml")
                if os.path.exists(filepath):
                    co_files.append(filepath)

    packs = []
    lock = Lock()

    def process_co_file(filepath):
        nonlocal packs
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                rule_meta = yaml.safe_load(f)

            curr_rule = filepath.split("\\")[-2]

            if "ContentRelations" in rule_meta:
                try:
                    for formula in rule_meta["ContentRelations"]["Uses"]["SIEMKB"][
                        "Auto"
                    ]["NormalizationRules"]:
                        nf_value = rule_meta["ContentRelations"]["Uses"]["SIEMKB"][
                            "Auto"
                        ]["NormalizationRules"][formula]
                        if nf_value in nf_list:
                            parts = filepath.split("\\")
                            packages_index = parts.index("packages")
                            result = parts[packages_index + 1]
                            with lock:
                                packs.append(result)
                                if result in dependent_corrs.keys():
                                    dependent_corrs[result].append(curr_rule)
                                else:
                                    dependent_corrs[result] = [curr_rule]
                except (KeyError, TypeError):
                    pass
        except (yaml.YAMLError, UnicodeDecodeError):
            pass

    # Многопоточная обработка
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        list(
            tqdm(
                executor.map(process_co_file, co_files),
                total=len(co_files),
                desc="Correlation packs",
                leave=False,
            )
        )

    return list(set(packs)), dependent_corrs


def process_policy_item(args):
    """Обработка одного элемента политики для многопоточного выполнения"""
    policy_key, policy_data, root_directory = args

    if policy_data.get("queries"):
        all_packs = []
        all_deps = {}  # Собираем все зависимости в один словарь

        for item in policy_data["queries"]:
            print("Я итем! -> " + str(item))
            # Используем многопоточные версии функций
            norms = find_matching_js_files_parallel(root_directory, item)
            packs, deps_for_item = find_correlation_packs_parallel(
                root_directory, norms
            )
            all_packs.extend(packs)

            # Сохраняем зависимости для текущего item
            all_deps[dict_to_query_string(item)] = {
                key: list(set(value)) for key, value in deps_for_item.items()
            }

        return policy_key, list(set(all_packs)), all_deps

    return policy_key, [], {}


def main():
    # Загрузка политик
    policies = {}
    with open(
        "configs\\event_policies_old.json", "r", encoding="utf-8"
    ) as policies_file_long:
        policies_long = json.load(policies_file_long)

    policies_long_transformed = transform_queries(policies_long)
    for single_change in policies_long_transformed:
        policies_long[single_change]["queries"] = policies_long_transformed[
            single_change
        ]["queries"]

    with open("configs\\event_policies_old.json", "w", encoding="utf-8") as filled:
        json.dump(policies_long, filled, indent=4, ensure_ascii=False)

    with open(
        "configs\\event_policies_old.json", "r", encoding="utf-8"
    ) as policies_file:
        policies = json.load(policies_file)

    packs_about_many_softs = ["bruteforce", "profiling", "remote_work"]

    root_directory = r"D:\Work\repo\knowledgebase\packages"
    total_policies = len(policies)

    print(f"Всего политик: {total_policies}")
    print("Начинаем обработку...")

    # Подготовка аргументов для многопоточной обработки
    policy_args = [(key, policies[key], root_directory) for key in policies.keys()]

    with open("configs\\packages_names.json", "r", encoding="utf-8") as f_packs:
        packs_names = json.load(f_packs)

    file_blacklist = []

    with open(r"D:\Work\repo\knowledgebase\_extra\slices.yaml") as exclude_file:
        cfg = yaml.safe_load(exclude_file)
        file_blacklist = cfg["KnowledgebaseSlices"]["SIEM-Public"]["Excludes"]["Files"]
        for i in range(len(file_blacklist)):
            file_blacklist[i] = file_blacklist[i].replace("packages/", "")

    print(file_blacklist)
    separate_dict = {}

    # Многопоточная обработка политик
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        for policy_key, packs, found_deps in tqdm(
            executor.map(process_policy_item, policy_args),
            total=total_policies,
            desc="Обработка политик",
        ):
            policies[policy_key]["KB_packs"] = packs
            if policy_key not in separate_dict:
                separate_dict[policy_key] = {}
            separate_dict[policy_key] = found_deps

    for policy_key in policies:
        temp = policies[policy_key]["KB_packs"]
        policies[policy_key]["KB_packs"] = [
            item
            for item in temp
            if (item not in file_blacklist and item not in packs_about_many_softs)
        ]
        for i in range(0, len(policies[policy_key]["KB_packs"])):
            policies[policy_key]["KB_packs"][i] = localize_pack(
                policies[policy_key]["KB_packs"][i], packs_names
            )

    # Сохранение результатов
    with open("configs\\event_policies_old.json", "w", encoding="utf-8") as filled:
        json.dump(policies, filled, indent=4, ensure_ascii=False)

    with open("configs\\event_policies.json", "w", encoding="utf-8") as filled:
        json.dump(separate_dict, filled, indent=4, ensure_ascii=False)

    print("Обработка завершена!")


if __name__ == "__main__":
    main()
