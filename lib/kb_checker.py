from pathlib import Path
import xlsxwriter
import json
import asyncio
import aiohttp
import logging
import requests
from tqdm.asyncio import tqdm
from collections import defaultdict
import unicodedata

try:
    from .settings_checker import Settings
except:
    from settings_checker import Settings
try:
    from .get_token import MPXAuthenticator
except:
    from get_token import MPXAuthenticator
try:
    from .incidents_checker import Inc_Checker
except:
    from incidents_checker import Inc_Checker
import difflib
from datetime import datetime


class KB_Checker:
    """Класс запроса событий из SIEM"""

    semaphore: asyncio.Semaphore
    settings: Settings
    logger: logging.Logger
    auth: MPXAuthenticator

    def __init__(self, settings, logger, auth):
        self.settings = settings
        self.logger = logger
        self.semaphore = asyncio.Semaphore(self.settings.max_threads_for_siem_api)
        self.auth = auth
        self.auth.headers["Content-Database"] = self.get_ContentDB()

    def localize_pack(self, pack, loc_dict):
        for item in loc_dict["categories"]:
            if item["id"] == pack:
                return item["name"]
        
        return pack
    
    def merge_dicts(self, d1, d2):
        result = {}

        for key in d1.keys() | d2.keys():
            v1 = d1.get(key)
            v2 = d2.get(key)

            buf = []

            if isinstance(v1, list):
                buf.extend(v1)          # уже список → добавляем элементы
            elif v1 is not None:       # обычное значение
                buf.append(v1)

            if isinstance(v2, list):
                buf.extend(v2)
            elif v2 is not None:
                buf.append(v2)

            result[key] = buf          # любой ключ → список (в том числе с 1 элементом)

        return result

    def put_rules_to_packs(self, items):
        grouped = defaultdict(list)

        for rec in items:
            if 'FolderPath' in rec.keys() and rec['FolderPath'] is not None:
                folder = unicodedata.normalize('NFKC', rec['FolderPath']) \
                            .encode('cp1251', errors='replace').decode('cp1251')
                rest = {k: (unicodedata.normalize('NFKC', v)
                            .encode('cp1251', errors='replace')
                            .decode('cp1251') if isinstance(v, str) else v)
                        for k, v in rec.items() if k != 'FolderPath'}
                grouped[folder.rsplit("/")[0]].append(rest)

        grouped = dict(grouped)

        return grouped

    def get_formula_text(self, ptkb_id):
        url = "https://{}:8091/api-studio/siem/correlation-rules/{}".format(
            self.settings.mpx_host, ptkb_id
        )
        response_temp = requests.session().get(
            url=url, headers=self.auth.headers, verify=False, cookies=self.auth.cookies
        )
        if response_temp.status_code == 200:
            response = response_temp.json()
        else:
            self.logger.debug(response_temp)

        if response:
            return response["Formula"]

    def diff_formulas_to_file(
        self, formula1_str, formula2_str, file1, file2, output_file="diff.txt"
    ):
        lines1 = formula1_str.splitlines()
        lines2 = formula2_str.splitlines()

        diff = difflib.unified_diff(lines1, lines2, fromfile=file1, tofile=file2)
        result = "\n".join(diff)

        diffs_dir = self.settings.out_folder / "diffs"
        diffs_dir.mkdir(parents=True, exist_ok=True)

        try:
            with (diffs_dir / output_file).open("w") as f:
                f.write(result)
        except IOError as e:
            self.logger.error(f"Ошибка записи в файл: {e}")
            return False

        return True

    def get_forks(self, object_list):
        copied_objects = {}
        object_dict = {
            obj["Id"]: obj for obj in object_list
        }  # Создаем словарь для быстрого доступа по Id

        for obj in object_list:
            if "CopyOf" in obj and obj["CopyOf"] and "Id" in obj["CopyOf"]:
                original_id = obj["CopyOf"]["Id"]
                if original_id in object_dict:
                    original_obj = object_dict[original_id]
                    original_object_id = original_obj["ObjectId"]
                    original_object_name = original_obj["SystemName"]

                    key = (original_object_id, original_id, original_object_name)
                    value = (obj["ObjectId"], obj["Id"], obj["SystemName"])

                    if key in copied_objects:
                        copied_objects[key].append(value)
                    else:
                        copied_objects[key] = [value]

        return copied_objects

    def get_ContentDB(self):
        DB_name = ""
        url = "https://{}:8091/api-studio/databases/content-databases".format(
            self.settings.mpx_host
        )
        response_temp = requests.session().get(
            url=url, headers=self.auth.headers, verify=False, cookies=self.auth.cookies
        )
        get_resp = False
        if response_temp.status_code == 200:
            response_temp.raise_for_status()
            response = response_temp.json()
            get_resp = True
        elif response_temp.status_code == 401:
            raise ConnectionError(
                "Get status code 401 unauthorized. Check your rights (or your token). "
                "If you don't have rights and it's right disable kabachock (KB_check mode), use `k=0` "
                "in config file")
        else:
            raise ConnectionError(f'Status: {response_temp.status_code}. Content: {response_temp.content}')
        if get_resp:
            for database in response:
                if database["IsDeployable"]:
                    DB_name = database["Name"]
        return DB_name

    def get_real_names_pipeline(self, curr_conveyors):
        url = "https://{}:8091/api-studio/siem/pipelines".format(self.settings.mpx_host)
        response_temp = requests.session().get(
            url=url, headers=self.auth.headers, verify=False, cookies=self.auth.cookies
        )
        if response_temp.status_code == 200:
            response = response_temp.json()
        else:
            self.logger.debug(response_temp)

        for i in range(len(curr_conveyors)):
            for item in response:
                if item["Id"] == curr_conveyors[i]:
                    curr_conveyors[i] = item["Alias"]

        return curr_conveyors

    def get_siems_info(self):
        url = "https://{}/api/siem_manager/v1/siems".format(self.settings.mpx_host)
        response_temp = requests.session().get(
            url=url, headers=self.auth.headers, verify=False, cookies=self.auth.cookies
        )
        if response_temp.status_code == 200:
            response = response_temp.json()
        else:
            self.logger.debug(response_temp)

        result = {}
        for pipeline in response:
            url_count = "https://{}/api/events/v1/siem_counters/correlation_rules?siem_id={}".format(
                self.settings.mpx_host, pipeline["id"]
            )
            response_temp_count = requests.session().get(
                url=url_count,
                headers=self.auth.headers,
                verify=False,
                cookies=self.auth.cookies,
            )
            if response_temp_count.status_code == 200:
                response_count = response_temp_count.json()
            else:
                self.logger.error(url_count)
                self.logger.error(response_temp_count.status_code)
                self.logger.error(response_temp_count.content)

            result[pipeline["alias"]] = response_count

            for key, value in result.items():
                result[key] = [
                    {"name": item["name"], "runCount": item["runCount"]}
                    for item in sorted(value, key=lambda x: x["runCount"], reverse=True)
                    if "ubrule" not in item["name"] and "ubRule" not in item["name"]
                ][:15]

        return result

    def get_content_by_type(self, type_, amount):

        query = {
                "skip": 0,
                "folderId": None,
                "filters": {"SiemObjectType": [type_]},
                "search": "",
                "sort": [{"name": "objectId", "order": 0, "type": 0}],
                "recursive": True,
                "setId": "00000000-0000-0000-0000-000000000001",
                "withoutSets": False,
                "take": amount,
            }

        url = "https://{}:8091/api-studio/siem/objects/list".format(
                self.settings.mpx_host
            )

        response_temp = requests.session().post(
            url=url,
            headers=self.auth.headers,
            verify=False,
            json=query,
            cookies=self.auth.cookies,
        )
        if response_temp.status_code == 201:
            return response_temp.json()
        else:
            raise RuntimeError(
                f"GET content failed: {response_temp.status_code} – {response_temp.text}"
            )

    def get_original_item_ids(self, content, name_set):
        if name_set is not None:
            ids = {
                item["SystemName"]: item["Id"]
                for item in content["Rows"]
                if item["SystemName"] in name_set and item["CopyOf"] is None
            }
        else:
            ids = {
                item["SystemName"]: item["Id"]
                for item in content["Rows"]
                if item["CopyOf"] is None
            }
        return ids

    def get_deploy(self, content, name_set):
        if name_set is not None:
            ids = {
                item["SystemName"]: item["DeploymentStatuses"]
                for item in content["Rows"]
                if item["SystemName"] in name_set and item["CopyOf"] is None
            }
        else:
            ids = {
                item["SystemName"]: item["DeploymentStatuses"]
                for item in content["Rows"]
                if item["CopyOf"] is None
            }
        return ids

    def get_conveyors(self, content):
        return {
            key for row in content["Rows"] for key in row["DeploymentStatuses"].keys()
        }

    async def _check_one(self, key, value, prog):
        async with self.semaphore:
            async with aiohttp.ClientSession() as session:
                query = {
                    "skip": 0,
                    "take": 50,
                    "filters": {"ContentType": ["User"]},
                    "sort": None,
                }
                url = f"https://{self.settings.mpx_host}:8091/api-studio/siem/tabular-lists/{value}/rows"
                async with session.post(
                    url,
                    json=query,
                    headers=self.auth.headers,
                    cookies=self.auth.cookies,
                    ssl=False,
                ) as resp:
                    if resp.status == 201:
                        data = await resp.json()
                        if data.get("Count", 0) > 0:
                            prog.update(1)
                            return {key: value}
        prog.update(1)
        return None

    async def get_changed(self, all_tables_list):
        prog = tqdm(
            total=len(all_tables_list), desc="Checking tables", leave=True, unit="req"
        )
        tasks = [self._check_one(k, v, prog) for k, v in all_tables_list.items()]

        results = [r for r in await asyncio.gather(*tasks) if r is not None]
        prog.close()
        return results

    def work(self):
        self.logger.info("Получаем правила")

        all_tables = self.get_content_by_type("TabularList", 1000)
        current_conveyors = list(self.get_conveyors(all_tables))
        all_tables_list = self.get_original_item_ids(all_tables, None)
        table_statuses = self.get_deploy(all_tables, None)

        self.logger.info(f"В сиеме есть конвейеры: {current_conveyors}")        
        all_corrs = self.get_content_by_type("Correlation", 10000)["Rows"]

        self.logger.info("Восстанавливаем структуру пакетов экспертизы")

        expertise_dict = self.put_rules_to_packs(all_corrs)
        table_dict = self.put_rules_to_packs(all_tables["Rows"])
        combined_dict = self.merge_dicts(expertise_dict, table_dict)

        with open(f"{self.settings.out_folder}\\KB_struct.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(combined_dict, indent=4, ensure_ascii=False))

        uninstalled_content = {}
        for expert_pack in combined_dict.keys():
            temp_list = []
            for item in combined_dict[expert_pack]:
                if item["GeneralDeploymentStatus"] != "Installed":
                    temp_list.append(item["SystemName"])
            uninstalled_content[expert_pack] = temp_list

        with open(f"{self.settings.out_folder}\\KB_struct_uninstalled.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(uninstalled_content, indent=4, ensure_ascii=False))            

        with open("configs\\packages_names.json", "r", encoding="utf-8") as f_packs:
            packs_names = json.load(f_packs)

        with open("configs\\table_filters.json", "r") as f:
            table_filters = json.load(f)

        file_name = (
            datetime.now().strftime("%Y-%m-%d")
            + "-table_report-"
            + self.settings.mpx_host
            + ".xlsx"
        )

        report_file = f"{self.settings.out_folder}\\{file_name}"

        changed = asyncio.run(self.get_changed(all_tables_list))

        changed_dict = {}
        for item in changed:
            changed_dict.update(item)

        missing = []
        for group, names in table_filters.items():
            for name in names:
                if name not in all_tables_list and group != "comment":
                    missing.append(name)

        categories = {group: [] for group in table_filters}
        for group, names in table_filters.items():
            if group != "comment":
                categories[group].extend(names)

        self.logger.info("Пошли спрашивать инциденты")

        inc_info = Inc_Checker(self.settings, self.logger, self.auth)

        closed_incidents = inc_info.get_info_about_inc()
        self.logger.info(
            "Пошли искать ручные инциденты среди {}".format(
                len(closed_incidents["incidents"])
            )
        )
        closed_manual = asyncio.run(inc_info.check_all_inc(closed_incidents))
        self.logger.info("Cправились найти ручные инцы {}".format(len(closed_manual)))
        workbook = xlsxwriter.Workbook(report_file)

        green_format = workbook.add_format({"bg_color": "#4d6335"})
        red_format = workbook.add_format({"bg_color": "#9a1115"})
        yellow_format = workbook.add_format({"bg_color": "#F0E40C"})

        header_format = workbook.add_format(
            {"bold": True, "font_color": "white", "bg_color": "#4F81BD", "border": 1}
        )

        # cell_format = workbook.add_format({"border": 1})

        worksheet = workbook.add_worksheet("TOTAL_STATS")

        worksheet.write(1, 0, "Пакет экспертизы", header_format)
        worksheet.write(1, 1, "Имя табличного списка", header_format)
        worksheet.merge_range(
            0, 2, 0, 2 + len(current_conveyors) - 1, "Конвейеры", header_format
        )
        worksheet.write(
            1, 2 + len(current_conveyors), "Изменялся вручную", header_format
        )
        worksheet.write(
            1,
            4 + len(current_conveyors),
            "Инциденты закрываются за неделю",
            header_format,
        )
        worksheet.write(
            1,
            5 + len(current_conveyors),
            "Используются ручные инциденты",
            header_format,
        )
        worksheet.write(
            1, 14 + 2 * len(current_conveyors), "Не установлены правила", header_format
        )
        worksheet.write(
            2,
            4 + len(current_conveyors),
            "YES" if closed_incidents else "NO",
            green_format
            if closed_incidents and closed_incidents["totalItems"] > 0
            else red_format,
        )
        worksheet.write(
            2,
            5 + len(current_conveyors),
            "YES" if closed_manual else "NO",
            green_format if closed_manual else red_format,
        )

        for row_idx, tbl_name in enumerate(
            [item for _, tables in categories.items() for item in tables], start=2
        ):
            fmt = green_format if tbl_name in changed_dict else red_format

            worksheet.write(row_idx, 1, tbl_name, fmt)
            worksheet.write(
                row_idx,
                2 + len(current_conveyors),
                "YES" if tbl_name in changed_dict else "NO",
                fmt,
            )

            for i in range(len(current_conveyors)):
                if current_conveyors[i] in table_statuses[tbl_name].keys():
                    if table_statuses[tbl_name][current_conveyors[i]] == "Installed":
                        worksheet.write(row_idx, 2 + i, "Installed", green_format)
                else:
                    worksheet.write(row_idx, 2 + i, "Not installed", red_format)

                if table_statuses[tbl_name] == {}:
                    worksheet.write(
                        row_idx, 2 + len(current_conveyors), "-----", yellow_format
                    )

        if missing != []:
            worksheet.write(
                0,
                7 + len(current_conveyors),
                "Отсутствуют в установочной БД",
                header_format,
            )
            index = 2
            for miss in missing:
                worksheet.write(index, 7 + len(current_conveyors), miss, red_format)
                index += 1

        index = 3
        for key in categories.keys():
            if key != "comment":
                shift = len(categories[key])
                if shift > 1:
                    worksheet.merge_range(
                        "A{}:A{}".format(index, index + shift - 1), self.localize_pack(key, packs_names), header_format
                    )
                else:
                    worksheet.write(index - 1, 0, self.localize_pack(key, packs_names), header_format)
                index += shift

        current_conveyors = self.get_real_names_pipeline(current_conveyors)
        top_triggered_rules = self.get_siems_info()

        for i in range(len(current_conveyors)):
            worksheet.write(1, 2 + i, current_conveyors[i], header_format)
            worksheet.write(
                1,
                9 + i * 2 + len(current_conveyors),
                current_conveyors[i],
                header_format,
            )
            worksheet.write(
                1,
                10 + i * 2 + len(current_conveyors),
                "Сработок в сутки",
                header_format,
            )
            for j in top_triggered_rules.keys():
                if current_conveyors[i] == j:
                    index = 2
                    for rule in top_triggered_rules[j]:
                        worksheet.write(
                            index, 9 + i * 2 + len(current_conveyors), rule["name"]
                        )
                        worksheet.write(
                            index, 10 + i * 2 + len(current_conveyors), rule["runCount"]
                        )
                        index += 1    
              

        self.logger.info("Ищем неустановленные правила среди {}".format(len(all_corrs)))
        uninstalled_rules = [r for r in all_corrs if r["DeploymentStatuses"] == {}]
        self.logger.info("И нашли: {}".format(len(uninstalled_rules)))

        self.logger.info("Ищем форки правила среди {}".format(len(all_corrs)))
        forks = self.get_forks(all_corrs)

        for i in range(len(uninstalled_rules)):
            worksheet.write(
                2 + i,
                14 + 2 * len(current_conveyors),
                uninstalled_rules[i]["SystemName"],
            )
            worksheet.write(
                2 + i, 15 + 2 * len(current_conveyors), uninstalled_rules[i]["ObjectId"]
            )

        worksheet.autofit()
        workbook.close()
        self.logger.info(f"Отчёт успешно создан: {report_file}")

        for key, value in forks.items():
            original = self.get_formula_text(key[1])
            for val in value:
                forked = self.get_formula_text(val[1])
                self.diff_formulas_to_file(
                    original,
                    forked,
                    key[0] + " - " + key[2],
                    val[0] + " - " + val[2],
                    self.settings.mpx_host + "_" + key[0] + "_diff_" + val[0] + ".txt",
                )


# create_report()
if __name__ == "__main__":
    settings = Settings()
    logger = logging.getLogger("KB_Checker_Manual")
    auth = MPXAuthenticator(logger)
    auth.authenticate(settings)
    KB_Check = KB_Checker(settings, logger, auth)
    KB_Check.work()
