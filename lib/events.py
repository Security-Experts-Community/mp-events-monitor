import asyncio
import json
import logging
import re
import time
import warnings
from pathlib import Path

import xlsxwriter
from aiohttp import (BasicAuth, ClientError, ClientResponseError,
                     ClientSession, ClientTimeout)
from aiohttp.client_exceptions import ContentTypeError
from tqdm.asyncio import tqdm

from lib.get_token import MPXAuthenticator
from lib.policies_checker import EventPolicies
from lib.settings_checker import Settings
from lib.xlsx_out import MonitorXlsxWriter

warnings.filterwarnings("ignore")


class EventsWorker:
    """Класс запроса событий из SIEM"""

    semaphore: asyncio.Semaphore
    settings: Settings
    logger: logging.Logger
    policies: EventPolicies
    auth: MPXAuthenticator
    async_session: ClientSession

    def __init__(
        self,
        settings,
        logger,
        policies,
        auth,
        pol_blacklist=None,
        pol_whitelist=None,
        pol_spec=None,
        mand_pols=None,
        assets: bool = False,
    ):
        self.settings = settings
        self.logger = logger
        self.policies = policies
        self.semaphore = asyncio.Semaphore(self.settings.max_threads_for_siem_api)
        self.auth = auth
        self.policies.filter_policies(pol_blacklist, pol_whitelist, pol_spec, mand_pols)
        self.proxy_inf = {}
        if self.auth.session.proxies:
            self.proxy_inf["proxy"] = (
                f"http://{self.settings.proxy_host}:{self.settings.proxy_port}/"
            )
            if self.settings.proxy_user:
                self.proxy_inf["proxy_auth"] = BasicAuth(
                    self.settings.proxy_user,
                    self.settings.proxy_password.get_secret_value(),
                )

        if assets and self.settings.audit_hack:
            audit_pol = {
                "name": "Audit Events Hack",
                "number": 0,
                "filter": 'id = "PT_Positive_Technologies_MaxPatrol_customevent_collector_job_start"',
                "full_filter": 'filter(id = "PT_Positive_Technologies_MaxPatrol_customevent_collector_job_start") | '
                "select(time, event_src.host, dst.host, dst.asset, object.name) | sort(time desc) | "
                "group(key: [dst.asset, object.name], agg: COUNT(*) as Cnt) | sort(Cnt desc) "
                "| limit(100000)",
            }
            self.policies.rebuilt_policies.append(audit_pol)
            self.policies.small_policies.update(
                {audit_pol["name"]: {audit_pol["filter"]: {}}}
            )
        self.statistic = {}

    async def work(
        self,
        group_id,
        asset_ids,
        out_folder,
        second_siem_dict=None,
        second_event_field="",
    ):
        if self.policies.rebuilt_policies:
            self.async_session = ClientSession(
                cookies=self.auth.cookies, headers=self.auth.headers, **self.proxy_inf
            )

            time_from_value = (
                int(time.time()) - self.settings.time_delta_hours * 60 * 60
            )
            group_tasks = []
            group_tasks_second = []
            for index, policy in enumerate(self.policies.rebuilt_policies):
                filter_new = policy["full_filter"]
                filter_second = policy["full_filter"]
                if asset_ids:
                    if policy["name"] != "Audit Events Hack":
                        filter_new = create_new_filter(
                            asset_ids, filter_new, "event_src.asset"
                        )
                        temp_time_from = time_from_value
                        if second_siem_dict:
                            filter_second = filter_second.replace(
                                "group(key: [event_src.asset, event_src.host]",
                                f"group(key: [event_src.asset, event_src.host, {second_event_field}]",
                            )
                            filter_second = create_new_filter(
                                second_siem_dict.values(),
                                filter_second,
                                second_event_field,
                            )
                    else:
                        filter_new = create_new_filter(
                            asset_ids, filter_new, "dst.asset"
                        )
                        temp_time_from = int(time.time()) - 700 * 60 * 60
                else:
                    temp_time_from = time_from_value
                group_tasks.append(
                    asyncio.create_task(
                        self.take_events(
                            group_id, temp_time_from, filter_new, out_folder, policy
                        )
                    )
                )
                if second_siem_dict:
                    group_tasks_second.append(
                        asyncio.create_task(
                            self.take_events(
                                group_id,
                                temp_time_from,
                                filter_second,
                                out_folder,
                                policy,
                                second_siem_dict,
                            )
                        )
                    )
            await tqdm.gather(*group_tasks)
            if group_tasks_second:
                await tqdm.gather(*group_tasks_second, desc="second")
            if self.settings.logging_level == "DEBUG":
                with (out_folder / "!out_all.json").open(
                    "w", encoding="utf-8"
                ) as out_file:
                    json.dump(
                        self.policies.rebuilt_policies,
                        out_file,
                        ensure_ascii=False,
                        indent=4,
                    )
                with (out_folder / "!small_policies.json").open(
                    "w", encoding="utf-8"
                ) as out_file:
                    json.dump(
                        self.policies.small_policies,
                        out_file,
                        ensure_ascii=False,
                        indent=4,
                    )
            await self.async_session.close()
            return self.policies
        else:
            return [], {}

    async def take_events(
        self, group_id, time_from, event_filter, out_dir, all_policy, secondary=None
    ):
        url = f"https://{self.settings.mpx_host}:443/api/events/v3/events/aggregation"
        file_name = all_policy["name"].replace(" ", "_")
        if secondary:
            file_name += "_secondary"
        temp_policy = all_policy
        if "host_ids" not in temp_policy:
            temp_policy["host_ids"] = {}

        if "list_value" in temp_policy:
            file_name += "_" + temp_policy["list_value"]

        # Очистка имени файла от недопустимых символов
        file_name = re.sub(r"[^a-zA-Zа-яА-Я_0-9\-]", "_", file_name)
        # Ограничение длины имени
        if len(file_name) > 35:
            file_name = file_name[:35]

        file_name += "_" + str(temp_policy["number"])
        file_name += ".json"

        # Генерация уникального имени файла
        original_name = file_name
        index = 0
        while (out_dir / file_name).exists():
            # Убираем ".json" перед добавлением индекса
            base_name = original_name[:-5]  # "-5" because ".json"
            file_name = f"{base_name}_{index}.json"
            index += 1

        filter_file_name = file_name[:-5] + ".txt"
        file_path = out_dir / filter_file_name

        modified_delta = 0
        data = {"filter": event_filter, "timeFrom": time_from}

        if isinstance(group_id, str):
            param = {"groupId": group_id}
        else:
            param = {"groupIds": group_id}
        if self.settings.logging_level == "DEBUG":
            with file_path.open("w", encoding="utf-8") as out_file:
                temp_data_with_params = {"data": data, "params": param}
                json.dump(temp_data_with_params, out_file, ensure_ascii=False, indent=4)
                del temp_data_with_params

        try_number = 0
        all_ok = False
        response = {}

        # ⚠️ ВАЖНО: семафор захватываем только на отправку запроса, НЕ на retry + sleep!
        while try_number < self.settings.reconnect_times:
            try_number += 1
            try:
                # захват семафора только для короткого действия: отправки запроса
                async with self.semaphore:
                    async with self.async_session.post(
                        url=url,
                        json=data,
                        params=param,
                        ssl=False,
                        timeout=ClientTimeout(total=600),  # явный таймаут 100 сек
                    ) as response_temp:
                        status = response_temp.status
                        if status == 200:
                            response = await response_temp.json()
                            if not response.get("errors"):
                                all_ok = True
                                break  # успех — выходим из retry-цикла
                            else:
                                # Ошибка в ответе — уменьшаем timeFrom
                                if not modified_delta:
                                    modified_delta = self.settings.time_delta_hours // 2
                                else:
                                    modified_delta = modified_delta // 2
                                data["timeFrom"] = (
                                    int(time.time()) - modified_delta * 60 * 60
                                )
                                self.logger.warning(
                                    f"Errors in take_events response for {file_path} (try {try_number}): "
                                    f"{response}. New timeFrom: {data['timeFrom']}. Retrying in 5s..."
                                )
                        elif status >= 500:
                            # Серверная ошибка — retry
                            if not modified_delta:
                                modified_delta = self.settings.time_delta_hours // 2
                            else:
                                modified_delta = modified_delta // 2
                            data["timeFrom"] = (
                                int(time.time()) - modified_delta * 60 * 60
                            )
                            self.logger.warning(
                                f"Server error {status} for {file_path}. Try {try_number}/{self.settings.reconnect_times}. "
                                f"New timeFrom: {data['timeFrom']}. Retrying in 5s..."
                            )
                        elif status == 400:
                            response = await response_temp.json()
                            self.logger.error(
                                f"Bad request (400) for {file_path}. Error: {response.get('message', 'unknown')}."
                            )
                            self.logger.error(
                                f"Full response: {json.dumps(response, indent=4)}"
                            )
                            break  # не retryable
                        else:
                            response = await response_temp.json()
                            self.logger.error(
                                f"Unexpected status {status} for {file_path}. Break. Response: {response}"
                            )
                            break  # не retryable

                # ✅ sleep — ВНЕ semafore!
                if not all_ok and try_number < self.settings.reconnect_times:
                    await asyncio.sleep(5)

            except (ClientError, ClientResponseError, ContentTypeError) as Err:
                self.logger.warning(
                    f"HTTP error (try {try_number}): {Err}. Retrying in 5s..."
                )
                # sleep вне семафора — уже сделано выше, но для надёжности:
                if try_number < self.settings.reconnect_times:
                    await asyncio.sleep(5)
            except Exception as Err:
                self.logger.exception(
                    f"Unexpected error in take_events (try {try_number}): {Err}"
                )
                if try_number < self.settings.reconnect_times:
                    await asyncio.sleep(5)

        # Обработка результата
        if all_ok and response.get("rows"):
            for row in response["rows"]:
                event_info = {
                    "count": row["values"][0],
                    "event_src.host": [row["groups"][1]],
                }
                group_id_from_row = row["groups"][0]
                if group_id_from_row not in temp_policy["host_ids"].keys():
                    temp_policy["host_ids"].update({group_id_from_row: event_info})
                    if secondary:
                        for asset_id_sec, asset_host in secondary.items():
                            if (
                                asset_host == row["groups"][2]
                                and asset_id_sec != group_id_from_row
                            ):
                                if asset_id_sec not in temp_policy["host_ids"].keys():
                                    temp_policy["host_ids"].update(
                                        {asset_id_sec: event_info}
                                    )
                                else:
                                    temp_policy["host_ids"][asset_id_sec][
                                        "count"
                                    ] += row["values"][0]
                elif not secondary:
                    temp_policy["host_ids"][group_id_from_row]["count"] += row[
                        "values"
                    ][0]
                    temp_policy["host_ids"][group_id_from_row]["event_src.host"].append(
                        row["groups"][1]
                    )

        # Сохранение в файл (только если нужно)
        if self.settings.logging_level == "DEBUG":
            with (out_dir / file_name).open("w", encoding="utf-8") as out_file:
                json.dump(temp_policy, out_file, ensure_ascii=False, indent=4)

        return temp_policy["host_ids"] if all_ok else {}

    def make_readable_out(
        self,
        out_path,
        asset_attrs,
        asset_dict,
        no_assets,
        need_up_file,
        asset_filter_comment=None,
    ):
        """Создание читаемых выводов"""
        # Считаем, что политики не меняют своей последовательности и те, что остались пустыми, не удалены
        # В целом можно организовать обратный разбор файлов через small_policies, если Итоговый json будет слишком большим
        excel_file = MonitorXlsxWriter(
            out_path,
            self.settings.mpx_host,
            self.settings.time_delta_hours,
            need_up_file,
            self.logger,
        )
        excel_file.add_start_info(
            self.policies.small_policies, asset_attrs, asset_filter_comment
        )
        for policy in self.policies.small_policies.keys():
            if policy != "Audit Events Hack":
                excel_file.prepare_pol_sheets(
                    policy, self.policies.small_policies[policy], out_path
                )
        if self.policies.rebuilt_policies:
            asset_dict = excel_file.create_asset_dict(
                self.policies.rebuilt_policies, self.policies.small_policies, asset_dict
            )
        excel_file.work_with_asset_dict(
            self.policies.small_policies,
            asset_dict,
            no_assets,
            out_path,
            self.policies.mandatory_policies,
        )
        self.statistic = excel_file.statistics.model_dump()
        if "exp_coverage_percent_array" in self.statistic.keys():
            self.statistic.pop("exp_coverage_percent_array")
        with (out_path / "!asset_dict.json").open("w", encoding="utf-8") as out_assets:
            json.dump(asset_dict, out_assets, indent=4, ensure_ascii=False)
        closed = False
        for try_number in range(self.settings.reconnect_times):
            try:
                excel_file.workbook.close()
                closed = True
                break
            except xlsxwriter.exceptions.FileCreateError:
                self.logger.error(
                    f"Can't create file {excel_file.workbook.filename}. Retry."
                )
                time.sleep(10)
        if not closed:
            self.logger.error(f"{excel_file.workbook.filename} not created. Skipping.")
        if Path(".bot.json").is_file():
            try:
                from .test_bot import start_work_bot

                bot = start_work_bot(excel_file.workbook.filename)
                bot.stop_polling()
            except Exception as Err:
                pass


def create_new_filter(asset_ids, filter_new, field):
    filter_pref = "filter({} in [".format(field)
    for asset_id in asset_ids:
        if field.find(".asset") != -1:
            filter_pref += asset_id + ","
        else:
            filter_pref += '"' + asset_id + '",'
    filter_pref = filter_pref.rstrip(",")
    filter_pref += "]"
    if filter_new.startswith("filter("):
        filter_new = filter_new.lstrip("filter").lstrip("(")
        filter_pref += " and "
        filter_new = filter_pref + filter_new
    else:
        filter_new = filter_pref + ") | " + filter_new
    return filter_new
