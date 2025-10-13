import logging
import json
import warnings
import time
import re
from copy import deepcopy
from venv import logger

from .xlsx_out import MonitorXlsxWriter
from .settings_checker import Settings
from .policies_checker import EventPolicies
from .get_token import MPXAuthenticator
from pathlib import Path
import requests

import asyncio
from aiohttp import ClientSession, client_exceptions
import xlsxwriter
from tqdm.asyncio import tqdm

warnings.filterwarnings('ignore')

class EventsWorker:
    """Класс запроса событий из SIEM"""
    semaphore: asyncio.Semaphore
    settings: Settings
    logger: logging.Logger
    policies: EventPolicies
    auth: MPXAuthenticator

    def __init__(self, settings, logger, policies, auth, pol_blacklist, pol_whitelist = None, pol_spec = None, assets: bool = False):
        self.settings = settings
        self.logger = logger
        self.policies = policies
        self.semaphore = asyncio.Semaphore(self.settings.max_threads_for_siem_api)
        self.auth = auth
        self.policies.filter_policies(pol_blacklist, pol_whitelist, pol_spec)
        if assets:
            audit_pol = {
                'name': 'Audit Events Hack', 'number': 0,
                'filter': 'id = "PT_Positive_Technologies_MaxPatrol_customevent_collector_job_start"',
                'full_filter': "filter(id = \"PT_Positive_Technologies_MaxPatrol_customevent_collector_job_start\") | "
                               "select(time, event_src.host, dst.host, dst.asset, object.name) | sort(time desc) | "
                               "group(key: [dst.asset, object.name], agg: COUNT(*) as Cnt) | sort(Cnt desc) "
                               "| limit(100000)"}
            self.policies.rebuilt_policies.append(audit_pol)
            self.policies.small_policies.update({audit_pol["name"]: {"count": 1, "filters": [audit_pol["filter"]],
                                                                     "full_filters": [audit_pol["full_filter"]]}})

    async def work(self, group_id, asset_ids, out_folder):
        if self.policies.rebuilt_policies:
            time_from_value = int(time.time()) - self.settings.time_delta_hours * 60 * 60
            group_tasks = []
            for index, policy in enumerate(self.policies.rebuilt_policies):
                filter_new = policy["full_filter"]
                if asset_ids:
                    if policy["name"] != 'Audit Events Hack':
                        filter_new = create_new_filter(asset_ids, filter_new, "event_src")
                        temp_time_from = time_from_value
                    else:
                        filter_new = create_new_filter(asset_ids, filter_new, "dst")
                        temp_time_from = int(time.time()) - 700 * 60 * 60
                else:
                    temp_time_from = time_from_value
                group_tasks.append(asyncio.create_task(self.take_events(group_id, temp_time_from, filter_new, out_folder, policy)))
            results = await tqdm.gather(*group_tasks)
            for index, policy in enumerate(self.policies.rebuilt_policies):
                if "host_ids" not in self.policies.rebuilt_policies[index].keys():
                    self.policies.rebuilt_policies[index].update({"host_ids": results[index]})
                else:
                    for host in results[index]:
                        self.policies.rebuilt_policies[index]["host_ids"].update({host: results[index][host]})
                # break
            with (out_folder / "!out_all.json").open('w', encoding='utf-8') as out_file:
                json.dump(self.policies.rebuilt_policies, out_file, ensure_ascii=False, indent=4)
            with (out_folder / "!small_policies.json").open('w', encoding='utf-8') as out_file:
                json.dump(self.policies.small_policies, out_file, ensure_ascii=False, indent=4)
            return self.policies
        else:
            return [], {}

    async def take_events(self, group_id, time_from, event_filter, out_dir, all_policy):
        url = 'https://{}:443/api/events/v3/events/aggregation'.format(self.settings.mpx_host)
        file_name = all_policy["name"].replace(" ", "_")
        temp_policy = deepcopy(all_policy)
        temp_policy["host_ids"] = {}
        if "list_value" in temp_policy.keys():
            file_name += "_" + temp_policy["list_value"]
        file_name = re.sub('[^a-zA-Zа-яА-я_ 0-9-]', '_', file_name)
        if len(file_name) > 35:
            file_name = file_name[:35]
        index = 0
        here = False
        file_name += "_" + str(temp_policy["number"])
        while True:
            file_name += ".json"
            if (out_dir / file_name).is_file():
                if here:
                    file_name = file_name[:-6]
                else:
                    file_name = file_name[:-5]
                file_name += "_" + str(index)
                here = True
            else:
                break
            index += 1
        filter_file_name = file_name[:-5] + ".txt"
        with (out_dir / filter_file_name).open('w', encoding='utf-8') as out_file:
            out_file.write(event_filter)
        data = {
            "filter": event_filter,
            "timeFrom": time_from
        }
        if type(group_id) is str:
            param = {"groupId": group_id}
        else:
            param = {"groupIds": group_id}
        try_number = 0
        all_ok = False
        response = {}
        async with self.semaphore:
            while try_number < self.settings.reconnect_times:
                try:
                    try_number += 1
                    async with ClientSession(cookies=self.auth.cookies) as session:
                        async with session.post(url=url, json=data, headers=self.auth.headers, params=param, ssl=False,
                                                timeout=10000000) as response_temp:
                            if response_temp.status == 200:
                                response = await response_temp.json()
                                self.logger.debug(f'take_events response for {data}: {response}')
                                if not response["errors"]:
                                    all_ok = True
                                    break
                                else:
                                    self.logger.warning(f'Errors in take_events response for {data} in try number {try_number}: {response}')
                            elif response_temp.status >= 500:
                                self.logger.warning(f"Response code: {response_temp.status}. Try number: {try_number}. Next try after 5 seconds")
                                await asyncio.sleep(5)
                            elif response_temp.status == 400:
                                response = await response_temp.json()
                                self.logger.error(f"Response code: {response_temp.status}. Response message: {response["message"]}.")
                                self.logger.error(f"Most likely there is an error in the request: {event_filter}")
                                self.logger.error(f"Full response: {json.dumps(response, indent=4)}")
                                break
                            else:
                                response = await response_temp.json()
                                self.logger.error(f"Unspecified response code: {response_temp.status}. Event filter: {event_filter}. Break.")
                                self.logger.error(f"Full response: {json.dumps(response, indent=4)}")
                                break
                except requests.exceptions.RequestException as Err:
                    self.logger.warning(f"Connection error, something went horribly wrong, let's try again. Error: {Err}")
                except client_exceptions.ContentTypeError as Err:
                    self.logger.warning(f"Connection error, something went horribly wrong, let's try again. Error: {Err}")
        if all_ok:
            if response["rows"]:
                for row in response["rows"]:
                    if row['groups'][0] not in temp_policy["host_ids"].keys():
                        temp_policy["host_ids"].update({row['groups'][0]: {'count': row['values'][0], 'event_src.host': [row['groups'][1]]}})
                    else:
                        temp_policy["host_ids"][row['groups'][0]]['count'] += row['values'][0]
                        temp_policy["host_ids"][row['groups'][0]]['event_src.host'].append(row['groups'][1])
            with (out_dir / file_name).open('w', encoding='utf-8') as out_file:
                json.dump(temp_policy, out_file, ensure_ascii=False, indent=4)
            # TODO возможно лучше прям тут заполнять все политики
            return temp_policy["host_ids"]
        else:
            return {}

    def make_readable_out(self, out_path, asset_attrs, asset_dict, no_assets, need_up_file, asset_filter_comment = None):
        """Создание читаемых выводов"""
        # Считаем, что политики не меняют своей последовательности и те, что остались пустыми, не удалены
        # В целом можно организовать обратный разбор файлов через small_policies, если Итоговый json будет слишком большим
        excel_file = MonitorXlsxWriter(out_path, self.settings.mpx_host, self.settings.time_delta_hours, need_up_file)
        excel_file.add_start_info(self.policies.small_policies, asset_attrs, asset_filter_comment)
        for policy in self.policies.small_policies.keys():
            if policy != "Audit Events Hack":
                excel_file.prepare_pol_sheets(policy, self.policies.small_policies[policy])
                # worksheets = prepare_pol_sheets(workbook, worksheets, policy, small_policies[policy])
        if self.policies.rebuilt_policies:
            asset_dict = excel_file.create_asset_dict(self.policies.rebuilt_policies, self.policies.small_policies, asset_dict)
        with (out_path / "!asset_dict.json").open('w', encoding='utf-8') as out_assets:
            json.dump(asset_dict, out_assets, indent=4, ensure_ascii=False)
        excel_file.work_with_asset_dict(self.policies.small_policies, asset_dict, no_assets)
        closed = False
        for try_number in range(self.settings.reconnect_times):
            try:
                excel_file.workbook.close()
                closed = True
                break
            except xlsxwriter.exceptions.FileCreateError:
                logger.error(f"Can't create file {excel_file.workbook.filename}. Retry.")
                time.sleep(10)
        if not closed:
            logger.error(f"{excel_file.workbook.filename} not created. Skipping.")
        if Path(".bot.json").is_file():
            try:
                from .test_bot import start_work_bot
                bot = start_work_bot(excel_file.workbook.filename)
                bot.stop_polling()
            except Exception as Err:
                pass


def create_new_filter(asset_ids, filter_new, field):
    filter_pref = "filter({}.asset in [".format(field)
    for asset_id in asset_ids:
        filter_pref += asset_id + ","
    filter_pref = filter_pref.rstrip(",")
    filter_pref += "]"
    if filter_new.startswith("filter("):
        filter_new = filter_new.lstrip("filter").lstrip("(")
        filter_pref += " and "
        filter_new = filter_pref + filter_new
    else:
        filter_new = filter_pref + ") | " + filter_new
    return filter_new
