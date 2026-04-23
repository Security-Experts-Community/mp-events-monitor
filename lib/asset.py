import asyncio
import json
import logging
import re
import time
from copy import deepcopy
from pathlib import Path
from typing import Union
from uuid import UUID

import requests

from lib.events import EventsWorker
from lib.get_token import MPXAuthenticator
from lib.policies_checker import EventPolicies
from lib.settings_checker import Settings

EventsWorker = EventsWorker


class AssetWorker:
    settings: Settings
    auth: MPXAuthenticator
    logger: logging.Logger
    policies: EventPolicies
    filter_name: str
    pdql: Union[str, list[str]]
    default_politics_whitelist: Union[str, list[str], None]
    default_politics_blacklist: Union[str, list[str], None]
    mandatory_policies: Union[str, list[str], None]
    comment: Union[str, list[str], None]
    group: Union[str, UUID, list[UUID], list[str]]
    specific_politics: dict[str, list[dict[str, str | list[str]]]] | None
    """
    specific_politics - по сути легальный способ перезаписать системную политику, так как внутри filter_policies
    self.filtered_policies.update({add_pol..., то есть если встретится политика с таким же названием, то она 
    перезапишется.
    """
    all_search_values: dict[str, list[str]] | None
    """
    это нужно если ты хочешь прибить гвоздями проверки каких-то определенных хостов, проще примером:
    предположим у тебя в инфре есть хост c FQDN = one.default.local
    ты можешь сделать запрос PDQL filter(host.fqdn = "one.default.local") | select(@host, host.fqdn)
    и если этот PDQL тебе не принесет актива, то никто тебе не скажет что актива нет
    
    в целом это не беда, когда у тебя проверяется пара активов, но иногда нужно захардкодить штук 30 активов, 
    например ты знаешь что вот 30 хостов, и ты не хочешь делать стат группу АМ, тогда ты можешь описать 
    "all_search_values": {
        "host.fqdn": [
          "one.default.local"
    ]}    
    Тогда скрипт проверит что в результате AM запроса этот актив был не был, и если его не было, 
    то он выведет это в лог, а также в ексельке это значение будет оставаться красным
    """

    def __init__(
        self,
        settings,
        auth,
        logger: logging.Logger,
        policies,
        filter_name,
        filter_settings: dict,
    ):
        self.settings = settings
        # так надо, ведь мы не указываем весь набор библиотек необходимых для работы с озером
        if self.settings.dl_mode:
            global EventsWorker
            from lib.events_dl import EventsWorkerDL

            EventsWorker = EventsWorkerDL
        self.auth = auth
        self.logger = logger
        self.policies = policies
        self.filter_name = filter_name
        self.pdql = filter_settings["PDQL"]
        self.default_politics_whitelist = filter_settings.get(
            "default_politics_whitelist"
        )
        self.default_politics_blacklist = filter_settings.get(
            "default_politics_blacklist"
        )
        self.mandatory_policies = filter_settings.get("mandatory_policies")
        if self.mandatory_policies:
            if not self.default_politics_blacklist:
                self.default_politics_blacklist = []
            elif type(self.default_politics_blacklist) is str:
                self.default_politics_blacklist = [self.default_politics_blacklist]
            if type(self.mandatory_policies) is str:
                self.default_politics_blacklist.append(self.mandatory_policies)
                self.mandatory_policies = [self.mandatory_policies]
            elif type(self.mandatory_policies) is list:
                self.default_politics_blacklist.extend(self.mandatory_policies)
            else:
                logger.error(
                    f"self.mandatory_policies {self.mandatory_policies} is not string or list. Skip."
                )
        self.comment = filter_settings.get("comment")
        self.group = filter_settings["group"]
        self.specific_politics = filter_settings.get("specific_politics")
        self.all_search_values = filter_settings.get("all_search_values")
        self.fallback_search_field = filter_settings.get("fallback_search_field")
        if (self.default_politics_whitelist or self.default_politics_blacklist) is None:
            self.logger.warning(
                f"In asset filter {filter_name} no default_politics_whitelist or "
                f"default_politics_blacklist. Use default default_politics_blacklist: "
                f"{self.settings.event_policies}"
            )
            self.default_politics_blacklist = self.settings.event_policies
        self.statistic = {"name": self.filter_name}

    def assets_take_info(self, out_folder, need_up_file, all_search_values):
        if self.comment:
            if type(self.comment) is str:
                self.comment = [self.comment]
        asset_dict, asset_fields, no_assets = self.work(out_folder)
        num_assets = len(asset_dict.keys())
        self.logger.info(f"find {num_assets} assets")
        if no_assets:
            self.logger.info(f"and {len(no_assets)} lines with asset_id null")
        counter = self.settings.max_uuids_in_siem_query
        if num_assets > 0:
            ev = EventsWorker(
                self.settings,
                self.logger,
                self.policies,
                self.auth,
                self.default_politics_blacklist,
                self.default_politics_whitelist,
                self.specific_politics,
                self.mandatory_policies,
                not self.settings.dl_mode,
            )
            self.logger.info("Now take events by policies")
            count = (
                num_assets // counter + 1
                if num_assets % counter > 0
                else num_assets // counter
            )
            temp_list = list(asset_dict.keys())
            second_siem_dict = {}
            second_event_field = ""
            if (
                self.fallback_search_field
                and self.fallback_search_field.get("asset_field")
                and self.fallback_search_field.get("event_field")
            ):
                second_event_field = self.fallback_search_field.get("event_field")
                for asset in asset_dict:
                    if asset_dict[asset].get("asset_info") and asset_dict[asset][
                        "asset_info"
                    ].get(self.fallback_search_field["asset_field"]):
                        second_siem_dict[asset] = asset_dict[asset]["asset_info"][
                            self.fallback_search_field["asset_field"]
                        ]
            for stack in range(count):
                if stack + 1 < count:
                    out_dir = out_folder / (
                        str(stack * counter) + "-" + str((stack + 1) * counter)
                    )
                else:
                    out_dir = out_folder / (
                        str(stack * counter) + "-" + str(num_assets)
                    )
                out_dir.mkdir()
                asyncio.run(
                    ev.work(
                        self.settings.mpx_group,
                        temp_list[stack * counter : (stack + 1) * counter],
                        out_dir,
                        second_siem_dict,
                        second_event_field,
                    )
                )
                if stack + 1 < count:
                    self.logger.debug(f"{stack * counter}-{(stack + 1) * counter} done")
                else:
                    self.logger.debug(f"{stack * counter}-{num_assets} done")
                ev.semaphore = asyncio.Semaphore(self.settings.max_threads_for_siem_api)
            self.logger.info(f"make readable out in {out_folder}")
            ev.make_readable_out(
                out_folder,
                asset_fields,
                asset_dict,
                no_assets,
                need_up_file,
                self.comment,
            )
            self.statistic.update(ev.statistic)
        elif no_assets:
            ev = EventsWorker(
                self.settings,
                self.logger,
                self.policies,
                self.auth,
                self.default_politics_blacklist,
                self.default_politics_whitelist,
                self.specific_politics,
                self.mandatory_policies,
                not self.settings.dl_mode,
            )
            ev.policies.rebuilt_policies = []
            ev.policies.small_policies = {}
            ev.make_readable_out(
                out_folder,
                asset_fields,
                asset_dict,
                no_assets,
                need_up_file,
                self.comment,
            )
            self.statistic.update(ev.statistic)
        with (out_folder / "AssetWorker_stat.json").open("w", encoding="utf-8") as stat:
            json.dump(self.statistic, stat, ensure_ascii=False, indent=4)

    def work(self, out_folder):
        response, fields, asset_id_field, all_search_values = self.create_pdql_token(
            out_folder
        )
        if self.all_search_values:
            all_search_values = self.all_search_values
        if response:
            temp_token = str(response["token"])
            self.logger.debug(f"get PDQL token: {temp_token}")
            asset_dict, no_assets = self.take_assets(
                response["token"], out_folder, asset_id_field, all_search_values, fields
            )
            return asset_dict, fields, no_assets
        else:
            self.logger.warning("no PDQL token, return {}, [], []")
            return {}, [], []

    def create_pdql_token(self, out_folder: Path):
        url = "https://{}:443/api/assets_temporal_readmodel/v1/assets_grid".format(
            self.settings.mpx_host
        )
        old_pdql = self.pdql

        all_search_values = {}
        while self.pdql.find("<dynamic!{") != -1:
            dyn_filter = self.pdql[
                self.pdql.find("<dynamic!{") + 9 : self.pdql.find("}dynamic!>") + 1
            ]
            self.logger.info(f"find dyn filter in self.pdql: {self.pdql}")
            self.logger.info(json.dumps(json.loads(dyn_filter), indent=4))
            new_filter, all_search_values = work_with_dynamic(
                json.loads(dyn_filter), out_folder, self.logger
            )
            if new_filter:
                self.pdql = self.pdql.replace(
                    "<dynamic!" + dyn_filter + "dynamic!>", new_filter
                )
            else:
                self.logger.info(
                    "make cleared self.pdql for work and not stopping algo"
                )
                # TODO кривенькая заглушка, host.@id не обязателен, надо подменять поле из запроса
                self.pdql = self.pdql.replace(
                    "<dynamic!" + dyn_filter + "dynamic!>",
                    "host.@id = 00000000-0000-0000-0000-000000000000",
                )
        if old_pdql != self.pdql:
            self.logger.info(f"new self.pdql:{self.pdql}")
        if type(self.group) is str:
            if self.group == "-1":
                self.group = ["00000000-0000-0000-0000-000000000002"]
            else:
                self.group = [self.group]
        data = {
            "pdql": self.pdql,
            "selectedGroupIds": self.group,
            "includeNestedGroups": True,
            "utcOffset": "+03:00",
        }
        retry_num = 0
        while True:
            try:
                self.logger.debug("try to make self.pdql token")
                retry_num += 1
                if self.settings.reconnect_times < retry_num:
                    self.logger.error(
                        f"{retry_num - 1} attempt was unsuccessful while create_pdql_token: {self.pdql}. Exiting."
                    )
                    return {}, [], "", {}
                response_temp = self.auth.session.post(
                    url=url,
                    json=data,
                    headers=self.auth.headers,
                    verify=False,
                    cookies=self.auth.cookies,
                )
                if response_temp.status_code == 200:
                    file_name = "create_pdql_token_" + str(retry_num) + ".json"
                    if self.settings.logging_level == "DEBUG":
                        with (out_folder / file_name).open(
                            "w", encoding="utf-8"
                        ) as token_file:
                            json.dump(
                                response_temp.json(),
                                token_file,
                                ensure_ascii=False,
                                indent=4,
                            )
                    response = response_temp.json()
                    fields = []
                    asset_exist = False
                    asset_id_field = ""
                    for field in response["fields"]:
                        fields.append(field["name"])
                        if field["name"] == "asset_id" and field["type"] == "uuid":
                            asset_exist = True
                            asset_id_field = "asset_id"
                        elif field["name"] == "asset_id":
                            self.logger.error(
                                'Error in self.pdql, field "asset_id" is not uuid'
                            )
                            raise ValueError
                    if not asset_exist:
                        for field in response["fields"]:
                            if field["type"] == "assetInfo":
                                if asset_exist:
                                    asset_id_field = ""
                                    asset_exist = False
                                else:
                                    asset_exist = True
                                    asset_id_field = field["name"]
                    if not asset_exist:
                        self.logger.error(
                            f'Error in self.pdql, no field "asset_id" please add this field for correct work in next'
                            f' step:\n {self.pdql} \noften you need to add "host.@id as asset_id" to self.pdql'
                        )
                        raise ValueError
                    for all_search_value in all_search_values.keys():
                        if all_search_value not in fields:
                            self.logger.warning(
                                "all_search_value not in fields. Clear."
                            )
                            all_search_values = {}
                            break
                    return response, fields, asset_id_field, all_search_values
                elif response_temp.status_code == 400:
                    self.logger.error(
                        f"Error in self.pdql: {self.pdql}. Check self.pdql and rerun"
                    )
                    self.logger.info(
                        json.dumps(response_temp.json(), indent=4, ensure_ascii=False)
                    )
                    raise ValueError
                elif response_temp.status_code == 503:
                    self.logger.warning("503 Service Unavailable")
                elif response_temp.status_code == 401:
                    self.logger.error("Error with AuthHeader, stop script")
                    exit(1)
            except requests.exceptions.HTTPError as Err:
                self.logger.warning(
                    f"{retry_num - 1} attempt was unsuccessful while create_pdql_token: {self.pdql} Err: {Err}"
                )
            except requests.exceptions.RequestException as Err:
                self.logger.warning(
                    f"{retry_num - 1} attempt was unsuccessful while create_pdql_token: {self.pdql} Err: {Err}"
                )
            except ValueError:
                return {}, [], "", {}
            time.sleep(5)

    def take_assets(self, token, out_folder, asset_id_field, all_search_values, fields):
        url = "https://{}:443/api/assets_temporal_readmodel/v1/assets_grid/data".format(
            self.settings.mpx_host
        )
        offset = 0
        limit = 10000
        param = {
            "pdqlToken": token,
            "offset": offset,
            "limit": limit,
        }
        retry_num = 0
        try_num = 0
        asset_info = []
        unsuccessful = False

        self.logger.debug("try to get assets")
        while True:
            try:
                if unsuccessful:
                    retry_num += 1
                    unsuccessful = False
                    time.sleep(5)
                    self.logger.info(f"try number: {retry_num}")
                if self.settings.reconnect_times < retry_num:
                    self.logger.error(
                        f"{retry_num - 1} attempt was unsuccessful while take_assets: {self.pdql}. Exiting."
                    )
                    return {}, []
                response_temp = self.auth.session.get(
                    url=url,
                    params=param,
                    headers=self.auth.headers,
                    verify=False,
                    cookies=self.auth.cookies,
                )
                file_name = "take_assets_" + str(try_num) + ".json"
                try_num += 1
                if self.settings.logging_level == "DEBUG":
                    with (out_folder / file_name).open(
                        "w", encoding="utf-8"
                    ) as token_file:
                        json.dump(
                            response_temp.json(),
                            token_file,
                            ensure_ascii=False,
                            indent=4,
                        )
                    self.logger.info(
                        f"Create {file_name} code: {response_temp.status_code}"
                    )
                if response_temp.status_code == 200:
                    response = response_temp.json()
                    if not response.get("records"):
                        break
                    else:
                        asset_info.extend(response["records"])
                        len_resp_rec = len(response["records"])
                        if len(response["records"]) < limit:
                            self.logger.debug(f"take: {len_resp_rec} asset lines")
                            break
                        else:
                            self.logger.info(
                                f"take: {len_resp_rec} asset lines. limit: {limit}"
                            )
                            param["offset"] += limit
                elif response_temp.status_code in [400, 403, 404]:
                    self.logger.error(
                        f"problem take_assets {response_temp.status_code}"
                    )
                    exit(1)
            except requests.exceptions.HTTPError as Err:
                self.logger.warning(
                    f"{retry_num} attempt was unsuccessful while take_assets: {token}. pdql: {self.pdql}. Err: {Err}"
                )
                unsuccessful = True
            except requests.exceptions.RequestException as Err:
                self.logger.warning(
                    f"{retry_num} attempt was unsuccessful while take_assets: {token}. pdql: {self.pdql}. Err: {Err}"
                )
                unsuccessful = True
        if asset_info:
            asset_dict = {}
            no_assets = []
            for asset in asset_info:
                if all_search_values:
                    for all_search_attr in list(all_search_values.keys()).copy():
                        if (
                            asset.get(all_search_attr)
                            and asset[all_search_attr].lower()
                            in all_search_values[all_search_attr]
                        ):
                            search_val = asset[all_search_attr].lower()
                            all_search_values[all_search_attr].remove(search_val)
                            if all_search_attr[-5:] == ".fqdn":
                                dom_field = all_search_attr[: all_search_attr.find(".")]
                                if dom_field + ".hostname" in all_search_values.keys():
                                    hostname = search_val[: search_val.find(".")]
                                    if (
                                        hostname
                                        in all_search_values[dom_field + ".hostname"]
                                    ):
                                        all_search_values[
                                            dom_field + ".hostname"
                                        ].remove(hostname)
                        if not all_search_values[all_search_attr]:
                            all_search_values.pop(all_search_attr)
                asset_id = None
                if asset_id_field == "asset_id" and asset["asset_id"]:
                    asset_id = asset["asset_id"]
                elif asset_id_field != "asset_id" and asset[asset_id_field]["id"]:
                    asset_id = asset[asset_id_field]["id"]
                else:
                    no_assets.append(asset)
                if asset_id:
                    if asset_id not in asset_dict.keys():
                        asset_dict.update({asset_id: {"asset_info": asset}})
                    else:
                        if (
                            "asset_info_is_answer_again"
                            not in asset_dict[asset_id]["asset_info"].keys()
                        ):
                            asset_dict[asset_id]["asset_info"].update(
                                {"asset_info_is_answer_again": [asset]}
                            )
                        else:
                            asset_dict[asset_id]["asset_info"][
                                "asset_info_is_answer_again"
                            ].append(asset)
            if self.settings.logging_level == "DEBUG":
                file_name = "!take_assets.json"
                with (out_folder / file_name).open("w", encoding="utf-8") as token_file:
                    json.dump(asset_dict, token_file, ensure_ascii=False, indent=4)
            file_name = "!take_no_asset_ids.json"
            if all_search_values:
                self.logger.warning("not found by all_search_values:")
                self.logger.warning(
                    json.dumps(all_search_values, indent=4, ensure_ascii=False)
                )
                prep_dict = {i: None for i in fields}
                no_assets = all_search_to_no_asset(
                    all_search_values, prep_dict, no_assets
                )
            with (out_folder / file_name).open("w", encoding="utf-8") as token_file:
                json.dump(no_assets, token_file, ensure_ascii=False, indent=4)
            return asset_dict, no_assets
        else:
            no_assets = []
            if not all_search_values:
                self.logger.warning(
                    f"{self.pdql} give no asset info, no info, no attempt to taken events"
                )
            else:
                prep_dict = {i: None for i in fields}
                no_assets = all_search_to_no_asset(all_search_values, prep_dict, [])
            file_name = "!take_no_asset_ids.json"
            with (out_folder / file_name).open("w", encoding="utf-8") as token_file:
                json.dump(no_assets, token_file, ensure_ascii=False, indent=4)
            return {}, no_assets


def switch_and_clear_filter(
    static_filter, key_field, main_field, dm_fields, dyn_filter, main_values
):
    static_filter = static_filter.replace(
        '["+' + key_field + '"]', "['" + key_field + "']"
    )
    dm_fields[key_field] = re.search(
        "(\\S+) in <" + main_field + ">\\['" + key_field + "']", dyn_filter["filter"]
    ).group(1)
    if main_values[key_field]:
        str_list = str(main_values[key_field])
        if key_field.lower() in ["ips", "host.@id", "asset_id", "asset_ids"]:
            str_list = str_list.replace("'", "")
        static_filter = static_filter.replace(
            "<" + main_field + ">['" + key_field + "']", str_list
        )
    else:
        static_filter = re.sub(
            "<" + main_field + ">\\['" + key_field + "']", "", static_filter
        )
        static_filter = re.sub(dm_fields[key_field] + " in ", "", static_filter)
        static_filter = re.sub("(^ (or|and) )|( (or|and) $)", "", static_filter)
        static_filter = re.sub("(^ (or|and) )|( (or|and) $)", "", static_filter)
        maybe_two_conds = re.search(" (or|and)\\s{2,}(or|and) ", static_filter)
        if maybe_two_conds and maybe_two_conds.group(1) == "or":
            static_filter = static_filter.replace(maybe_two_conds.group(0), " or ")
        elif maybe_two_conds and maybe_two_conds.group(1) == "and":
            static_filter = static_filter.replace(maybe_two_conds.group(0), " and ")
    return static_filter, dm_fields


def work_with_dynamic(dyn_filter: dict, out_folder: Path, logger: logging.Logger):
    """
    Скрипт может выполнять работу на основе данных любого из предыдущих запросов.
    Примеры в дефолтных фильтрах: nginx_backends_to_hosts, а также можно дополнить по инструкции в comment_filter
    в фильтры:  HAP_Listen_info, HAP_Listen_bcks, HAP_Fr_Bck_info, HAP_Fr_Bck_bck.
    Может быть крайне полезным, если нужно снизить нагрузку за счет сокращения одного из джоинов.
    Когда-нибудь я напишу инструкцию как такие фильтры писать без попыток понять логику автора на основе примеров.
    """
    static_filter = ""
    dm_fields = {}
    out_folder = out_folder.parent
    out_folder = out_folder / dyn_filter["filter_name"]
    assets_file_path = out_folder / "!asset_dict.json"
    if not out_folder.is_dir() or not assets_file_path.is_file():
        logger.info(
            f"no folder {dyn_filter['filter_name']} in {out_folder} or no file with assets {assets_file_path}"
        )
        return "", {}
    assets = []
    # TODO а почему мы не берем !take_assets.json?
    with assets_file_path.open("r", encoding="utf-8") as assets_file:
        for asset_id, asset_info in json.load(assets_file).items():
            assets.append(asset_info["asset_info"])
    no_asset_path = out_folder / "!take_no_asset_ids.json"
    if no_asset_path.exists():
        with no_asset_path.open("r", encoding="utf-8") as no_asset_file:
            records = json.load(no_asset_file)
            if records:
                assets.extend(records)
    if not assets:
        logger.info(f"no assets in {dyn_filter['filter_name']}")
        return "", {}
    pref_com = ""
    if "prefix" in dyn_filter.keys():
        main_field = dyn_filter["prefix"][
            dyn_filter["prefix"].find("<") + 1 : dyn_filter["prefix"].find(">")
        ]
        pref_com = dyn_filter["prefix"].replace("<" + main_field + ">", "main_value")
    else:
        main_field = dyn_filter["filter"][
            dyn_filter["filter"].find("<") + 1 : dyn_filter["filter"].find(">")
        ]

    main_values_dict = False
    if "need_dict" in dyn_filter.keys() and dyn_filter["need_dict"]:
        main_values_dict = True
        main_values = {dict_key: [] for dict_key in dyn_filter["dict_keys"]}
    else:
        main_values = []

    for asset in assets:
        main_value = asset[main_field]
        if pref_com:
            main_value = eval(pref_com)
        if main_values_dict and dyn_filter["dict_keys"] == [
            "hostnames",
            "FQDNs",
            "IPs",
        ]:
            if re.search("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$", main_value):
                if main_value not in main_values["IPs"] and main_value != "127.0.0.1":
                    main_values["IPs"].append(main_value)
            elif main_value.find(".") != -1:
                if main_value not in main_values["FQDNs"]:
                    main_values["FQDNs"].append(main_value)
                if main_value.split(".")[0] not in main_values["hostnames"]:
                    main_values["hostnames"].append(main_value.split(".")[0])
            else:
                if (
                    main_value not in main_values["hostnames"]
                    and main_value != "localhost"
                ):
                    main_values["hostnames"].append(main_value)
        elif main_values_dict and dyn_filter["dict_keys"] == ["asset_ids"]:
            if main_value not in main_values["asset_ids"]:
                main_values["asset_ids"].append(main_value)
        else:
            main_values.append(main_value)
    if main_values_dict and dyn_filter["dict_keys"] == ["hostnames", "FQDNs", "IPs"]:
        static_filter = dyn_filter["filter"]
        for key_field in dyn_filter["dict_keys"]:
            static_filter, dm_fields = switch_and_clear_filter(
                static_filter, key_field, main_field, dm_fields, dyn_filter, main_values
            )
            main_values[dm_fields[key_field]] = main_values.pop(key_field)
    elif main_values_dict and dyn_filter["dict_keys"] == ["asset_ids"]:
        static_filter = dyn_filter["filter"]
        key_field = "asset_ids"
        static_filter, dm_fields = switch_and_clear_filter(
            static_filter, key_field, main_field, dm_fields, dyn_filter, main_values
        )
        main_values[dm_fields[key_field]] = main_values.pop(key_field)
    logger.info(json.dumps(main_values, indent=4))
    return static_filter, main_values


def all_search_to_no_asset(all_search_values: dict, prep_dict: dict, no_assets: list):
    for all_search_attr in list(all_search_values.keys()).copy():
        for value in all_search_values[all_search_attr]:
            app_dict = deepcopy(prep_dict)
            app_dict[all_search_attr] = value
            if all_search_attr[-5:] == ".fqdn":
                dom_field = all_search_attr[: all_search_attr.find(".")]
                if dom_field + ".hostname" in all_search_values.keys():
                    hostname = value[: value.find(".")]
                    if hostname in all_search_values[dom_field + ".hostname"]:
                        all_search_values[dom_field + ".hostname"].remove(hostname)
                        app_dict[dom_field + ".hostname"] = hostname
            no_assets.append(app_dict)
    return no_assets
