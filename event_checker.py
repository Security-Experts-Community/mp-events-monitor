from pathlib import Path
from venv import logger

import logging
import requests
import json
import re
import warnings
import shutil
from lib.settings_checker import Settings, check_group_id
from lib.get_token import MPXAuthenticator
from pydantic_core import ValidationError
from lib.policies_checker import EventPolicies
from lib.asset import AssetWorker

import sys
old_python = False
if sys.version.find('3.7.') == 0:
    print("Use old Python")
    old_python = True
    from lib.events_no_ai import EventsWorker
else:
    import asyncio
    from lib.events import EventsWorker

warnings.filterwarnings('ignore')

class MaxPatrolEventsMonitor():
    settings: Settings
    logger: logging.Logger = logging.getLogger("MaxPatrolEventsMonitor")
    policies: EventPolicies
    auth: MPXAuthenticator

    def __init__(self) -> None:
        try:
            self.settings = Settings()
        except (ValueError, ValidationError) as Err:
            print(Err)
            logging.basicConfig(level=30)
            self.logger.error(Err)
            exit(1)
        logging.basicConfig(level=self.settings.logging_level)
        self.logger.info(f'Settings checked. Accepted script mode: {self.settings.mode}')
        self.policies = EventPolicies(self.settings.event_policies_file, logger=self.logger)
        self.policies.check_policies()
        self.auth = MPXAuthenticator(self.logger)
        self.auth.authenticate(self.settings)


    def all_events_worker(self):
        temp_dir = (self.settings.out_folder / "ALL_events")
        temp_dir.mkdir()
        ev = EventsWorker(self.settings, self.logger, self.policies, self.auth, self.settings.event_policies)
        if not old_python:
            asyncio.run(ev.work(self.settings.mpx_group, [], temp_dir))
        else:
            ev.work(self.settings.mpx_group, [], temp_dir)
        ev.make_readable_out(temp_dir, [], {}, [], True, [])
    def asset_ids_worker(self):
        asset_dict = {}
        with Path("configs/asset_ids.txt").open("r", encoding='utf-8') as asset_ids_file:
            checker = True
            for line in asset_ids_file:
                line = line.strip()
                if not check_group_id(line, "configs/asset_ids.txt", self.logger):
                    checker = False
                elif line == "-1":
                    logging.error("-1 is not asset_id")
                    checker = False
                asset_dict.update({line: {}})
            if not checker:
                exit(1)
        temp_dir = (self.settings.out_folder / "Asset_IDs")
        temp_dir.mkdir()
        ev = EventsWorker(self.settings, self.logger, self.policies, self.auth, self.settings.event_policies)
        if not old_python:
            asyncio.run(ev.work(self.settings.mpx_group, asset_dict, temp_dir))
        else:
            ev.work(self.settings.mpx_group, asset_dict, temp_dir)
        ev.make_readable_out(temp_dir, [], asset_dict, [], True, [])
    def all_assets_worker(self):
        default_asset_filter = {
            "PDQL": self.settings.pdql_assets,
            "default_politics_blacklist": self.settings.event_policies,
            "group": self.settings.mpx_group
        }
        temp_dir = (self.settings.out_folder / "All_Assets")
        temp_dir.mkdir()
        aw = AssetWorker(self.settings, self.auth, self.logger, self.policies, "ALL_assets", default_asset_filter)
        aw.assets_take_info(temp_dir, True, {})
    def dynamic_modes(self):
        groups = []
        full_info_group = {}
        with Path('configs/dynamic_groups.txt').open('r', encoding='utf-8') as groups_file:
            for line in groups_file:
                line = line.strip()
                if check_group_id(line, "configs/dynamic_groups.txt", self.logger):
                    groups.append(line)
        for group in groups.copy():
            url = f"https://{self.settings.mpx_host}:443/api/assets_temporal_readmodel/v2/groups/{group}"
            response_temp = requests.session().get(url=url, headers=self.auth.headers, verify=False,
                                                   cookies=self.auth.cookies)
            if response_temp.status_code == 200:
                response = response_temp.json()
                if response["isDeleted"]:
                    self.logger.warning(f'{group} - {response["name"]} isDeleted. Skip group')
                    groups.remove(group)
                else:
                    full_info_group.update({group: response})
            elif response_temp.status_code == 400:
                self.logger.warning(f'{group} not exists. Skip group')
                groups.remove(group)
            else:
                print("Problem while take data about:", group + ".", "Error code:", response_temp.status_code)
        with Path((self.settings.out_folder / "group_info.json")).open('w', encoding='utf-8') as groups_file:
            json.dump(full_info_group, groups_file, indent=4, ensure_ascii=False)
        temp_dir = (self.settings.out_folder / "Dyn_groups")
        temp_dir.mkdir()
        if mem.settings.mode == "Dynamic_Groups_assets":
            default_asset_filter = {
                "PDQL": self.settings.default_PDQL_assets,
                "default_politics_blacklist": self.settings.event_policies,
                "group": groups
            }
            aw = AssetWorker(self.settings, self.auth, self.logger, self.policies,
                             "Dynamic_Groups_assets", default_asset_filter)
            aw.assets_take_info(temp_dir, True, {})
        elif mem.settings.mode == "Dynamic_Groups_events":
            ev = EventsWorker(self.settings, self.logger, self.policies, self.auth,
                              self.settings.event_policies)
            if not old_python:
                asyncio.run(ev.work(groups, [], temp_dir))
            else:
                ev.work(groups, [], temp_dir)
            ev.make_readable_out(temp_dir, [], {}, [],True, [])
    def asset_filters(self):
        with Path('configs/assets_filters.json').open('r', encoding='utf-8') as assets_filters_file:
            assets_filters = json.load(assets_filters_file)
        for assets_filter in assets_filters:
            print("\nstart", assets_filter)
            if assets_filter == "comments":
                continue
            folder_name = re.sub('[^a-zA-Zа-яА-я_ 0-9-]', '_', assets_filter)
            out_folder = (self.settings.out_folder / folder_name)
            if out_folder.exists():
                print(f'Out folder: {out_folder} exists. Skip filter')
                continue
            out_folder.mkdir()
            if "group" not in assets_filters[assets_filter]:
                assets_filters[assets_filter]["group"] = "-1"
            else:
                if type(assets_filters[assets_filter]["group"]) is list:
                    for group in assets_filters[assets_filter]["group"].copy():
                        if not check_group_id(
                                group, f"{assets_filter} in file \"assets_filters.json\"", logger):
                            assets_filters[assets_filter]["group"].remove(group)
                else:
                    if not check_group_id(assets_filters[assets_filter]["group"],
                                          f"{assets_filter} in file \"assets_filters.json\"", logger):
                        logger.info("Use default SIEM -1 AM 00000000-0000-0000-0000-000000000002")
                        assets_filters[assets_filter]["group"] = '-1'
            if type(assets_filters[assets_filter]["PDQL"]) is list:
                assets_filters[assets_filter]["PDQL"] = "".join(assets_filters[assets_filter]["PDQL"])
            elif type(assets_filters[assets_filter]["PDQL"]) is str:
                pass
            else:
                print("ERROR!\n", assets_filters[assets_filter]["PDQL"], "not a list and string, check.\nExiting")
                exit(1)
            all_search_values = {}
            if ("all_search_values" in assets_filters[assets_filter].keys()
                    and assets_filters[assets_filter]["all_search_values"]):
                all_search_values = assets_filters[assets_filter]["all_search_values"]
            aw = AssetWorker(self.settings, self.auth, self.logger, self.policies, assets_filter, assets_filters[assets_filter])
            aw.assets_take_info(out_folder, True, all_search_values)

if __name__ == "__main__":
    mem = MaxPatrolEventsMonitor()

    if mem.settings.mode == "ALL_events":
        mem.all_events_worker()
    elif mem.settings.mode == "Asset_IDs":
        mem.asset_ids_worker()
    elif mem.settings.mode == "ALL_assets":
        mem.all_assets_worker()
    elif mem.settings.mode in ["Dynamic_Groups_assets", "Dynamic_Groups_events"]:
        mem.dynamic_modes()
    elif mem.settings.mode == "Assets_filters":
        mem.asset_filters()
