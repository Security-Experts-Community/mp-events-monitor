import json
import logging
import re
import sys
import warnings
from pathlib import Path

import requests
from pydantic import ValidationError

from lib.asset import AssetWorker
from lib.get_token import MPXAuthenticator
from lib.kb_checker import KB_Checker
from lib.policies_checker import EventPolicies
from lib.settings_checker import Settings, check_group_id
from lib.test_bot import telem_bot

old_python = False
if sys.version.find("3.7.") == 0:
    print("Use old Python")
    old_python = True
    from lib.events_no_ai import EventsWorker
else:
    import asyncio

    from lib.events import EventsWorker

warnings.filterwarnings("ignore")


class MaxPatrolEventsMonitor:
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
        self.logger.info(
            f"Settings checked. Accepted script mode: {self.settings.mode}"
        )
        self.policies = EventPolicies(
            self.settings.event_policies_file, logger=self.logger
        )
        self.policies.check_policies()
        self.auth = MPXAuthenticator(self.logger)
        self.auth.authenticate(self.settings)
        self.statistic = {
            "common": {"host": self.settings.mpx_host},
            "asset_file": str(self.settings.asset_filters_file),
        }

    def all_events_worker(self):
        temp_dir = self.settings.out_folder / "ALL_events"
        temp_dir.mkdir()
        ev = EventsWorker(
            self.settings,
            self.logger,
            self.policies,
            self.auth,
            self.settings.event_policies,
        )
        if not old_python:
            asyncio.run(ev.work(self.settings.mpx_group, [], temp_dir))
        else:
            ev.work(self.settings.mpx_group, [], temp_dir)
        ev.make_readable_out(temp_dir, [], {}, [], True, [])
        self.statistic["ALL_events"] = ev.statistic

    def asset_ids_worker(self):
        asset_dict = {}
        with self.settings.asset_ids_file.open("r", encoding="utf-8") as asset_ids_file:
            checker = True
            for line in asset_ids_file:
                line = line.strip()
                if not check_group_id(line, self.settings.asset_ids_file, self.logger):
                    checker = False
                elif line == "-1":
                    logging.error("-1 is not asset_id")
                    checker = False
                asset_dict.update({line: {}})
            if not checker:
                exit(1)
        temp_dir = self.settings.out_folder / "Asset_IDs"
        temp_dir.mkdir()
        ev = EventsWorker(
            self.settings,
            self.logger,
            self.policies,
            self.auth,
            self.settings.event_policies,
        )
        if not old_python:
            asyncio.run(ev.work(self.settings.mpx_group, asset_dict, temp_dir))
        else:
            ev.work(self.settings.mpx_group, asset_dict, temp_dir)
        ev.make_readable_out(temp_dir, [], asset_dict, [], True, [])
        self.statistic["Asset_IDs"] = ev.statistic

    def all_assets_worker(self):
        default_asset_filter = {
            "PDQL": self.settings.pdql_assets,
            "default_politics_blacklist": self.settings.event_policies,
            "group": self.settings.mpx_group,
        }
        temp_dir = self.settings.out_folder / "All_Assets"
        temp_dir.mkdir()
        aw = AssetWorker(
            self.settings,
            self.auth,
            self.logger,
            self.policies,
            "ALL_assets",
            default_asset_filter,
        )
        aw.assets_take_info(temp_dir, True, {})
        self.statistic["ALL_assets"] = aw.statistic

    def dynamic_modes(self):
        groups = []
        full_info_group = {}
        with self.settings.dynamic_groups_file.open(
            "r", encoding="utf-8"
        ) as groups_file:
            for line in groups_file:
                line = line.strip()
                if check_group_id(line, self.settings.dynamic_groups_file, self.logger):
                    groups.append(line)
        for group in groups.copy():
            url = f"https://{self.settings.mpx_host}:443/api/assets_temporal_readmodel/v2/groups/{group}"
            response_temp = requests.session().get(
                url=url,
                headers=self.auth.headers,
                verify=False,
                cookies=self.auth.cookies,
            )
            if response_temp.status_code == 200:
                response = response_temp.json()
                if response["isDeleted"]:
                    self.logger.warning(
                        f'{group} - {response["name"]} isDeleted. Skip group'
                    )
                    groups.remove(group)
                else:
                    full_info_group.update({group: response})
            elif response_temp.status_code == 400:
                self.logger.warning(f"{group} not exists. Skip group")
                groups.remove(group)
            else:
                self.logger.warning(
                    f"Problem while take data about: {group}. Error code: {response_temp.status_code}"
                )
        with Path((self.settings.out_folder / "group_info.json")).open(
            "w", encoding="utf-8"
        ) as groups_file:
            json.dump(full_info_group, groups_file, indent=4, ensure_ascii=False)
        temp_dir = self.settings.out_folder / "Dyn_groups"
        temp_dir.mkdir()
        if mem.settings.mode == "Dynamic_Groups_assets":
            default_asset_filter = {
                "PDQL": self.settings.pdql_assets,
                "default_politics_blacklist": self.settings.event_policies,
                "group": groups,
            }
            aw = AssetWorker(
                self.settings,
                self.auth,
                self.logger,
                self.policies,
                "Dynamic_Groups_assets",
                default_asset_filter,
            )
            aw.assets_take_info(temp_dir, True, {})
            self.statistic["Dynamic_Groups_assets"] = aw.statistic
        elif mem.settings.mode == "Dynamic_Groups_events":
            ev = EventsWorker(
                self.settings,
                self.logger,
                self.policies,
                self.auth,
                self.settings.event_policies,
            )
            if not old_python:
                asyncio.run(ev.work(groups, [], temp_dir))
            else:
                ev.work(groups, [], temp_dir)
            ev.make_readable_out(temp_dir, [], {}, [], True, [])
            self.statistic["Asset_IDs"] = ev.statistic

    def asset_filters(self):
        self.statistic["|Asset_filters|"] = {}
        with Path(self.settings.asset_filters_file).open(
            "r", encoding="utf-8"
        ) as assets_filters_file:
            assets_filters = json.load(assets_filters_file)
        for assets_filter in assets_filters:
            self.logger.info(f"start {assets_filter}")
            if assets_filter == "comments":
                continue
            folder_name = re.sub("[^a-zA-Zа-яА-я_ 0-9-]", "_", assets_filter)
            out_folder = self.settings.out_folder / folder_name
            if out_folder.exists():
                self.logger.info(f"Out folder: {out_folder} exists. Skip filter")
                try:
                    with (out_folder / "AssetWorker_stat.json").open(
                        "r", encoding="utf-8"
                    ) as old_aw_stat_file:
                        old_aw_stat = json.load(old_aw_stat_file)
                        old_aw_stat["OLD AssetWorker_stat"] = True
                        self.statistic["|Asset_filters|"][assets_filter] = old_aw_stat
                except Exception:
                    pass
                continue
            out_folder.mkdir()
            if "group" not in assets_filters[assets_filter]:
                assets_filters[assets_filter]["group"] = "-1"
            else:
                if type(assets_filters[assets_filter]["group"]) is list:
                    for group in assets_filters[assets_filter]["group"].copy():
                        if not check_group_id(
                            group,
                            f'{assets_filter} in file "assets_filters.json"',
                            self.logger,
                        ):
                            assets_filters[assets_filter]["group"].remove(group)
                else:
                    if not check_group_id(
                        assets_filters[assets_filter]["group"],
                        f'{assets_filter} in file "assets_filters.json"',
                        self.logger,
                    ):
                        self.logger.info(
                            "Use default SIEM -1 AM 00000000-0000-0000-0000-000000000002"
                        )
                        assets_filters[assets_filter]["group"] = "-1"
            if type(assets_filters[assets_filter]["PDQL"]) is list:
                assets_filters[assets_filter]["PDQL"] = "".join(
                    assets_filters[assets_filter]["PDQL"]
                )
            elif type(assets_filters[assets_filter]["PDQL"]) is str:
                pass
            else:
                self.logger.error(
                    f"{assets_filters[assets_filter]['PDQL']} not a list and string, check."
                )
                self.logger.error("Exiting")
                exit(1)
            all_search_values = {}
            if (
                "all_search_values" in assets_filters[assets_filter].keys()
                and assets_filters[assets_filter]["all_search_values"]
            ):
                all_search_values = assets_filters[assets_filter]["all_search_values"]
            aw = AssetWorker(
                self.settings,
                self.auth,
                self.logger,
                self.policies,
                assets_filter,
                assets_filters[assets_filter],
            )
            aw.assets_take_info(out_folder, True, all_search_values)
            self.statistic["|Asset_filters|"][assets_filter] = aw.statistic


if __name__ == "__main__":
    mem = MaxPatrolEventsMonitor()
    if mem.settings.kb_check_mode:
        kb_check_a = KB_Checker(mem.settings, mem.logger, mem.auth)
        kb_check_a.work()
        mem.statistic["lic_info"] = kb_check_a.lic_info
        with open(
            f"{mem.settings.out_folder}\\KB_struct_uninstalled.json",
            "r",
            encoding="utf-8",
        ) as f:
            mem.statistic["not_installed"] = json.load(f)
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
    # elif mem.settings.mode == "Only_KB":
    #     mem.kb_check()
    stat_file_path = mem.settings.out_folder / f"{mem.settings.mpx_host}_stat.json"

    with open(
        f"{mem.settings.out_folder}\\empty_tables.json",
        "r",
        encoding="utf-8",
    ) as f:
        mem.statistic["empty_table_lists_to_fill"] = json.load(f)

    mem.statistic = dict(sorted(mem.statistic.items()))

    with stat_file_path.open("w", encoding="utf-8") as stat:
        json.dump(mem.statistic, stat, ensure_ascii=False, indent=4)
    if mem.settings.telemetry != "no":
        telem_bot(stat_file_path)
