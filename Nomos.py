# Проверка версии Python — до любых сторонних импортов: иначе оператор
# видит "DLL load failed while importing _pydantic_core" вместо объяснения
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from envcheck import ensure_supported_python  # noqa: E402

ensure_supported_python()

# В собранном exe работаем в его папке: configs/, out/ и logs/ лежат рядом
# с исполняемым файлом, откуда бы его ни запустили
if getattr(sys, "frozen", False):
    os.chdir(os.path.dirname(os.path.abspath(sys.executable)))

import asyncio
import json
import logging
import re
import warnings
from datetime import datetime, timezone
from pathlib import Path

import requests
from pydantic import ValidationError

from lib.asset import AssetWorker
from lib.events import EventsWorker
from lib.find_bad_assets import MaxPatrolPDQL
from lib.get_token import MPXAuthenticator
from lib.kb_checker import KB_Checker
from lib.policies_checker import EventPolicies
from lib.settings_checker import Settings, check_group_id
from lib.test_bot import archive_upload
from lib.xlsx_unified import XlsxUnited
from nomos import init_debug_harness
from nomos.autofinish import finalize_last_run
from nomos.errors import FilterExecutionError
from nomos.domain import dump_unified_report, merge_asset_statistics
from nomos.queryplan import plan_asset_worker
from nomos.service.collect import collect_unified

warnings.filterwarnings("ignore")

# Дружественный CLI: принимаем как канонический формат pydantic-settings
# (--debug_dump true), так и env-стиль (debug_dump=true) — оператору не нужно
# помнить разницу между файлом конфигурации и командной строкой.
sys.argv[1:] = [
    ("--" + arg) if ("=" in arg and not arg.startswith("-")) else arg
    for arg in sys.argv[1:]
]

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    level=logging.WARNING,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class MaxPatrolEventsMonitor:
    settings: Settings
    policies: EventPolicies
    auth: MPXAuthenticator
    united_xlsx: XlsxUnited

    def __init__(self, settings=None) -> None:
        self.logger: logging.Logger = logging.getLogger("MaxPatrolEventsMonitor")
        try:
            self.settings = settings if settings is not None else Settings()
        except (ValueError, ValidationError) as Err:
            self.logger.error(Err)
            sys.exit(1)
        # Этап 0: журналирование (консоль + nomos.log + JSONL) и HTTP-трассировка.
        # Ставится ДО первого запроса к MaxPatrol, чтобы попала и аутентификация.
        self.run_id = init_debug_harness(self.settings)
        self.logger.setLevel(self.settings.logging_level)
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
            "common": {"host": self.settings.mpx_host, "run_id": self.run_id},
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
        asyncio.run(ev.work(self.settings.mpx_group, [], temp_dir))
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
                sys.exit(1)
        temp_dir = self.settings.out_folder / "Asset_IDs"
        temp_dir.mkdir()
        ev = EventsWorker(
            self.settings,
            self.logger,
            self.policies,
            self.auth,
            self.settings.event_policies,
        )
        asyncio.run(ev.work(self.settings.mpx_group, asset_dict, temp_dir))
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
            asyncio.run(ev.work(groups, [], temp_dir))
            ev.make_readable_out(temp_dir, [], {}, [], True, [])
            self.statistic["Asset_IDs"] = ev.statistic

    def asset_filters(self, progress_cb=None, filter_done_cb=None):
        self.statistic["|Asset_filters|"] = {}
        with Path(self.settings.asset_filters_file).open(
            "r", encoding="utf-8"
        ) as assets_filters_file:
            assets_filters = json.load(assets_filters_file)
        filter_names = [f for f in assets_filters if f != "comments"]
        for assets_filter in assets_filters:
            self.logger.info(f"start {assets_filter}")
            if assets_filter == "comments":
                continue
            if progress_cb is not None:
                progress_cb(
                    filter_names.index(assets_filter) + 1,
                    len(filter_names),
                    assets_filter,
                )
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
                # N2: кривой PDQL — пропуск фильтра, а не смерть прогона
                self.statistic["|Asset_filters|"][assets_filter] = {
                    "FAILED": "PDQL не строка и не список строк"
                }
                continue
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
            if self.settings.dump_queries:
                # Отладка: что именно будет спрошено по этому фильтру
                plan_asset_worker(aw, self.logger)
            try:
                aw.assets_take_info(out_folder, True, all_search_values)
                if filter_done_cb is not None:
                    filter_done_cb(assets_filter)
            except FilterExecutionError as err:
                # N2: один сломанный фильтр не должен убивать прогон.
                self.logger.error(f"Фильтр пропущен: {err}")
                self.statistic["|Asset_filters|"][assets_filter] = {
                    "FAILED": err.reason
                }
                if filter_done_cb is not None:
                    filter_done_cb(assets_filter)
                continue

            self.statistic["|Asset_filters|"][assets_filter] = aw.statistic
            self.statistic["|Asset_filters|"][assets_filter]["PDQL"] = assets_filters[
                assets_filter
            ]["PDQL"]
        return assets_filters

    def unified_report(self, assets_filters, bad_assets={}):
        self.logger.info("try to make unified-report")
        # Этап 2: сборка вынесена в nomos.service.collect.collect_unified —
        # тот же код строит промежуточные срезы по мере выполнения фильтров.
        all_assets, all_no_asset, unified = collect_unified(
            out_folder=self.settings.out_folder,
            assets_filters=list(assets_filters),
            bad_assets=bad_assets,
            filters_statistic=self.statistic.get("|Asset_filters|"),
            host=self.settings.mpx_host,
            time_delta_hours=self.settings.time_delta_hours,
            run_id=self.run_id,
        )
        self.united_xlsx = XlsxUnited(
            self.settings.out_folder,
            self.settings.mpx_host,
            self.settings.time_delta_hours,
            self.logger,
        )
        self.united_xlsx.add_united_start_info(
            list(all_assets[list(all_assets.keys())[0]].keys())
        )
        self.united_xlsx.write_assets(all_assets)
        self.united_xlsx.write_no_assets(all_no_asset)
        self.united_xlsx.write_bad_assets(bad_assets)
        self.statistic["unified statistic"] = self.united_xlsx.statistics.model_dump()
        self.united_xlsx.workbook.close()

        # Этап 1/2: типизированный отчёт собран в collect_unified выше.
        self.last_unified_report = unified
        dump_unified_report(unified, self.settings.out_folder)

    def asset_analyzer(self, all_assets, asset_id, asset_stat, assets_filter):
        """Этап 1: логика слияния перенесена в
        nomos.domain.analysis.merge_asset_statistics."""
        merge_asset_statistics(all_assets, asset_id, asset_stat, assets_filter)


if __name__ == "__main__":
    _run_failed = False
    try:
        mem = MaxPatrolEventsMonitor()
        if not (mem.settings.out_folder / "bad_assets.json").exists():
            processor = MaxPatrolPDQL(mem.settings, mem.logger, mem.auth)
            processor.process_bad_assets(processor.asset_filters())
        if (mem.settings.out_folder / "bad_assets.json").exists():
            mem.logger.info("Load bad_assets.json")
            with (mem.settings.out_folder / "bad_assets.json").open(
                "r", encoding="utf-8"
            ) as f:
                bad_assets = json.load(f)
        global_assets_filters = []
        if mem.settings.kb_check_mode:
            if not (
                (mem.settings.out_folder / "KB_struct_uninstalled.json").exists()
                and (mem.settings.out_folder / "KB_struct.json").exists()
            ):
                kb_check_a = KB_Checker(mem.settings, mem.logger, mem.auth)
                kb_check_a.work()
            if (mem.settings.out_folder / "license_info.json").exists():
                with (mem.settings.out_folder / "license_info.json").open(
                    "r", encoding="utf-8"
                ) as f:
                    mem.statistic["lic_info"] = json.load(f)
            if (mem.settings.out_folder / "KB_struct_uninstalled.json").exists():
                with (mem.settings.out_folder / "KB_struct_uninstalled.json").open(
                    "r", encoding="utf-8"
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
            global_assets_filters = mem.asset_filters()
            mem.unified_report(global_assets_filters, bad_assets)
        # elif mem.settings.mode == "Only_KB":
        #     mem.kb_check()
        if (mem.settings.out_folder / "empty_tables.json").exists():
            mem.logger.info("Load empty_tables.json")
            with (mem.settings.out_folder / "empty_tables.json").open(
                "r", encoding="utf-8"
            ) as f:
                mem.statistic["empty_table_lists_to_fill"] = json.load(f)

        mem.statistic = dict(sorted(mem.statistic.items()))
        stat_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%m")
        stat_file_path = (
            mem.settings.out_folder / f"{stat_time}-{mem.settings.mpx_host}_stat.json"
        )
        with stat_file_path.open("w", encoding="utf-8") as stat:
            mem.logger.info(f"Dump telemetry file in {stat_file_path.absolute()}")
            json.dump(mem.statistic, stat, ensure_ascii=False, indent=4)

        if mem.settings.telemetry != "no":
            archive_upload(stat_file_path)
            mem.logger.info(
                f"File {stat_file_path.absolute()} was sent to storage.ptsecurity.com"
            )
        if mem.settings.telemetry == "all":
            archive_upload(mem.settings.out_folder, stat_file_path)
            mem.logger.info(
                f"Folder {mem.settings.out_folder.absolute()} was sent to storage.ptsecurity.com"
            )
        if mem.settings.telemetry == "no":
            user_choice = (
                input("Upload archive to storage.ptsecurity.com? (Y/N): ").strip().upper()
            )
            if user_choice == "Y":
                report_type = (
                    input("Short or full report? (F - full / other - simplified): ")
                    .strip()
                    .upper()
                )

                if report_type == "F":
                    archive_upload(mem.settings.out_folder, stat_file_path)
                else:
                    archive_upload(stat_file_path)
    except SystemExit:
        _run_failed = True
        raise
    except BaseException:
        _run_failed = True
        logging.getLogger("Nomos").exception("Прогон завершился ошибкой")
        raise
    finally:
        # Автозавершение: эталон/сверка + диагностический бандл (см. nomos.autofinish)
        finalize_last_run(failed=_run_failed)
