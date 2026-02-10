import asyncio
import json
import logging
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Optional  # type: ignore[attr-defined]

import backoff
import pandas as pd
from datalake_client import DatalakeClient, DatalakeSettings
from loguru import logger as guru_logger
from pydantic.v1 import BaseModel
from sqlalchemy.exc import DBAPIError
from tqdm.asyncio import tqdm

from .events import EventsWorker

warnings.filterwarnings("ignore")
global_reconnect_times = 5
pd.set_option("display.max_columns", 10)
pd.set_option("display.width", 1500)


def get_backoff_decorator(
    exceptions: tuple | type[Exception] | None = Exception,
) -> Any:
    """Обертка над on_exception, чтобы прибить параметры и не перечислять каждый раз"""
    global global_reconnect_times
    return backoff.on_exception(
        backoff.expo,
        exceptions,
        max_tries=global_reconnect_times,
        logger="IMCommon:backoff_custom",
        backoff_log_level=logging.WARNING,
        raise_on_giveup=False,
    )


class SQLFilter(BaseModel):
    select: str = 'select event_src__asset, event_src__host, COUNT(*) AS cnt from datalake."data".{table_name} '
    event_filter: str = (
        "where {event_filter} and \"__emitted_at\" > timestamp '{time_with_delta}' "
    )
    uuid_filter: str = " and event_src__asset in ({uuids}) "
    group: str = "group by event_src__asset, event_src__host"


class EventsWorkerDL(EventsWorker):
    """Класс запроса событий из SIEM"""

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
        super().__init__(
            settings,
            logger,
            policies,
            auth,
            pol_blacklist,
            pol_whitelist,
            pol_spec,
            mand_pols,
            assets,
        )
        guru_logger.remove()
        guru_logger.add(sys.stderr, level=self.settings.logging_level)
        self.pool: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=self.settings.max_threads_for_siem_api
        )
        dl_settings = {}
        for extra_name, extra_value in settings.__pydantic_extra__.items():
            if extra_name.startswith("datalake_"):
                dl_settings[extra_name[9:]] = extra_value
        self.dl_client: DatalakeClient = DatalakeClient(
            DatalakeSettings(
                upload_chunk_size=self.settings.datalake_chunk_size, **dl_settings
            )
        )
        self.logger.info("EventsWorkerDL initialized")

    async def work(self, group_id, asset_ids, out_folder):
        group_tasks = []
        # не тащим Audit Events Hack потому что аудит выполняется EDR, а значит
        # https://gitlab.ptsecurity.com/dzaripov/am_scripts/-/tree/master/siem_scans?ref_type=heads
        # не применим и таких событий никогда не будет
        if not self.policies.rebuilt_policies:
            return [], {}
        time_from_value = (
            datetime.now(UTC) - timedelta(hours=self.settings.time_delta_hours)
        ).strftime("%Y-%m-%d %H:%M:%S")
        for index, policy in enumerate(self.policies.rebuilt_policies):
            file_name, file_path = _file_dumper(policy, out_folder)
            policy["file_name"] = file_name
            filter_dl = SQLFilter()
            filter_dl.select = filter_dl.select.format(
                table_name=self.settings.dl_table
            )
            filter_dl.event_filter = filter_dl.event_filter.format(
                event_filter=self.check_filter(policy["filter"]),
                time_with_delta=time_from_value,
            )
            if asset_ids:
                string_ids = ""
                for asset_id in asset_ids:
                    string_ids += f"UUID '{asset_id}',"
                filter_dl.uuid_filter = filter_dl.uuid_filter.format(
                    uuids=string_ids.rstrip(",")
                )
                filter_new = (
                    filter_dl.select
                    + filter_dl.event_filter
                    + filter_dl.uuid_filter
                    + filter_dl.group
                )
            else:
                filter_new = filter_dl.select + filter_dl.event_filter + filter_dl.group
            policy["sql"] = filter_new
            with file_path.open("w", encoding="utf-8") as out_file:
                self.logger.debug(f"Create {file_path} with info for SQL query")
                json.dump(policy, out_file, ensure_ascii=False, indent=4)
            group_tasks.append(
                asyncio.create_task(self._starter_get_data_by_sql(policy, out_folder))
            )
        results = await tqdm.gather(*group_tasks)
        for index, policy in enumerate(self.policies.rebuilt_policies):
            if "host_ids" not in self.policies.rebuilt_policies[index].keys():
                self.policies.rebuilt_policies[index].update(
                    {"host_ids": results[index]}
                )
            else:
                for host in results[index]:
                    self.policies.rebuilt_policies[index]["host_ids"].update(
                        {host: results[index][host]}
                    )
        with (out_folder / "!out_all.json").open("w", encoding="utf-8") as out_file:
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
                self.policies.small_policies, out_file, ensure_ascii=False, indent=4
            )
        return self.policies

    def check_filter(self, old_filter: str):
        new_filter = old_filter
        if new_filter.find("'") != -1 and new_filter.find('"') != -1:
            self.logger.error(
                f"strange filter {old_filter}. Have double and single quotes"
            )
        new_filter = new_filter.replace('"', "'")
        spliter = re.split("\\s(and|or)\\s", new_filter)
        for index, one_filter in enumerate(spliter):
            # нужно заменить точки в именах таксономических полей, но не вырезать их внутри значений
            if one_filter.find(".") != -1:
                quote_position = one_filter.find("'")
                if quote_position != -1:
                    spliter[index] = (
                        one_filter[:quote_position].replace(".", "__")
                        + one_filter[quote_position:]
                    )
                else:
                    # СИЕМ пережует проверку существования как and any_field, а SQL нет
                    if one_filter.startswith("not "):
                        one_filter = f"{one_filter} IS NULL"
                    else:
                        one_filter = f"{one_filter} IS NOT NULL"
                    spliter[index] = one_filter.replace(".", "__")

        return " ".join(spliter)

    async def _starter_get_data_by_sql(
        self, policy: dict[Any], out_dir: Path
    ) -> dict[str, dict[str, float | list[str]]]:
        """Ассинхронный стартер для SELECT запросов"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.pool, self._sql_worker, policy, out_dir)

    def _sql_worker(
        self,
        policy: dict[str, str | dict[str, dict[str, float | list[str]]]],
        out_dir: Path,
    ):
        start_time = time.time()
        data_by_sql = self.get_data_by_sql(policy["sql"])
        if type(data_by_sql) is not pd.DataFrame:
            self.logger.error("get_data_by_sql Backoff final")
            return {}
        lead_time = time.time() - start_time
        self.logger.debug(
            f"Get {data_by_sql.shape[0]} rows for {policy['file_name']}. Lead time SQL: {lead_time} seconds."
        )
        policy["host_ids"] = {}
        for row in data_by_sql.to_dict("records"):
            asset = str(row["event_src__asset"])

            if asset not in policy["host_ids"].keys():
                policy["host_ids"].update(
                    {
                        asset: {
                            "count": row["cnt"],
                            "event_src.host": [row["event_src__host"]],
                        }
                    }
                )
            else:
                policy["host_ids"][asset]["count"] += row["cnt"]
                policy["host_ids"][asset]["event_src.host"].append(
                    row["event_src__host"]
                )
        with (out_dir / policy["file_name"]).open("w", encoding="utf-8") as out_file:
            json.dump(policy, out_file, ensure_ascii=False, indent=4)
        return policy["host_ids"]

    @get_backoff_decorator()
    @backoff.on_exception(
        backoff.expo,
        DBAPIError,
        max_tries=1,
        logger="IMCommon:get_data_by_sql",
        backoff_log_level=logging.WARNING,
        raise_on_giveup=False,
    )
    def get_data_by_sql(self, sql_query: str) -> pd.DataFrame:
        """
        Функция для получения данных согласно SQL.

        Args:
            sql_query: SQL запрос для получения данных

        Returns: Данные в формате pd.DataFrame

        """
        self.logger.debug(sql_query)
        df = pd.DataFrame(self.dl_client.run_query(sql_query))
        return df


def _file_dumper(all_policy: dict[Any], out_dir: Path):
    file_name = all_policy["name"].replace(" ", "_")
    temp_policy = deepcopy(all_policy)
    temp_policy["host_ids"] = {}
    if "list_value" in temp_policy.keys():
        file_name += "_" + temp_policy["list_value"]
    file_name = re.sub("[^a-zA-Zа-яА-я_ 0-9-]", "_", file_name)
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
    file_path = out_dir / filter_file_name
    return file_name, file_path
