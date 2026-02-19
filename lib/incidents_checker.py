from datetime import datetime, timedelta, timezone

import requests

try:
    from .settings_checker import Settings
except:
    from settings_checker import Settings
try:
    from .get_token import MPXAuthenticator
except:
    from get_token import MPXAuthenticator

import asyncio
import logging

import aiohttp
from tqdm.asyncio import tqdm


class Inc_Checker:
    """Класс запроса событий из SIEM"""

    settings: Settings
    logger: logging.Logger
    auth: MPXAuthenticator
    semaphore: asyncio.Semaphore

    def __init__(self, settings, logger, auth):
        self.settings = settings
        self.logger = logger
        self.auth = auth
        self.semaphore = asyncio.Semaphore(self.settings.max_threads_for_siem_api)

    def iso_utc_millis(self, dt):
        iso = dt.isoformat(timespec="milliseconds")
        return iso.replace("+00:00", "Z")

    def get_time_one_week_ago(self):
        now_utc = datetime.now(timezone.utc)
        week_ago = now_utc - timedelta(weeks=1)
        return self.iso_utc_millis(week_ago)

    def get_info_about_inc(self):
        query = {
            "offset": 0,
            "limit": 500,
            "groups": {"filterType": "no_filter"},
            "timeFrom": self.get_time_one_week_ago(),
            "timeTo": None,
            "filterTimeType": "creation",
            "filter": {
                "select": [
                    "key",
                    "name",
                    "category",
                    "type",
                    "status",
                    "created",
                    "assigned",
                ],
                "where": 'status = "Closed"',
                "orderby": [
                    {"field": "created", "sortOrder": "descending"},
                    {"field": "status", "sortOrder": "ascending"},
                    {"field": "severity", "sortOrder": "descending"},
                ],
            },
            "queryIds": ["all_incidents"],
        }

        url = "https://{}/api/v2/incidents".format(self.settings.mpx_host)
        resp = requests.session().post(
            url=url,
            headers=self.auth.headers,
            verify=False,
            json=query,
            cookies=self.auth.cookies,
        )
        if resp.status_code == 200:
            return resp.json()
        else:
            raise RuntimeError(f"GET content failed: {resp.status_code} – {resp.text}")

    async def check_single_inc(self, inc_guid):
        async with self.semaphore:
            url = "https://{}/api/incidentsReadModel/incidents/{}".format(
                self.settings.mpx_host, inc_guid
            )
            async with aiohttp.ClientSession(
                headers=self.auth.headers, cookies=self.auth.cookies
            ) as session:
                try:
                    async with session.get(url, verify_ssl=False) as response:
                        if response.status == 200:
                            resp = await response.json()
                            return resp if resp.get("source") == "user" else None
                        else:
                            self.logger.error(
                                f"GET content failed for {inc_guid}: {response.status} – {await response.text()}"
                            )
                            return None
                except aiohttp.ClientError as e:
                    self.logger.error(f"AIOHTTP error for {inc_guid}: {e}")
                    return None

    async def check_all_inc(self, list_of_inc):
        incidents = list_of_inc["incidents"]
        results = []
        with tqdm(total=len(incidents), desc="Checking Incidents") as pbar:
            tasks = [
                asyncio.create_task(self.check_single_inc(inc["id"]))
                for inc in incidents
            ]
            for future in asyncio.as_completed(tasks):  # Process as they complete
                result = await future
                if result is not None:
                    results.append(result)
                pbar.update(1)

        return results
