import json
import logging
import shutil
import time
from pathlib import Path
from typing import Any

import requests

try:
    from lib.settings_checker import Settings
except ImportError:
    from settings_checker import Settings

try:
    from .get_token import MPXAuthenticator
except ImportError:
    from get_token import MPXAuthenticator


class PDQLProcessor:
    def __init__(
        self,
        settings: Settings,
        auth: MPXAuthenticator,
        logger: logging.Logger,
        filter_name: str,
        filter_settings: dict[str, Any],
    ):
        self.settings = settings
        self.auth = auth
        self.logger = logger
        self.filter_name = filter_name
        self.pdql = filter_settings["PDQL"]
        self.comment = filter_settings.get("comment")
        self.group = filter_settings["group"]

    def _create_pdql_token(self, retries: int) -> tuple[dict, list, str, dict]:
        url = f"https://{self.settings.mpx_host}:443/api/assets_temporal_readmodel/v1/assets_grid"
        group = (
            ["00000000-0000-0000-0000-000000000002"]
            if self.group == "-1"
            else [self.group] if isinstance(self.group, str) else self.group
        )
        payload = {
            "pdql": self.pdql,
            "selectedGroupIds": group,
            "includeNestedGroups": True,
            "utcOffset": "+03:00",
        }

        for attempt in range(1, retries + 1):
            self.logger.debug("Attempting to create PDQL token (attempt %d)", attempt)
            try:
                response = self.auth.session.post(
                    url=url,
                    json=payload,
                    headers=self.auth.headers,
                    verify=False,
                    cookies=self.auth.cookies,
                )
                response.raise_for_status()

                data = response.json()
                fields = [f["name"] for f in data["fields"]]
                asset_id_field = ""

                # Find asset_id field
                for field in data["fields"]:
                    name, ftype = field["name"], field["type"]
                    if name == "asset_id":
                        if ftype == "uuid":
                            asset_id_field = "asset_id"
                            break
                        else:
                            self.logger.error(
                                'Field "asset_id" must be of type "uuid".'
                            )
                            raise ValueError("Invalid asset_id type")

                # Fallback: search for assetInfo field
                if not asset_id_field:
                    asset_info_fields = [
                        f["name"] for f in data["fields"] if f["type"] == "assetInfo"
                    ]
                    if len(asset_info_fields) == 1:
                        asset_id_field = asset_info_fields[0]
                    elif len(asset_info_fields) > 1:
                        self.logger.error(
                            "Multiple assetInfo fields found; ambiguous selection."
                        )
                        raise ValueError("Ambiguous assetInfo fields")

                if not asset_id_field:
                    self.logger.error(
                        'No asset_id field found. Consider adding "host.@id as asset_id" to your PDQL.'
                    )
                    raise ValueError("Missing asset_id field")

                return data, fields, asset_id_field, {}

            except (requests.HTTPError, requests.RequestException, ValueError) as e:
                self.logger.warning(
                    "PDQL token creation failed (attempt %d/%d): %s",
                    attempt,
                    retries,
                    e,
                )
                if attempt == retries:
                    return {}, [], "", {}

            time.sleep(5)

        return {}, [], "", {}

    def _fetch_assets(self, token: str, retries: int) -> list[dict]:
        url = f"https://{self.settings.mpx_host}:443/api/assets_temporal_readmodel/v1/assets_grid/data"
        params = {"pdqlToken": token, "offset": 0, "limit": 10_000}
        all_assets: list[dict] = []

        for attempt in range(1, retries + 1):
            try:
                self.logger.debug("Fetching assets (attempt %d)", attempt)
                response = self.auth.session.get(
                    url=url,
                    params=params,
                    headers=self.auth.headers,
                    verify=False,
                    cookies=self.auth.cookies,
                )
                response.raise_for_status()

                data = response.json()
                records = data.get("records", [])
                if not records:
                    break

                all_assets.extend(records)
                self.logger.info(
                    "Fetched %d records (total: %d)", len(records), len(all_assets)
                )

                if len(records) < params["limit"]:
                    break
                params["offset"] += params["limit"]

            except (requests.HTTPError, requests.RequestException) as e:
                self.logger.warning(
                    "Asset fetch failed (attempt %d/%d): %s", attempt, retries, e
                )
                if attempt == retries:
                    break
                time.sleep(5)

        return all_assets

    def run(self) -> list[dict]:
        data, fields, asset_id_field, _ = self._create_pdql_token(
            self.settings.reconnect_times
        )
        if not data:
            self.logger.warning("PDQL token creation failed; skipping filter")
            return []

        token = data["token"]
        self.logger.debug("Received PDQL token: %s", token)
        return self._fetch_assets(token, self.settings.reconnect_times)


class MaxPatrolPDQL:
    def __init__(self, settings, logger, auth):
        self.settings = settings
        self.logger = logger
        self.auth = auth

    def _prepare_pdql(self, pdql: Any) -> str:
        if isinstance(pdql, list):
            return "".join(pdql)
        elif isinstance(pdql, str):
            return pdql
        else:
            self.logger.error("PDQL must be string or list of strings")
            exit(1)

    def asset_filters(self) -> list[dict]:
        total_assets: list[dict] = []
        try:
            with open(self.settings.bad_asset_file, encoding="utf-8") as f:
                filters = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error("Failed to load filters: %s", e)
            return []

        for name, config in filters.items():
            if name == "comments":
                continue

            self.logger.info("Processing filter: %s", name)

            config["group"] = "-1"
            config["PDQL"] = self._prepare_pdql(config["PDQL"])

            processor = PDQLProcessor(
                settings=self.settings,
                auth=self.auth,
                logger=self.logger,
                filter_name=name,
                filter_settings=config,
            )
            assets = processor.run()
            for ass in assets:
                ass["reason"] = config["comment"][0]

            total_assets.extend(assets)

        return total_assets

    def process_bad_assets(self, assets: list[dict]) -> dict[str, str]:
        result: dict[str, str] = {}
        for item in assets:
            if "COMPACTUNIQUE(@Host)" in item:
                for record in item["COMPACTUNIQUE(@Host)"]["data"]:
                    result[record["id"]] = {record["name"]: item.get("reason", "")}
            else:
                host = item.get("@Host", {})
                result[host.get("id", "")] = {
                    host.get("name", ""): item.get("reason", "")
                }

        with (self.settings.out_folder / "bad_assets.json").open(
            "w", encoding="utf-8"
        ) as f:
            f.write(json.dumps(result, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    gl_settings = Settings()
    gl_logger = logging.getLogger("MaxPatrolPDQL_Manual")
    gl_auth = MPXAuthenticator(gl_logger)
    gl_auth.authenticate(gl_settings)
    gl_processor = MaxPatrolPDQL(gl_settings, gl_logger, gl_auth)
    gl_processor.process_bad_assets(gl_processor.asset_filters())
