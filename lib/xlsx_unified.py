import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import xlsxwriter

from lib.xlsx_out import MonitorXlsxWriter, WriterStatistic


class XlsxUnited(MonitorXlsxWriter):
    def __init__(
        self,
        main_out_path: Path,
        mpx: str,
        delta_hours: int,
        logger: logging.Logger,
    ):
        """Активация класса"""
        self.logger = logger
        self.main_out_path = main_out_path
        current_time = datetime.now().strftime("%Y-%m-%d")
        self.workbook_path = main_out_path / (
            "!_READ_ME_FIRST_" + current_time + "-final_stat-" + mpx + ".xlsx"
        )
        self.workbook = xlsxwriter.Workbook(self.workbook_path)
        self.worksheets = {
            "all_assets": self.workbook.add_worksheet("all_assets"),
            "all_no_assets": self.workbook.add_worksheet("all_no_assets"),
            "bad_assets": self.workbook.add_worksheet("bad_assets"),
        }
        self._add_formats()
        self.delta_hours = delta_hours
        self.worksheets_line_number = {
            "all_assets": 0,
            "all_no_assets": 0,
            "bad_assets": 0,
        }
        self.statistics: WriterStatistic = WriterStatistic()

    def add_united_start_info(self, asset_attrs: Optional[list] = None):
        self._time_to_page(self.worksheets["all_assets"], self.delta_hours)
        self.worksheets_line_number["all_assets"] += 3
        self._stat_to_simple("all_assets")
        col_size_simple = [
            15,  # STATUS
            20,  # reports
            40,  # asset_info
            30,  # description
            36,  # asset_id
            20,  # audit_time
            15,  # audit_status
            15,  # edr_on_host
            40,  # event_src.host
            50,  # good_policy
            50,  # not all_policy
            40,  # empty policies
        ]
        for index, col_size in enumerate(col_size_simple):
            self.worksheets["all_assets"].set_column(index, index, col_size)
        self.worksheets["all_assets"].write_row(
            self.worksheets_line_number["all_assets"], 0, asset_attrs, self.formats.cyan
        )
        self.worksheets["all_assets"].autofilter(
            self.worksheets_line_number["all_assets"],
            0,
            self.worksheets_line_number["all_assets"],
            len(asset_attrs) - 1,
        )
        self.worksheets_line_number["all_assets"] += 1

    def write_assets(self, all_assets: dict[str, dict[str, str | list[str]]]):
        for asset_id, asset_info in all_assets.items():
            self.statistics.asset += 1
            col_number = 0
            for key, value in asset_info.items():
                write_format = self.formats.white_wrapped
                value_for_write = ""
                if type(value) is str:
                    value_for_write = value
                    if key == "STATUS":
                        if value == "ok":
                            write_format = self.formats.green
                            self.statistics.ok += 1
                        else:
                            write_format = self.formats.red
                            if value == "no os events":
                                self.statistics.no_os_events += 1
                            elif value == "no audit":
                                self.statistics.no_audit += 1
                            elif value == "no audit, no os events":
                                self.statistics.no_audit_no_os_event += 1
                            else:
                                self.logger.warning(
                                    f"Status {value} unexpected in {asset_id}."
                                )
                    elif key == "edr_on_host":
                        if value == "Good EDR events":
                            write_format = self.formats.green
                        elif value == "No policies":
                            write_format = self.formats.yellow
                        else:
                            write_format = self.formats.red
                elif type(value) is list:
                    value_for_write = ", ".join(value)
                self.worksheets["all_assets"].write(
                    self.worksheets_line_number["all_assets"],
                    col_number,
                    value_for_write,
                    write_format,
                )
                col_number += 1
            self.worksheets_line_number["all_assets"] += 1
        total_found_hosts = self.statistics.asset + self.statistics.no_asset
        self.worksheets["all_assets"].write(
            5, 2, total_found_hosts, self.formats.white
        )  # C6
        self.worksheets["all_assets"].write(
            6, 2, self.statistics.asset, self.formats.white
        )  # C7
        self.worksheets["all_assets"].write(
            6, 1, self.statistics.asset / total_found_hosts, self.formats.percents
        )  # B7
        self.worksheets["all_assets"].write(
            9,
            1,
            (self.statistics.ok + self.statistics.no_os_events) / total_found_hosts,
            self.formats.percents,
        )
        self.worksheets["all_assets"].write(
            9, 2, self.statistics.ok + self.statistics.no_os_events, self.formats.white
        )
        self.worksheets["all_assets"].write(
            10,
            1,
            (self.statistics.ok + self.statistics.no_audit) / total_found_hosts,
            self.formats.percents,
        )
        self.worksheets["all_assets"].write(
            10, 2, self.statistics.ok + self.statistics.no_audit, self.formats.white
        )

    def write_no_assets(self, no_assets):
        self.statistics.no_asset = len(no_assets)
        self.worksheets["all_no_assets"].write_row(
            self.worksheets_line_number["all_no_assets"],
            0,
            ["Статистика", "Количество", self.statistics.no_asset],
            self.formats.white_bold,
        )
        self.worksheets_line_number["all_no_assets"] += 2
        self.worksheets["all_no_assets"].write_row(
            self.worksheets_line_number["all_no_assets"],
            0,
            ["STATUS", "report"],
            self.formats.cyan,
        )
        self.worksheets["all_no_assets"].write_row(
            self.worksheets_line_number["all_no_assets"],
            2,
            [str(i) for i in range(18)],
            self.formats.cyan,
        )
        self.worksheets["all_no_assets"].autofilter(
            self.worksheets_line_number["all_no_assets"],
            0,
            self.worksheets_line_number["all_no_assets"],
            19,
        )
        self.worksheets_line_number["all_no_assets"] += 1
        col_size_simple = [
            15,  # STATUS
            20,  # reports
        ]
        for index, col_size in enumerate(col_size_simple):
            self.worksheets["all_no_assets"].set_column(index, index, col_size)
        col_sizer = []
        for no_asset in no_assets:
            (
                attrs_list,
                col_sizer,
                index_col,
                extra_info,
                simple_attrs,
            ) = self._asset_info_to_list(no_asset, col_sizer)
            attrs_list.append("")
            index_col += 1
            self.worksheets["all_no_assets"].write_row(
                self.worksheets_line_number["all_no_assets"],
                1,
                attrs_list,
                self.formats.white,
            )
            self.worksheets["all_no_assets"].write(
                self.worksheets_line_number["all_no_assets"],
                0,
                "No asset",
                self.formats.red,
            )
            self.worksheets_line_number["all_no_assets"] += 1

    def write_bad_assets(self, bad_assets):
        self.statistics.bad_assets = len(bad_assets)
        ws = self.worksheets["bad_assets"]
        line = self.worksheets_line_number["bad_assets"]

        ws.write_row(
            line,
            0,
            ["Статистика", "Количество", self.statistics.bad_assets],
            self.formats.white_bold,
        )
        line += 2

        ws.write_row(line, 0, ["STATUS", "Asset name", "Reason"], self.formats.cyan)
        ws.autofilter(line, 0, line, 2)
        line += 1

        ws.set_column(0, 0, 40)  # STATUS
        ws.set_column(1, 1, 60)  # Host
        ws.set_column(2, 2, 45)  # Причина

        for uid, info in bad_assets.items():
            host, reason = next(iter(info.items()))
            ws.write(line, 0, uid, self.formats.red)
            ws.write(line, 1, host, self.formats.white)
            ws.write(line, 2, reason, self.formats.white)
            line += 1

        self.worksheets_line_number["bad_assets"] = line
