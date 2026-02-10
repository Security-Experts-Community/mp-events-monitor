import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import xlsxwriter

old_python = False
if sys.version.find("3.7.") == 0:
    from typing import Any


class MonitorXlsxWriter:
    class MainFormats:
        white: xlsxwriter.workbook.Format
        white_bold: xlsxwriter.workbook.Format
        percents: xlsxwriter.workbook.Format
        white_wrapped: xlsxwriter.workbook.Format
        red: xlsxwriter.workbook.Format
        green: xlsxwriter.workbook.Format
        light_green: xlsxwriter.workbook.Format
        cyan: xlsxwriter.workbook.Format
        yellow: xlsxwriter.workbook.Format
        orange: xlsxwriter.workbook.Format

    workbook: xlsxwriter.Workbook
    if old_python:
        worksheets: Any
    else:
        worksheets: dict[str, xlsxwriter.workbook.Worksheet]

    formats: MainFormats = MainFormats()
    if old_python:
        worksheets_line_number: Any
        worksheets_line_starter: Any
        kb_view: Any
        kb_uninstalled: Any
        kb_installed: Any
        table_mapping: Any
    else:
        worksheets_line_number: dict[str, int]
        worksheets_line_starter: dict[str, int]
        kb_view: dict[str, dict[str, int]]
        kb_uninstalled: dict[str, list[dict[str, str | dict[str, str | bool]]]]
        kb_installed: dict[
            str,
            list[
                dict[str, str | int | bool | None | dict[str, str | int | bool | None]]
            ],
        ]
        table_mapping: dict[str, list[dict[str, str]] | None]
    workbook_path: Path
    delta_hours: int
    len_attrs_simple: int
    start_col_second: int
    main_out_path: Path

    def __init__(
        self,
        main_out_path: Path,
        mpx: str,
        delta_hours: int,
        need_up_file: bool,
        logger: logging.Logger,
    ):
        """Активация класса"""
        self.logger = logger
        self.main_out_path = main_out_path
        self._set_workbook_path(main_out_path, mpx, need_up_file)
        self.workbook = xlsxwriter.Workbook(self.workbook_path)
        self.worksheets = {
            "simple": self.workbook.add_worksheet("simple"),
            "FULL": self.workbook.add_worksheet("FULL"),
        }
        self._add_formats()
        self.delta_hours = delta_hours
        self.worksheets_line_number = {"simple": 0, "FULL": 0}
        self.start_col_second = 7
        self.kb_view = {}
        self.kb_uninstalled = {}
        try:
            kb_uninstalled_path = (
                self.main_out_path.parent / "KB_struct_uninstalled.json"
            )
            if kb_uninstalled_path.exists():
                with kb_uninstalled_path.open(
                    "r", encoding="utf-8"
                ) as kb_uninstalled_file:
                    self.kb_uninstalled = json.load(kb_uninstalled_file)
        except Exception as Err:
            self.logger.warning(
                f"No file {self.main_out_path.parent} / KB_struct_uninstalled.json"
            )
            self.logger.warning(f"Full Exception {Err}")
            pass

        self.kb_installed = {}
        try:
            kb_installed_path = self.main_out_path.parent / "KB_struct.json"
            if kb_installed_path.exists():
                with kb_installed_path.open("r", encoding="utf-8") as kb_installed_file:
                    self.kb_installed = json.load(kb_installed_file)
        except Exception as Err:
            self.logger.warning(f"No file {self.main_out_path.parent} / KB_struct.json")
            self.logger.warning(f"Full Exception {Err}")
            pass

        self.table_mapping = {}
        try:
            table_mapping_path = self.main_out_path.parent / "table_mapping_filled.json"
            if table_mapping_path.exists():
                with table_mapping_path.open(
                    "r", encoding="utf-8"
                ) as table_mapping_file:
                    self.table_mapping = json.load(table_mapping_file)
        except Exception as Err:
            self.logger.warning(
                f"No file {self.main_out_path.parent} / table_mapping_filled.json"
            )
            self.logger.warning(f"Full Exception {Err}")
            pass

        self.kb_check = {}
        self.worksheets_line_starter = {}

    def _set_workbook_path(self, main_out_path: Path, mpx: str, need_up_file: bool):
        current_time = datetime.now().strftime("%Y-%m-%d")
        if need_up_file:
            file_name = current_time + "-" + main_out_path.name + "-" + mpx
            if len(file_name) > 65:
                file_name = file_name[:65]
            file_name = re.sub("[^a-zA-Zа-яА-я_ 0-9-]", "_", file_name)
            file_name += ".xlsx"
            self.workbook_path = main_out_path.parent / file_name
        else:
            self.workbook_path = main_out_path / (current_time + "-" + mpx + ".xlsx")

    def _add_formats(self):
        # TODO переделать этот мусорный код в pydantic чтобы цвета элементы класса рисовались динамически
        self.formats.white = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "white"}
        )
        self.formats.white_bold = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "white", "bold": True}
        )
        self.formats.percents = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "white", "num_format": "0%"}
        )
        self.formats.white_wrapped = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "white", "text_wrap": True}
        )
        self.formats.red = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "#FF8A8A"}
        )
        self.formats.green = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "#B8F299"}
        )
        self.formats.light_green = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "#ceffbc"}
        )
        self.formats.cyan = self.workbook.add_format(
            {"pattern": 1, "border": 2, "bg_color": "cyan"}
        )
        self.formats.yellow = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "#FDFF8C"}
        )
        self.formats.orange = self.workbook.add_format(
            {"pattern": 1, "border": 1, "bg_color": "#FFD087"}
        )

    def add_start_info(
        self,
        small_policies: Optional[dict] = None,
        asset_attrs: Optional[list] = None,
        asset_filter_comment: Optional[list] = None,
    ):
        self._time_to_page(self.worksheets["FULL"], self.delta_hours)
        self.worksheets_line_number["simple"] += 3
        self._time_to_page(self.worksheets["simple"], self.delta_hours)
        self.worksheets_line_number["FULL"] += 3
        if asset_filter_comment:
            self.worksheets["simple"].write(
                0, 6, "Комментарий из asset_filters", self.formats.white_bold
            )  # G1
            for index, comment_line in enumerate(asset_filter_comment):
                self.worksheets["simple"].merge_range(
                    index + 1, 6, index + 1, 8, str(comment_line), self.formats.white
                )
        self._stat_to_simple()
        index = 0
        self.worksheets_line_number["FULL"] += 2
        if asset_attrs:
            self.worksheets["FULL"].write_row(
                self.worksheets_line_number["FULL"],
                index,
                asset_attrs,
                self.formats.cyan,
            )
            index += len(asset_attrs)
            self.worksheets["FULL"].write_row(
                self.worksheets_line_number["FULL"],
                index,
                ["event_src.host"],
                self.formats.cyan,
            )
            index += 1
        else:
            self.worksheets["FULL"].write_row(
                self.worksheets_line_number["FULL"],
                0,
                ["event_src.asset", "event_src.host"],
                self.formats.cyan,
            )
            index = 2
        for policy in small_policies.keys():
            self.worksheets["FULL"].merge_range(
                self.worksheets_line_number["FULL"] - 1,
                index,
                self.worksheets_line_number["FULL"] - 1,
                index + 1,
                policy,
                self.formats.cyan,
            )
            self.worksheets["FULL"].write_row(
                self.worksheets_line_number["FULL"],
                index,
                ["satisfaction", "COUNT"],
                self.formats.cyan,
            )
            index += 2
        self.worksheets_line_number["simple"] += 2
        attrs_simple = [
            "STATUS",
            "asset_info",
            "description",
            "asset_id",
            "audit_time",
            "audit_status",
            "audit_task",
            "event_src.host",
            "good_policy",
            "not all_policy",
            "empty policies",
        ]
        self.worksheets["simple"].write_row(
            self.worksheets_line_number["simple"], 0, attrs_simple, self.formats.cyan
        )
        self.worksheets["simple"].write_row(
            self.worksheets_line_number["simple"] - 1,
            len(attrs_simple),
            ["full attrs for no_asset"],
            self.formats.cyan,
        )
        self.len_attrs_simple = len(attrs_simple)
        self.worksheets["simple"].merge_range(
            self.worksheets_line_number["simple"] - 1,
            self.len_attrs_simple,
            self.worksheets_line_number["simple"] - 1,
            self.len_attrs_simple - 1 + len(asset_attrs),
            "full attrs for no_asset",
            self.formats.cyan,
        )
        self.worksheets["simple"].write_row(
            self.worksheets_line_number["simple"],
            self.len_attrs_simple,
            asset_attrs,
            self.formats.cyan,
        )
        self.worksheets["simple"].autofilter(
            self.worksheets_line_number["simple"],
            0,
            self.worksheets_line_number["simple"],
            self.len_attrs_simple - 1 + len(asset_attrs),
        )

        self.worksheets["simple"].set_row(self.worksheets_line_number["simple"], 20)
        self.worksheets["FULL"].autofilter(
            self.worksheets_line_number["FULL"],
            0,
            self.worksheets_line_number["FULL"],
            index - 1,
        )
        self.worksheets_line_number["FULL"] += 1
        self.worksheets_line_number["simple"] += 1

    def _time_to_page(self, sheet: xlsxwriter.workbook.Worksheet, delta_hours):
        sheet.write(0, 0, "Дата отчета", self.formats.white_bold)  # A1
        sheet.write(
            0, 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.formats.white
        )  # B1
        sheet.write(1, 0, "Глубина", self.formats.white_bold)  # A2
        sheet.write(1, 1, "Часы", self.formats.white_bold)  # B2
        sheet.write(1, 2, delta_hours, self.formats.white)  # C2
        sheet.write(2, 1, "Дни", self.formats.white_bold)  # B3
        sheet.write(2, 2, delta_hours / 24, self.formats.white)  # C3

    def _stat_to_simple(self):
        self.worksheets["simple"].write_column(
            4, 0, ["статистика", "Общее", "Активов"], self.formats.white_bold
        )  # A5
        self.worksheets["simple"].write_row(
            4, 1, ["Количество", "%"], self.formats.white_bold
        )  # B5
        self.worksheets["simple"].write(5, 2, "", self.formats.white_bold)  # C6
        self.worksheets["simple"].write_column(
            8,
            0,
            [
                "Критерии",
                "Процент актуального аудита",
                "Хостов с полным покрытием по событиям",
            ],
            self.formats.white_bold,
        )  # A9
        self.worksheets["simple"].write_row(
            8, 1, ["% выполнения"], self.formats.white_bold
        )  # B9
        self.worksheets_line_number["simple"] += 9

    def prepare_pol_sheets(self, policy_name, policy, out_path):
        self.worksheets.update({policy_name: self.workbook.add_worksheet(policy_name)})
        self.worksheets[policy_name].write(0, 0, policy_name, self.formats.cyan)  # A1
        self.worksheets[policy_name].write(1, 0, "Фильтр")  # A2
        self.worksheets_line_number.update({policy_name: 1})
        num_col = 3
        list_attrs = ["event_src.asset", "event_src.host", "COUNT", "№ фильтра"]
        self.worksheets[policy_name].write(
            1, num_col + 1, "№ фильтра", self.formats.yellow
        )
        # пишем фильтр в merge_range и на соседнюю ячейку номер фильтра
        for index_ef, event_filter in enumerate(policy.keys()):
            self.worksheets_line_number[policy_name] += 1
            self.worksheets[policy_name].merge_range(
                self.worksheets_line_number[policy_name],
                0,
                self.worksheets_line_number[policy_name],
                num_col,
                event_filter,
                self.formats.white,
            )
            self.worksheets[policy_name].write(
                self.worksheets_line_number[policy_name],
                num_col + 1,
                index_ef + 1,
                self.formats.yellow,
            )
        self.worksheets_line_number[policy_name] += 2
        # описание левой таблицы
        self.worksheets[policy_name].write_row(
            self.worksheets_line_number[policy_name], 0, list_attrs, self.formats.cyan
        )
        # записываем откуда пойдет информация об источниках событий
        self.worksheets_line_starter[policy_name] = self.worksheets_line_number[
            policy_name
        ]
        self.worksheets[policy_name].autofilter(
            self.worksheets_line_number[policy_name],
            0,
            self.worksheets_line_number[policy_name],
            num_col - 1,
        )
        self.worksheets[policy_name].set_column(0, 0, 40)
        self.worksheets[policy_name].set_column(1, 1, 35)
        # описание правой таблицы (собранной из левой)
        list_for_second_table = ["event_src.asset", "event_src.host"]
        list_for_second_table.extend([str(_ + 1) for _ in range(len(policy.keys()))])
        self.worksheets[policy_name].write_row(
            self.worksheets_line_number[policy_name],
            self.start_col_second,
            list_for_second_table,
            self.formats.cyan,
        )
        # ячейка № фильтра
        if len(policy.keys()) > 1:
            self.worksheets[policy_name].merge_range(
                self.worksheets_line_number[policy_name] - 1,
                self.start_col_second + 2,
                self.worksheets_line_number[policy_name] - 1,
                self.start_col_second + 1 + len(policy.keys()),
                "№ фильтра",
                self.formats.yellow,
            )
        else:
            self.worksheets[policy_name].write(
                self.worksheets_line_number[policy_name] - 1,
                self.start_col_second + 2,
                "№ фильтра",
                self.formats.yellow,
            )
        # я не нашел возможности сделать два разных
        # (It isn't possible to create more than one autofilter per worksheet in Excel)
        self.worksheets[policy_name].autofilter(
            self.worksheets_line_number[policy_name],
            0,
            self.worksheets_line_number[policy_name],
            self.start_col_second + 1 + len(policy.keys()),
        )
        self.worksheets[policy_name].set_column(
            self.start_col_second, self.start_col_second, 40
        )
        self.worksheets[policy_name].set_column(
            self.start_col_second + 1, self.start_col_second + 1, 35
        )
        # накидываем пакеты экспертизы справа
        self.kb_view.update(
            {
                policy_name: {
                    "col": self.start_col_second + 3 + len(policy.keys()),
                    "row": 1,
                }
            }
        )
        self.worksheets[policy_name].write(
            self.kb_view[policy_name]["row"],
            self.kb_view[policy_name]["col"],
            "Связанные пакеты экспертизы",
            self.formats.cyan,
        )
        self.worksheets[policy_name].set_column(
            self.kb_view[policy_name]["col"],
            self.kb_view[policy_name]["col"] + 6,
            60,
        )
        self.kb_view[policy_name]["row"] += 1
        self.prepare_stat_for_kb(policy_name, policy, out_path)

    def prepare_stat_for_kb(self, policy_name, policy, out_path: Path):
        self.kb_check[policy_name] = {}
        if self.kb_installed:
            for index, event_filter in enumerate(policy.keys()):
                for pack in policy[event_filter]:
                    if pack not in self.kb_check[policy_name].keys():
                        self.kb_check[policy_name][pack] = {}
                    for rule in policy[event_filter][pack]:
                        if rule not in self.kb_check[policy_name][pack].keys():
                            install_status = False
                            if pack in self.kb_installed:
                                for kb_rule in self.kb_installed[pack]:
                                    if (
                                        rule == kb_rule["SystemName"]
                                        and len(kb_rule["DeploymentStatuses"]) > 0
                                    ):
                                        install_status = True
                                        break
                            self.kb_check[policy_name][pack][rule] = {
                                "install_status": install_status,
                                "event_filter_indexes": {},
                                # {номер, общий статус, где 0 - событий нет, 1 - частично, 2 во всех активах}
                            }
                        self.kb_check[policy_name][pack][rule][
                            "event_filter_indexes"
                        ].update({str(index): False})

    def create_asset_dict(self, policies, small_policies, asset_dict):
        index = 0
        old_name = ""
        pol_num = 0
        for policy in policies:
            if policy["name"] == "Audit Events Hack":
                continue
            # one_policy = True if small_policies[policy["name"]]["count"] == 1 else False
            if policy["name"] != old_name:
                index = 0
                old_name = policy["name"]
            pol_num += 1
            for host in policy["host_ids"].keys():
                index += 1
                out_list_attrs = [
                    host,
                    " / ".join(policy["host_ids"][host]["event_src.host"]),
                ]
                out_list_attrs.extend(
                    [policy["host_ids"][host]["count"], policy["number"]]
                )
                self.worksheets[policy["name"]].write_row(
                    index + self.worksheets_line_number[policy["name"]],
                    0,
                    out_list_attrs,
                    self.formats.white,
                )
                value = {
                    "full_info": {
                        str(policy["number"]): policy["host_ids"][host]["count"]
                    },
                    "sum_count": policy["host_ids"][host]["count"],
                    "satisfaction": "PART",
                }
                if host not in asset_dict:
                    asset_dict.update(
                        {
                            host: {
                                "policies": {policy["name"]: value},
                                "names": policy["host_ids"][host]["event_src.host"],
                            }
                        }
                    )
                elif "policies" not in asset_dict[host].keys():
                    asset_dict[host].update(
                        {
                            "policies": {policy["name"]: value},
                            "names": policy["host_ids"][host]["event_src.host"],
                        }
                    )
                else:
                    if policy["name"] not in asset_dict[host]["policies"]:
                        asset_dict[host]["policies"].update({policy["name"]: value})
                        for host_name in policy["host_ids"][host]["event_src.host"]:
                            if host_name not in asset_dict[host]["names"]:
                                asset_dict[host]["names"].append(host_name)
                    else:
                        asset_dict[host]["policies"][policy["name"]][
                            "full_info"
                        ].update(
                            {str(policy["number"]): policy["host_ids"][host]["count"]}
                        )
                        asset_dict[host]["policies"][policy["name"]][
                            "sum_count"
                        ] += policy["host_ids"][host]["count"]
                if len(
                    asset_dict[host]["policies"][policy["name"]]["full_info"].keys()
                ) == len(small_policies[policy["name"]].keys()):
                    asset_dict[host]["policies"][policy["name"]]["satisfaction"] = "YES"
                for host_name in policy["host_ids"][host]["event_src.host"]:
                    if host_name not in asset_dict[host]["names"]:
                        asset_dict[host]["names"].append(host_name)
        if policies[-1]["name"] == "Audit Events Hack":
            for host in policies[-1]["host_ids"].keys():
                asset_dict[host].update(
                    {"audit_info": policies[-1]["host_ids"][host]["event_src.host"]}
                )
        return asset_dict

    def polycolor_one_policy(self, policies_statistic, small_policies):
        if policies_statistic and self.kb_installed:
            for policy in policies_statistic.keys():
                if policy == "Audit Events Hack":
                    # ну или брать просто до list(policies_statistic.keys())[:-1]
                    continue
                for index_filter, filter_query in enumerate(policies_statistic[policy]):
                    index_filter_str = str(index_filter)
                    # хотел убрать small_policies, но это не работает, приделываются все ключи из фильтра, хз почему
                    # for pack in self.kb_check[policy].keys():
                    #     for rule in self.kb_check[policy][pack].keys():
                    #         self.kb_check[policy][pack][rule]["event_filter_indexes"][index_filter_str] = (
                    #             policies_statistic)[policy][filter_query]
                    for pack in small_policies[policy][filter_query].keys():
                        for rule in small_policies[policy][filter_query][pack]:
                            self.kb_check[policy][pack][rule]["event_filter_indexes"][
                                index_filter_str
                            ] = policies_statistic[policy][filter_query]
            for policy in self.kb_check.keys():
                green_rules = 0
                total_rules = 0
                filters_like_list = list(policies_statistic[policy].keys())
                for pack in self.kb_check[policy].keys():
                    pack_len = 0
                    pack_color = self.formats.green
                    pack_start_row = self.kb_view[policy]["row"]
                    for rule in self.kb_check[policy][pack].keys():
                        total_rules += 1
                        rule_color = "green"
                        rule_in_list = [rule]
                        if not self.kb_check[policy][pack][rule]["install_status"]:
                            rule_color = "red"
                            pack_color = self.formats.red
                        else:
                            for filter_query_num in self.kb_check[policy][pack][rule][
                                "event_filter_indexes"
                            ]:
                                if not self.kb_check[policy][pack][rule][
                                    "event_filter_indexes"
                                ][filter_query_num]:
                                    rule_color = "yellow"
                                    if pack_color == self.formats.green:
                                        pack_color = self.formats.yellow
                                    rule_in_list.append(
                                        filters_like_list[int(filter_query_num)]
                                    )
                        if rule in self.table_mapping.keys():
                            if self.table_mapping[rule] != []:
                                for smthing in self.table_mapping[rule]:
                                    if (
                                        smthing[list(smthing.keys())[0]]
                                        == "No_manual_changes"
                                    ):
                                        rule_color = "yellow"
                                        if pack_color == self.formats.green:
                                            pack_color = self.formats.yellow
                                        rule_in_list.append(
                                            "{} is empty and needs fill".format(
                                                list(smthing.keys())[0]
                                            )
                                        )
                                    elif (
                                        smthing[list(smthing.keys())[0]]
                                        == "Not Installed!!!"
                                    ):
                                        rule_color = "red"
                                        if pack_color == self.formats.green:
                                            pack_color = self.formats.yellow
                                        rule_in_list.append(
                                            "{} needs to be installed with rule".format(
                                                list(smthing.keys())[0]
                                            )
                                        )

                        self.worksheets[policy].write_row(
                            self.kb_view[policy]["row"],
                            self.kb_view[policy]["col"] + 1,
                            rule_in_list,
                            # проверить можно ли type в проверке, чтобы не использовать __getattribute__
                            self.formats.__getattribute__(rule_color),
                        )
                        self.kb_view[policy]["row"] += 1
                        pack_len += 1
                        if rule_color == "green":
                            green_rules += 1
                    if pack_len == 1:
                        self.worksheets[policy].write(
                            pack_start_row,
                            self.kb_view[policy]["col"],
                            pack,
                            pack_color,
                        )
                    else:
                        self.worksheets[policy].merge_range(
                            pack_start_row,
                            self.kb_view[policy]["col"],
                            self.kb_view[policy]["row"] - 1,
                            self.kb_view[policy]["col"],
                            pack,
                            pack_color,
                        )
                if total_rules != 0:
                    self.worksheets[policy].write(
                        1, 9, round(green_rules / total_rules * 100, 2)
                    )

    def work_with_asset_dict(
        self,
        small_policies,
        asset_dict,
        no_assets,
        out_path: Path,
        mandatory_policies=None,
    ):
        col_sizer = []
        event_quality_array = []
        index_row = self.worksheets_line_number["FULL"]
        index_row_no_extra = self.worksheets_line_number["simple"]
        audit_task_len = 10
        max_p_l = 50
        # TODO переместить вниз, чтобы без ассетные были в таблицы ниже (заранее научить писать вторую таблицу?)
        if no_assets:
            for no_asset in no_assets:
                event_quality_array.append(0)
                (
                    attrs_list,
                    col_sizer,
                    index_col,
                    extra_info,
                    simple_attrs,
                ) = _asset_info_to_list(no_asset, col_sizer)
                attrs_list.append("")
                index_col += 1
                self.worksheets["FULL"].write_row(
                    index_row, 0, attrs_list, self.formats.white
                )
                if small_policies:
                    self.worksheets["FULL"].write_row(
                        index_row,
                        index_col,
                        ["" for _ in range(len(small_policies.keys()) * 2)],
                        self.formats.white,
                    )
                attr_list_with_whitespaces = [
                    "" for _ in range(self.len_attrs_simple - 1)
                ]
                attr_list_with_whitespaces.extend(attrs_list[:-1])
                self.worksheets["simple"].write_row(
                    index_row_no_extra,
                    1,
                    attr_list_with_whitespaces,
                    self.formats.white,
                )
                self.worksheets["simple"].write(
                    index_row_no_extra, 0, "No asset", self.formats.red
                )
                index_row += 1
                index_row_no_extra += 1
        policies_statistic = {}
        if small_policies:
            for policy in small_policies.keys():
                policies_statistic[policy] = {}
                for event_filter in small_policies[policy]:
                    policies_statistic[policy][event_filter] = True
        for asset in asset_dict.keys():
            index_col = 0
            extra_info = []
            e_host_info = ""
            if "asset_info" not in asset_dict[asset].keys():
                if "names" in asset_dict[asset].keys():
                    e_host_info = " / ".join(asset_dict[asset]["names"])
                self.worksheets["FULL"].write_row(
                    index_row, index_col, [asset, e_host_info], self.formats.white
                )
                full_simple_attrs = ["", "", asset, "", "", "", e_host_info, "", ""]
                self.worksheets["simple"].write_row(
                    index_row_no_extra, 1, full_simple_attrs, self.formats.white_wrapped
                )
                full_simple_attrs = full_simple_attrs[:-2]
                index_col = 2
                col_sizer = [35, 30]
            else:
                (
                    attrs_list,
                    col_sizer,
                    index_col,
                    extra_info,
                    simple_attrs,
                ) = _asset_info_to_list(asset_dict[asset]["asset_info"], col_sizer)

                simple_attrs[2] = asset
                if "audit_info" in asset_dict[asset].keys():
                    audit_tasks = "\n".join(asset_dict[asset]["audit_info"])
                    if len(audit_tasks) > audit_task_len:
                        audit_task_len = len(audit_tasks) + 2
                else:
                    audit_tasks = ""
                simple_attrs.append(audit_tasks)
                if "names" in asset_dict[asset].keys():
                    e_host_info = " / ".join(asset_dict[asset]["names"])
                attrs_list.append(e_host_info)
                simple_attrs.append(e_host_info)
                full_simple_attrs = simple_attrs
                full_simple_attrs.extend(["", ""])
                self.worksheets["simple"].write_row(
                    index_row_no_extra, 1, full_simple_attrs, self.formats.white
                )
                full_simple_attrs = full_simple_attrs[:-2]
                index_col += 1
                if extra_info:
                    self.worksheets["FULL"].write_row(
                        index_row, 0, attrs_list, self.formats.light_green
                    )
                    for extra_index, extra in enumerate(extra_info):
                        (
                            attrs_list,
                            col_sizer,
                            index_col_1,
                            extra_info_1,
                            simple_attrs_1,
                        ) = _asset_info_to_list(extra, col_sizer)
                        if extra_info_1:
                            self.logger.warning(
                                "ERROR! attrs_list, col_sizer, index_col, extra_info_1 = _asset_info_to_list(extra, "
                                "col_sizer). extra_info_1 not null"
                            )
                        attrs_list.append(e_host_info)
                        self.worksheets["FULL"].write_row(
                            index_row + 1 + extra_index,
                            0,
                            attrs_list,
                            self.formats.light_green,
                        )
                else:
                    self.worksheets["FULL"].write_row(
                        index_row, 0, attrs_list, self.formats.white
                    )
            pol_out_list = []
            full_policies = []
            part_policies = []
            for policy in small_policies.keys():
                if (
                    "policies" in asset_dict[asset].keys()
                    and policy in asset_dict[asset]["policies"].keys()
                ):
                    self.worksheets_line_number[policy] += 1
                    pol_out_list.extend(
                        [
                            asset_dict[asset]["policies"][policy]["satisfaction"],
                            int(asset_dict[asset]["policies"][policy]["sum_count"]),
                        ]
                    )
                    self.worksheets[policy].write_row(
                        self.worksheets_line_number[policy],
                        self.start_col_second,
                        [asset, e_host_info],
                        self.formats.white,
                    )
                    empty_fields = []
                    for index_filter, filter_query in enumerate(small_policies[policy]):
                        # Зачем мы вообще везде превращаем число в строку. Потому что мы храним это все в словаре.
                        # И иногда дампим в JSON, а JSON не принимает число как ключ.
                        index_filter_str = str(index_filter)
                        full_info = asset_dict[asset]["policies"][policy].get(
                            "full_info"
                        )
                        if full_info:
                            if index_filter_str in full_info.keys():
                                # TODO вот тут проверка что мы вышли за трешхолд и надо красить фиолетовым
                                color = self.formats.green
                                value = full_info[index_filter_str]
                            else:
                                color = self.formats.red
                                empty_fields.append("0")
                                value = 0
                                if policies_statistic[policy][filter_query]:
                                    policies_statistic[policy][filter_query] = False
                        else:
                            color = self.formats.green
                            value = 0
                            self.logger.error("no full_info: in work_with_asset_dict")
                        self.worksheets[policy].write(
                            self.worksheets_line_number[policy],
                            self.start_col_second + 2 + index_filter,
                            value,
                            color,
                        )
                    if asset_dict[asset]["policies"][policy]["satisfaction"] == "YES":
                        full_policies.append(policy)
                    else:
                        part_policies.append(policy)
                else:
                    pol_out_list.extend(["", ""])
                    if mandatory_policies and policy in mandatory_policies:
                        self.worksheets_line_number[policy] += 1
                        if not e_host_info and "asset_info" in asset_dict[asset]:
                            for asset_field in asset_dict[asset]["asset_info"]:
                                if (
                                    type(asset_dict[asset]["asset_info"][asset_field])
                                    is dict
                                    and "name"
                                    in asset_dict[asset]["asset_info"][asset_field]
                                    and "deviceType"
                                    in asset_dict[asset]["asset_info"][asset_field]
                                ):
                                    e_host_info = asset_dict[asset]["asset_info"][
                                        asset_field
                                    ]["name"]
                                    break
                        self.worksheets[policy].write_row(
                            self.worksheets_line_number[policy],
                            self.start_col_second,
                            [asset, e_host_info],
                            self.formats.white,
                        )
                        empty_fields = []
                        for _ in small_policies[policy]:
                            empty_fields.append(0)
                        self.worksheets[policy].write_row(
                            self.worksheets_line_number[policy],
                            self.start_col_second + 2,
                            empty_fields,
                            self.formats.red,
                        )
                        for index_filter, filter_query in enumerate(
                            small_policies[policy]
                        ):
                            policies_statistic[policy][filter_query] = False
            if extra_info:
                for extra_index in range(len(extra_info) + 1):
                    self.worksheets["FULL"].write_row(
                        index_row, index_col, pol_out_list, self.formats.light_green
                    )
                    index_row += 1
                index_row -= 1
            else:
                self.worksheets["FULL"].write_row(
                    index_row, index_col, pol_out_list, self.formats.white
                )
            full_simple_attrs.append(full_policies)
            full_simple_attrs.append(part_policies)
            temp_quality = len(full_policies) + len(part_policies)
            temp_full = len(full_policies)
            full_policies = ", ".join(full_policies)
            part_policies = ", ".join(part_policies)
            simple_status, empty_policies_list = _status_master(
                full_simple_attrs, list(small_policies.keys()), mandatory_policies
            )
            temp_quality += len(empty_policies_list)
            empty_policies = ", ".join(empty_policies_list)
            if temp_quality > 0:
                event_quality_array.append(temp_full / temp_quality)
            else:
                event_quality_array.append(0)
            self.worksheets["simple"].write_row(
                index_row_no_extra,
                8,
                [full_policies, part_policies, empty_policies],
                self.formats.white_wrapped,
            )
            if simple_status == "ok":
                self.worksheets["simple"].write(
                    index_row_no_extra, 0, simple_status, self.formats.green
                )
            else:
                self.worksheets["simple"].write(
                    index_row_no_extra, 0, simple_status, self.formats.red
                )
            index_row += 1
            index_row_no_extra += 1
        with Path(out_path / f"!check_installation_small.json").open(
            "w", encoding="utf-8"
        ) as check_installation:
            json.dump(
                policies_statistic, check_installation, indent=4, ensure_ascii=False
            )
        self.polycolor_one_policy(policies_statistic, small_policies)
        # TODO удалить
        with Path(out_path / f"!check_installation.json").open(
            "w", encoding="utf-8"
        ) as check_installation:
            json.dump(self.kb_check, check_installation, indent=4, ensure_ascii=False)
        for index, col_size in enumerate(col_sizer):
            if col_size > 67:
                col_size = 67
            self.worksheets["FULL"].set_column(index, index, col_size + 3)
        if col_sizer != [35, 30]:
            self.worksheets["FULL"].set_column(len(col_sizer), len(col_sizer), 40)
        # STATUS - 15, asset_info - 40, description - 30, asset_id - 36, audit_time - 20, audit_status - 13
        # audit_task - audit_task_len (def 10), event_src.host - 40, good_policy,
        # not all_policy - max_p_l (50), empty policies - 20
        col_size_simple = [
            15,
            40,
            30,
            36,
            20,
            13,
            audit_task_len,
            40,
            max_p_l,
            max_p_l,
            40,
        ]
        for index, col_size in enumerate(col_size_simple):
            self.worksheets["simple"].set_column(index, index, col_size)
        self.worksheets["simple"].write_formula(
            5,
            1,
            f'=COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "*")',
            self.formats.white,
        )  # B6
        self.worksheets["simple"].write_formula(
            6,
            1,
            f'=B6 - COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "No asset")',
            self.formats.white,
        )  # B7
        self.worksheets["simple"].write_formula(
            6, 2, "=B7/B6", self.formats.percents
        )  # C7

        self.worksheets["simple"].write_formula(
            9,
            1,
            f'=1 - (COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "*audit*") + COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "*No asset*"))/$B$6',
            self.formats.percents,
        )
        if len(event_quality_array) > 0:
            self.worksheets["simple"].write(
                10,
                1,
                sum(event_quality_array) / len(event_quality_array),
                self.formats.percents,
            )

        temp_formula = "=AVERAGE("

        for policy in self.worksheets_line_starter.keys():
            if (
                self.worksheets_line_starter[policy]
                == self.worksheets_line_number[policy]
            ):
                self.worksheets[policy].hide()
            else:
                temp_formula += f"'{policy}'!J2,"

        if temp_formula != "=AVERAGE(":
            temp_formula = temp_formula.rstrip(",") + ")/100"
            self.worksheets["simple"].write_formula(
                11, 1, temp_formula, self.formats.percents
            )
            self.worksheets["simple"].write(
                11,
                0,
                "Эффективность контента при текущей настройке",
                self.formats.white_bold,
            )


def _asset_info_to_list(asset_info, col_sizer):
    # TODO а тут ли место этой функции?
    # TOOTOODUDU
    len_col = 0
    index_col = 0
    attrs_list = []
    extra_info = []
    simple_attrs = ["", "", "", "", ""]
    for attr_index, attr_name in enumerate(asset_info.keys()):
        attr_value = asset_info[attr_name]
        if attr_name == "$assetGridGroupKey":
            continue
        elif type(attr_value) is dict:
            if "name" in attr_value.keys():
                attrs_list.append(attr_value["name"])
                simple_attrs[0] = attr_value["name"]
                if attr_value["name"]:
                    len_col = len(attr_value["name"])
            elif "data" in attr_value.keys():
                if attr_value["totalCount"] == 1:
                    atr_j = str(attr_value["data"][0])
                else:
                    atr_j = str(attr_value["data"])
                attrs_list.append(atr_j)
                if atr_j:
                    len_col = len(atr_j)
            elif "displayName" in attr_value.keys():
                attrs_list.append(attr_value["displayName"])
                if attr_value["displayName"]:
                    len_col = len(attr_value["displayName"])
            elif "primaryType" in attr_value.keys():
                attrs_list.append(attr_value["primaryType"])
                if attr_value["primaryType"]:
                    len_col = len(attr_value["primaryType"])
            elif "value" in attr_value.keys():
                attrs_list.append(attr_value["value"])
                if attr_value["value"]:
                    len_col = len(attr_value["value"])
            else:
                print("ERROR asset_info_to_list")
                print(json.dumps(attr_value, indent=4, ensure_ascii=False))
        elif attr_name != "asset_info_is_answer_again" and type(attr_value) is list:
            attrs_list.append(str(attr_value))
            if attr_value:
                len_col = len(str(attr_value))
        elif attr_name == "asset_info_is_answer_again" and type(attr_value) is list:
            extra_info = attr_value
        else:
            attrs_list.append(attr_value)
            if attr_value:
                len_col = len(attr_value)
            if attr_name.lower().find(".@description") != -1:
                simple_attrs[1] = attr_value
            elif attr_name.lower().find(".@audittime") != -1:
                simple_attrs[3] = attr_value
            elif attr_name.lower().find(".@scanninginfo.status") != -1:
                simple_attrs[4] = attr_value
        if not (attr_name == "asset_info_is_answer_again" and type(attr_value) is list):
            if len(col_sizer) <= attr_index:
                col_sizer.append(len_col)
            elif len_col > col_sizer[attr_index]:
                col_sizer[attr_index] = len_col
            index_col += 1
    return attrs_list, col_sizer, index_col, extra_info, simple_attrs


def _status_master(full_simple_attrs, small_attrs, mandatory_policies=None):
    simple_pol_st_os = False
    simple_audit_st = False
    empty_policies = []
    if small_attrs[-1] == "Audit Events Hack":
        small_attrs = small_attrs[:-1]
    if len(full_simple_attrs) < 9:
        return "not 8"
    if not full_simple_attrs[0]:
        simple_audit_st = True
    elif full_simple_attrs[4] == "UpToDate":
        simple_audit_st = True
    elif (
        full_simple_attrs[4] == "NotDefined" or full_simple_attrs[4] is None
    ) and full_simple_attrs[3]:
        audit_date = datetime.strptime(full_simple_attrs[3], "%Y-%m-%dT%H:%M:%S%z")
        if (datetime.now(timezone.utc) - audit_date).days < 28:
            simple_audit_st = True
    if small_attrs:
        if full_simple_attrs[7]:
            if (
                small_attrs[0].find("w os Win") != -1
                and full_simple_attrs[7][0].find("w os Win") != -1
            ):
                simple_pol_st_os = True
                for pol in small_attrs:
                    if pol.find("w os Win") != -1:
                        if pol not in full_simple_attrs[7]:
                            simple_pol_st_os = False
                            empty = True
                            for not_all_with_msgid in full_simple_attrs[8]:
                                if not_all_with_msgid.find(pol) != -1:
                                    empty = False
                            if empty:
                                empty_policies.append(pol)
                    else:
                        break
            elif small_attrs[0].find(" os ") == 1:
                for pol in full_simple_attrs[7]:
                    if pol.find(" os ") == 1:
                        simple_pol_st_os = True
                        break
        if mandatory_policies:
            for mandatory in mandatory_policies:
                if mandatory not in full_simple_attrs[7]:
                    empty = True
                    for not_all_with_msgid in full_simple_attrs[8]:
                        if not_all_with_msgid.find(mandatory) != -1:
                            empty = False
                    if empty:
                        simple_pol_st_os = False
                        empty_policies.append(mandatory)

        if full_simple_attrs[8]:
            simple_pol_st_os = False
    else:
        simple_pol_st_os = True

    list_to_return = []
    if simple_pol_st_os and simple_audit_st:
        list_to_return.append("ok")
    else:
        if not simple_audit_st:
            list_to_return.append("audit")
        if not simple_pol_st_os:
            list_to_return.append("os events")
    return ", ".join(list_to_return), empty_policies
