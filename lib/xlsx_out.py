from pathlib import Path
from pickle import FLOAT

import xlsxwriter
from datetime import datetime, timezone
import json
import re
import sys
from typing import Optional
old_python = False
if sys.version.find('3.7.') == 0:
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
        kb_struct: Any
    else:
        worksheets_line_number: dict[str, int]
        worksheets_line_starter: dict[str, int]
        kb_view: dict[str, dict[str, int]]
        kb_struct: dict[str, list[dict[str, str | dict[str, str | bool]]]]
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
            need_up_file: bool
    ):
        """Активация класса"""
        self.main_out_path = main_out_path
        self._set_workbook_path(main_out_path, mpx, need_up_file)
        self.workbook = xlsxwriter.Workbook(self.workbook_path)
        self.worksheets = {"simple": self.workbook.add_worksheet("simple"), "FULL": self.workbook.add_worksheet("FULL")}
        self._add_formats()
        self.delta_hours = delta_hours
        self.worksheets_line_number = {"simple": 0, "FULL": 0}
        self.start_col_second = 7
        self.kb_view = {}

        self.kb_struct = {}
        try:
            kb_struct_path = (self.main_out_path.parent / "KB_struct_uninstalled.json")
            if kb_struct_path.exists():
                with kb_struct_path.open('r', encoding='utf-8') as kb_struct_file:
                    self.kb_struct = json.load(kb_struct_file)
        except Exception as Err:
            print(Err)
            pass


        self.worksheets_line_starter = {}

    def _set_workbook_path(self, main_out_path: Path, mpx: str, need_up_file: bool):
        current_time = datetime.now().strftime("%Y-%m-%d")
        if need_up_file:
            file_name = current_time + "-" + main_out_path.name + "-" + mpx
            if len(file_name) > 65:
                file_name = file_name[:65]
            file_name = re.sub('[^a-zA-Zа-яА-я_ 0-9-]', '_', file_name)
            file_name += ".xlsx"
            self.workbook_path = (main_out_path.parent / file_name)
        else:
            self.workbook_path = (main_out_path / (current_time + "-" + mpx + ".xlsx"))

    def _add_formats(self):
        self.formats.white = self.workbook.add_format({"pattern": 1, "border": 1, "bg_color": "white"})
        self.formats.white_bold = self.workbook.add_format({"pattern": 1, "border": 1, "bg_color": "white", "bold": True})
        self.formats.percents = self.workbook.add_format({"pattern": 1, "border": 1, "bg_color": "white", "num_format": "0%"})
        self.formats.white_wrapped = self.workbook.add_format({"pattern": 1, "border": 1, "bg_color": "white", "text_wrap": True})
        self.formats.red = self.workbook.add_format({"pattern": 1, "border": 1, "bg_color": "red"})
        self.formats.green = self.workbook.add_format({"pattern": 1, "border": 1, "bg_color": "green"})
        self.formats.light_green = self.workbook.add_format({"pattern": 1, "border": 1, "bg_color": "#ceffbc"})
        self.formats.cyan = self.workbook.add_format({"pattern": 1, "border": 2, "bg_color": "cyan"})
        self.formats.yellow = self.workbook.add_format({"pattern": 1, "border": 1, "bg_color": "yellow"})

    def add_start_info(self, small_policies: Optional[dict] = None, asset_attrs: Optional[list] = None, asset_filter_comment: Optional[list] = None):
        self._time_to_page(self.worksheets["FULL"], self.delta_hours)
        self.worksheets_line_number["simple"] += 3
        self._time_to_page(self.worksheets["simple"], self.delta_hours)
        self.worksheets_line_number["FULL"] += 3
        if asset_filter_comment:
            self.worksheets["simple"].write(0, 6, "Комментарий из asset_filters", self.formats.white_bold)  # G1
            for index, comment_line in enumerate(asset_filter_comment):
                self.worksheets["simple"].merge_range(index + 1, 6, index + 1, 8, str(comment_line), self.formats.white)
        self._stat_to_simple()
        index = 0
        self.worksheets_line_number["FULL"] += 2
        if asset_attrs:
            self.worksheets["FULL"].write_row(self.worksheets_line_number["FULL"], index, asset_attrs, self.formats.cyan)
            index += len(asset_attrs)
            self.worksheets["FULL"].write_row(self.worksheets_line_number["FULL"], index, ["event_src.host"], self.formats.cyan)
            index += 1
        else:
            self.worksheets["FULL"].write_row(self.worksheets_line_number["FULL"], 0, ["event_src.asset", "event_src.host"],
                                              self.formats.cyan)
            index = 2
        for policy in small_policies.keys():
            self.worksheets["FULL"].merge_range(self.worksheets_line_number["FULL"] - 1, index, self.worksheets_line_number["FULL"] - 1, index + 1,
                                                policy, self.formats.cyan)
            self.worksheets["FULL"].write_row(self.worksheets_line_number["FULL"], index, ["satisfaction", "COUNT"],
                                              self.formats.cyan)
            index += 2
        self.worksheets_line_number["simple"] += 2
        attrs_simple = ["STATUS", "asset_info", "description", "asset_id", "audit_time", "audit_status", "audit_task",
                        "event_src.host", "good_policy", "not all_policy", "empty policies"]
        self.worksheets["simple"].write_row(self.worksheets_line_number["simple"], 0, attrs_simple, self.formats.cyan)
        self.worksheets["simple"].write_row(self.worksheets_line_number["simple"] - 1, len(attrs_simple), ["full attrs for no_asset"],
                                            self.formats.cyan)
        self.len_attrs_simple = len(attrs_simple)
        self.worksheets["simple"].merge_range(self.worksheets_line_number["simple"] - 1, self.len_attrs_simple, self.worksheets_line_number["simple"] - 1,
                                              self.len_attrs_simple - 1 + len(asset_attrs),
                                              "full attrs for no_asset", self.formats.cyan)
        self.worksheets["simple"].write_row(self.worksheets_line_number["simple"], self.len_attrs_simple, asset_attrs,
                                            self.formats.cyan)
        self.worksheets["simple"].autofilter(self.worksheets_line_number["simple"], 0, self.worksheets_line_number["simple"],
                                             self.len_attrs_simple - 1 + len(asset_attrs))

        self.worksheets["simple"].set_row(self.worksheets_line_number["simple"], 20)
        self.worksheets["FULL"].autofilter(self.worksheets_line_number["FULL"], 0, self.worksheets_line_number["FULL"], index - 1)
        self.worksheets_line_number["FULL"] += 1
        self.worksheets_line_number["simple"] += 1

    def _time_to_page(self, sheet: xlsxwriter.workbook.Worksheet, delta_hours):
        sheet.write(0, 0, "Дата отчета", self.formats.white_bold)                            # A1
        sheet.write(0, 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.formats.white)  # B1
        sheet.write(1, 0, "Глубина", self.formats.white_bold)                                # A2
        sheet.write(1, 1, "Часы", self.formats.white_bold)                                   # B2
        sheet.write(1, 2, delta_hours, self.formats.white)                                   # C2
        sheet.write(2, 1, "Дни", self.formats.white_bold)                                    # B3
        sheet.write(2, 2, delta_hours / 24, self.formats.white)                              # C3

    def _stat_to_simple(self):
        self.worksheets["simple"].write_column(4, 0, ["статистика", "Общее", "Активов"], self.formats.white_bold)  # A5
        self.worksheets["simple"].write_row(4, 1, ["Количество", "%"], self.formats.white_bold)  # B5
        self.worksheets["simple"].write(5, 2, "", self.formats.white_bold)  # C6
        self.worksheets["simple"].write_column(8, 0, ["Активы", "Без проблем", "Проблемы аудита", "Проблемы сбора событий"], self.formats.white_bold)  # A9
        self.worksheets["simple"].write_row(8, 1, ["Количество", "% от активов", "% от общего кол-ва"], self.formats.white_bold)  # B9
        self.worksheets_line_number["simple"] += 9

    def prepare_pol_sheets(self, policy_name, policy):
        self.worksheets.update({policy_name: self.workbook.add_worksheet(policy_name)})
        self.worksheets[policy_name].write(0, 0, policy_name, self.formats.cyan)  # A1
        self.worksheets[policy_name].write(1, 0, "Фильтр")  # A2
        self.worksheets_line_number.update({policy_name: 1})
        num_col = 3
        if "list_field" in policy.keys():
            list_attrs = ["event_src.asset", "event_src.host", " / ".join(policy["list_field"]), "COUNT",
                          "pol_num"]
            num_col += 1
        else:
            list_attrs = ["event_src.asset", "event_src.host", "COUNT", "policy_count"]
        self.worksheets[policy_name].write(1, num_col+1, "№ фильтра", self.formats.yellow)
        for index_ef, event_filter in enumerate(policy["filters"]):
            self.worksheets_line_number[policy_name] += 1
            self.worksheets[policy_name].merge_range(
                self.worksheets_line_number[policy_name], 0, self.worksheets_line_number[policy_name], num_col,
                event_filter, self.formats.white
            )
            self.worksheets[policy_name].write(self.worksheets_line_number[policy_name], num_col+1, index_ef+1, self.formats.yellow)
        self.worksheets_line_number[policy_name] += 2
        self.worksheets[policy_name].write_row(self.worksheets_line_number[policy_name], 0, list_attrs, self.formats.cyan)
        self.worksheets_line_starter[policy_name] = self.worksheets_line_number[policy_name]
        self.worksheets[policy_name].autofilter(self.worksheets_line_number[policy_name], 0, self.worksheets_line_number[policy_name], num_col - 1)
        self.worksheets[policy_name].set_column(0, 0, 40)
        self.worksheets[policy_name].set_column(1, 1, 35)
        self.worksheets[policy_name].write_row(self.worksheets_line_number[policy_name], self.start_col_second,
                                               ["event_src.asset", "event_src.host"],
                                               self.formats.cyan)
        self.worksheets[policy_name].write_row(self.worksheets_line_number[policy_name], self.start_col_second + 2,
                                               [str(_ + 1) for _ in range(len(policy["filters"]))],
                                               self.formats.yellow)
        if len(policy["filters"]) > 1:
            self.worksheets[policy_name].merge_range(self.worksheets_line_number[policy_name] - 1, self.start_col_second + 2,
                                                     self.worksheets_line_number[policy_name] -1, self.start_col_second + 1 + len(policy["filters"]),
                                                     "№ фильтра", self.formats.yellow)
        else:
            self.worksheets[policy_name].write(self.worksheets_line_number[policy_name] - 1, self.start_col_second + 2, "№ фильтра", self.formats.yellow)
        self.worksheets[policy_name].autofilter(self.worksheets_line_number[policy_name], 0,
                                                self.worksheets_line_number[policy_name], self.start_col_second + 1 + len(policy["filters"]))
        self.worksheets[policy_name].set_column(self.start_col_second, self.start_col_second, 40)
        self.worksheets[policy_name].set_column(self.start_col_second + 1, self.start_col_second + 1, 35)
        self.kb_view.update({policy_name: {"col": self.start_col_second + 3 + len(policy["filters"]), "row": 1}})
        self.worksheets[policy_name].write(self.kb_view[policy_name]["row"], self.kb_view[policy_name]["col"], "Связанные пакеты экспертизы", self.formats.cyan)
        self.worksheets[policy_name].set_column(self.kb_view[policy_name]["col"], self.kb_view[policy_name]["col"], 80)
        self.worksheets[policy_name].set_column(self.kb_view[policy_name]["col"] + 1,
                                                self.kb_view[policy_name]["col"] + 1, 80)
        self.kb_view[policy_name]["row"] += 1
        if self.kb_struct:
            for kb_pack in policy["KB_packs"]:
                if kb_pack in self.kb_struct.keys():
                    if not self.kb_struct[kb_pack]:
                        self.worksheets[policy_name].write(
                            self.kb_view[policy_name]["row"], self.kb_view[policy_name]["col"], kb_pack,
                            self.formats.green
                        )
                        self.kb_view[policy_name]["row"] += 1
                    else:
                        self.worksheets[policy_name].write_column(
                            self.kb_view[policy_name]["row"], self.kb_view[policy_name]["col"] + 1,
                            self.kb_struct[kb_pack],
                            self.formats.red
                        )
                        len_packs = len(self.kb_struct[kb_pack]) - 1
                        if len_packs > 0:
                            self.worksheets[policy_name].merge_range(
                                self.kb_view[policy_name]["row"], self.kb_view[policy_name]["col"],
                                self.kb_view[policy_name]["row"] + len_packs,
                                self.kb_view[policy_name]["col"],
                                kb_pack, self.formats.red
                            )
                        else:
                            self.worksheets[policy_name].write(
                                self.kb_view[policy_name]["row"], self.kb_view[policy_name]["col"],
                                kb_pack, self.formats.red
                            )
                        self.kb_view[policy_name]["row"] += len_packs + 1
                else:
                    self.worksheets[policy_name].write(
                        self.kb_view[policy_name]["row"], self.kb_view[policy_name]["col"], kb_pack,
                        self.formats.white
                    )
                    self.kb_view[policy_name]["row"] += 1
                    # all_pack_activate_status = 0
                    # for rule in self.kb_struct[kb_pack]:
                    #     if rule["GeneralDeploymentStatus"] == "Installed":
                    #         all_pack_activate_status += 1
                    # if all_pack_activate_status == len(self.kb_struct[kb_pack]):
                    #     temp_color = self.formats.green
                    # elif all_pack_activate_status > 0:
                    #     temp_color = self.formats.yellow
                    # else:
                    #     temp_color = self.formats.red
                    # self.worksheets[policy_name].write(
                    #     self.kb_view[policy_name]["row"], self.kb_view[policy_name]["col"], kb_pack, temp_color
                    # )
                    # self.kb_view[policy_name]["row"] += 1
        else:
            self.worksheets[policy_name].write_column(
                self.kb_view[policy_name]["row"], self.kb_view[policy_name]["col"], policy["KB_packs"],
                self.formats.white
            )
            self.kb_view[policy_name]["row"] += len(policy["KB_packs"])

    def create_asset_dict(self, policies, small_policies, asset_dict):
        index = 0
        old_name = ""
        pol_num = 0
        for policy in policies:
            if policy["name"] == 'Audit Events Hack':
                continue
            one_policy = True if small_policies[policy["name"]]["count"] == 1 else False
            if policy["name"] != old_name:
                index = 0
                old_name = policy["name"]
            pol_num += 1
            for host in policy["host_ids"].keys():
                index += 1
                out_list_attrs = [host, " / ".join(policy["host_ids"][host]["event_src.host"])]
                if "list_field" in policy:
                    out_list_attrs.append(policy["list_value"])
                else:
                    if "list_field" in small_policies[policy["name"]].keys():
                        out_list_attrs.append("")
                out_list_attrs.extend([policy["host_ids"][host]["count"], policy["number"]])
                self.worksheets[policy["name"]].write_row(index + self.worksheets_line_number[policy["name"]], 0,
                                                          out_list_attrs, self.formats.white)
                if not one_policy:
                    if "list_value" in policy.keys():
                        value = {"full_info": {str(policy["number"]) + "_" + policy["list_value"]:
                                                   policy["host_ids"][host]["count"]},
                                 "sum_count": policy["host_ids"][host]["count"],
                                 "satisfaction": "PART"}
                    else:
                        value = {"full_info": {str(policy["number"]):
                                                   policy["host_ids"][host]["count"]},
                                 "sum_count": policy["host_ids"][host]["count"],
                                 "satisfaction": "PART"}
                else:
                    value = {"sum_count": policy["host_ids"][host]["count"],
                             "satisfaction": "YES"}

                if host not in asset_dict:
                    asset_dict.update({host: {
                        "policies": {
                            policy["name"]: value
                        },
                        "names": policy["host_ids"][host]["event_src.host"]
                    }})
                elif "policies" not in asset_dict[host].keys():
                    asset_dict[host].update({"policies": {policy["name"]: value},
                                             "names": policy["host_ids"][host]["event_src.host"]})
                else:
                    if policy["name"] not in asset_dict[host]["policies"]:
                        asset_dict[host]["policies"].update({
                            policy["name"]: value
                        })
                        for host_name in policy["host_ids"][host]["event_src.host"]:
                            if host_name not in asset_dict[host]["names"]:
                                asset_dict[host]["names"].append(host_name)
                    else:
                        if "list_value" in policy.keys():
                            asset_dict[host]["policies"][policy["name"]]["full_info"].update({
                                str(policy["number"]) + "_" + policy["list_value"]: policy["host_ids"][host]["count"]
                            })
                        else:
                            asset_dict[host]["policies"][policy["name"]]["full_info"].update({
                                str(policy["number"]): policy["host_ids"][host]["count"]
                            })
                        asset_dict[host]["policies"][policy["name"]]["sum_count"] += policy["host_ids"][host]["count"]
                        if len(asset_dict[host]["policies"][policy["name"]]["full_info"].keys()) == \
                                small_policies[policy["name"]]["count"]:
                            asset_dict[host]["policies"][policy["name"]]["satisfaction"] = "YES"
                        for host_name in policy["host_ids"][host]["event_src.host"]:
                            if host_name not in asset_dict[host]["names"]:
                                asset_dict[host]["names"].append(host_name)
        if policies[-1]["name"] == "Audit Events Hack":
            for host in policies[-1]["host_ids"].keys():
                asset_dict[host].update({"audit_info": policies[-1]["host_ids"][host]["event_src.host"]})
        return asset_dict

    def work_with_asset_dict(self, small_policies, asset_dict, no_assets, mandatory_policies=None):
        col_sizer = []
        index_row = self.worksheets_line_number["FULL"]
        index_row_no_extra = self.worksheets_line_number["simple"]
        audit_task_len = 10
        max_p_l = 50
        if no_assets:
            for no_asset in no_assets:
                attrs_list, col_sizer, index_col, extra_info, simple_attrs = _asset_info_to_list(no_asset, col_sizer)
                attrs_list.append("")
                index_col += 1
                self.worksheets["FULL"].write_row(index_row, 0, attrs_list, self.formats.white)
                if small_policies:
                    self.worksheets["FULL"].write_row(index_row, index_col,
                                                      ["" for _ in range(len(small_policies.keys()) * 2)],
                                                      self.formats.white)
                self.worksheets["simple"].write_row(index_row_no_extra, 1, ["" for _ in range(self.len_attrs_simple - 1)], self.formats.white)
                self.worksheets["simple"].write_row(index_row_no_extra, self.len_attrs_simple, attrs_list[:-1], self.formats.white)
                self.worksheets["simple"].write(index_row_no_extra, 0, "No asset", self.formats.red)
                index_row += 1
                index_row_no_extra += 1
        for asset in asset_dict.keys():
            index_col = 0
            extra_info = []
            e_host_info = ""
            if "asset_info" not in asset_dict[asset].keys():
                if "names" in asset_dict[asset].keys():
                    e_host_info = " / ".join(asset_dict[asset]["names"])
                self.worksheets["FULL"].write_row(index_row, index_col, [asset, e_host_info], self.formats.white)
                full_simple_attrs = ["", "", asset, "", "", "", e_host_info, "", ""]
                self.worksheets["simple"].write_row(index_row_no_extra, 1, full_simple_attrs, self.formats.white_wrapped)
                full_simple_attrs = full_simple_attrs[:-2]
                index_col = 2
                col_sizer = [35, 30]
            else:
                attrs_list, col_sizer, index_col, extra_info, simple_attrs = _asset_info_to_list(
                    asset_dict[asset]["asset_info"], col_sizer)

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
                self.worksheets["simple"].write_row(index_row_no_extra, 1, full_simple_attrs, self.formats.white)
                full_simple_attrs = full_simple_attrs[:-2]
                index_col += 1
                if extra_info:
                    self.worksheets["FULL"].write_row(index_row, 0, attrs_list, self.formats.light_green)
                    for extra_index, extra in enumerate(extra_info):
                        attrs_list, col_sizer, index_col_1, extra_info_1, simple_attrs_1 = _asset_info_to_list(
                            extra, col_sizer)
                        if extra_info_1:
                            print("ERROR! attrs_list, col_sizer, index_col, extra_info_1 = _asset_info_to_list(extra, "
                                  "col_sizer)\nextra_info_1 not null")
                        attrs_list.append(e_host_info)
                        self.worksheets["FULL"].write_row(index_row + 1 + extra_index, 0, attrs_list,
                                                          self.formats.light_green)
                else:
                    self.worksheets["FULL"].write_row(index_row, 0, attrs_list, self.formats.white)
            pol_out_list = []
            full_policies = []
            part_policies = []
            for policy in small_policies.keys():
                if "policies" in asset_dict[asset].keys() and policy in asset_dict[asset]["policies"].keys():
                    self.worksheets_line_number[policy] += 1
                    pol_out_list.extend([asset_dict[asset]["policies"][policy]["satisfaction"], int(
                        asset_dict[asset]["policies"][policy]["sum_count"])])
                    self.worksheets[policy].write_row(
                        self.worksheets_line_number[policy], self.start_col_second, [asset, e_host_info], self.formats.white)
                    empty_fields = []
                    for index_small_key, small_key in enumerate(small_policies[policy]["small_keys"]):
                        full_info = asset_dict[asset]["policies"][policy].get("full_info")
                        if full_info:
                            if small_key in full_info.keys():
                                color = self.formats.green
                            else:
                                color = self.formats.red
                                empty_fields.append(small_key)
                        else:
                            color = self.formats.green
                        self.worksheets[policy].write(
                            self.worksheets_line_number[policy],
                            self.start_col_second + 2 + index_small_key, small_key, color
                        )
                    if asset_dict[asset]["policies"][policy]["satisfaction"] == "YES":
                        full_policies.append(policy)
                    else:
                        part_policies.append(f'{policy} ({",".join(empty_fields)})')
                else:
                    pol_out_list.extend(["", ""])
            if extra_info:
                for extra_index in range(len(extra_info) + 1):
                    self.worksheets["FULL"].write_row(index_row, index_col, pol_out_list, self.formats.light_green)
                    index_row += 1
                index_row -= 1
            else:
                self.worksheets["FULL"].write_row(index_row, index_col, pol_out_list, self.formats.white)
            full_simple_attrs.append(full_policies)
            full_simple_attrs.append(part_policies)
            full_policies = ", ".join(full_policies)
            part_policies = ", ".join(part_policies)
            simple_status, empty_policies_list = _status_master(full_simple_attrs, list(small_policies.keys()), mandatory_policies)
            empty_policies = ", ".join(empty_policies_list)
            self.worksheets["simple"].write_row(index_row_no_extra, 8, [full_policies, part_policies, empty_policies],
                                                self.formats.white_wrapped)
            if simple_status == "ok":
                self.worksheets["simple"].write(index_row_no_extra, 0, simple_status, self.formats.green)
            else:
                self.worksheets["simple"].write(index_row_no_extra, 0, simple_status, self.formats.red)
            index_row += 1
            index_row_no_extra += 1
        for index, col_size in enumerate(col_sizer):
            if col_size > 67:
                col_size = 67
            self.worksheets["FULL"].set_column(index, index, col_size + 3)
        if col_sizer != [35, 30]:
            self.worksheets["FULL"].set_column(len(col_sizer), len(col_sizer), 40)
        # STATUS - 15, asset_info - 40, description - 30, asset_id - 36, audit_time - 20, audit_status - 13
        # audit_task - audit_task_len (def 10), event_src.host - 40, good_policy,
        # not all_policy - max_p_l (50), empty policies - 20
        col_size_simple = [15, 40, 30, 36, 20, 13, audit_task_len, 40, max_p_l, max_p_l, 20]
        for index, col_size in enumerate(col_size_simple):
            self.worksheets["simple"].set_column(index, index, col_size)
        self.worksheets["simple"].write_formula(
            5, 1,
            f'=COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "*")',
            self.formats.white
        )  # B6
        self.worksheets["simple"].write_formula(
            6, 1,
            f'=B6 - COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "No asset")',
            self.formats.white
        )  # B7
        self.worksheets["simple"].write_formula(6, 2, '=B7/B6', self.formats.percents)  # C7
        self.worksheets["simple"].write_formula(
            9, 1,
            f'=COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "ok")',
            self.formats.white
        )  # B10
        self.worksheets["simple"].write_formula(
            10, 1,
            f'=COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "*audit*")',
            self.formats.white
        )  # B11
        self.worksheets["simple"].write_formula(
            11, 1,
            f'=COUNTIF(A{str(self.worksheets_line_number["simple"] + 1)}:A{str(index_row_no_extra)}, "*os events*")',
            self.formats.white
        )  # B12
        for row in range(3):
            self.worksheets["simple"].write_formula(row + 9, 2, f'=B{str(row + 10)}/$B$7', self.formats.percents)
            self.worksheets["simple"].write_formula(row + 9, 3, f'=B{str(row + 10)}/$B$6', self.formats.percents)

        for policy in self.worksheets_line_starter.keys():
            if self.worksheets_line_starter[policy] == self.worksheets_line_number[policy]:
                self.worksheets[policy].hide()


if __name__ == "__main__":
    miccc = MonitorXlsxWriter(Path(r"out"), "asdsd", 1, False, {"w os h": 1, "w os k": 1}, ["@host", "host.fqdn"], ["asda", "qqq"])
    policie1s = {
        "w os h": {
            "filters": ["event_src.title = \"windows\"", "event_src.title = \"askdljasiokd\""], "list_field": ["msgid"]
        },
        "w os k": {
            "filters": ["event_src.title = \"windowaaas\"", "event_src.title = \"iouwqeioqwueioqw\""], "list_field": ["msgid"]
        }
    }
    for i in policie1s.keys():
        miccc.prepare_pol_sheets(i, policie1s[i])
    miccc.workbook.close()


def _asset_info_to_list(asset_info, col_sizer):
    # TODO а тут ли место этой функции?
    len_col = 0
    index_col = 0
    attrs_list = []
    extra_info = []
    simple_attrs = ["", "", "", "", ""]

    # print(json.dumps(asset_info, indent=4, ensure_ascii=False))
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


def _status_master(full_simple_attrs, small_attrs, mandatory_policies = None):
    simple_pol_st_os = False
    simple_audit_st = False
    empty_policies = []
    if small_attrs[-1] == 'Audit Events Hack':
        small_attrs = small_attrs[:-1]
    if len(full_simple_attrs) < 9:
        return "not 8"
    if not full_simple_attrs[0]:
        simple_audit_st = True
    elif full_simple_attrs[4] == "UpToDate":
        simple_audit_st = True
    elif (full_simple_attrs[4] == "NotDefined" or full_simple_attrs[4] is None) and full_simple_attrs[3]:
        audit_date = datetime.strptime(full_simple_attrs[3], "%Y-%m-%dT%H:%M:%S%z")
        if (datetime.now(timezone.utc) - audit_date).days < 28:
            simple_audit_st = True
    if small_attrs:
        if full_simple_attrs[7]:
            if small_attrs[0].find("w os Win") != -1 and full_simple_attrs[7][0].find("w os Win") != -1:
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
