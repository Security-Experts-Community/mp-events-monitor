from json import JSONDecodeError
from pathlib import Path
import json
import re
import logging
from typing import Union
import sys
old_python = False
if sys.version.find('3.7.') == 0:
    old_python = True
    from typing import Any
class EventPolicies:
    if old_python:
        policies_by_file: Any
        filtered_policies: Any
        rebuilt_policies: Any
        small_policies: Any
    else:
        policies_by_file: dict[str, list[dict[str, Union[str, list[str]]]]]
        filtered_policies: dict[str, list[dict[str, str | list[str]]]]
        rebuilt_policies: list[dict[str, str | int | dict[str, dict[str, Union[str, int]]]]]
        small_policies: dict[str, dict[str, str | list[Union[str,int]]]]
    policies_path: Path
    loger: logging.Logger

    def __init__(self, policies_path: Path, logger: logging.Logger):
        self.policies_path = policies_path
        self.logger = logger
        try:
            self.logger.info(f"Loading policies from: {self.policies_path} for checking")
            with self.policies_path.open('r', encoding='utf-8') as policies_file:
                self.policies_by_file = json.load(policies_file)
        except JSONDecodeError as Err:
            self.logger.error(f'Policies file {self.policies_path} is not JSON: {Err}')
            exit(1)

    def check_policies(self):
        all_good = True

        if not self.policies_by_file:
            self.logger.error("Policies file is empty")
            exit(1)
        for policy in self.policies_by_file.keys():
            if type(self.policies_by_file[policy]) is not list:
                self.logger.error(f"Policy `{policy}` is not a list")
                all_good = False
            else:
                for policy_filter in self.policies_by_file[policy]:
                    if type(policy_filter) is not dict:
                        self.logger.error(f"Policy `{policy}` filter `{policy_filter}` is not a dictionary")
                        all_good = False
                    else:
                        list_fields = []
                        for taxonomy_field in policy_filter.keys():
                            if type(taxonomy_field) is not str:
                                self.logger.error(f"Policy `{policy}` filter `{policy_filter}` taxonomy field "
                                                  f"`{taxonomy_field}` is not a string")
                                all_good = False
                            elif re.sub("[a-zA-Z0-9_.]", "", taxonomy_field) != "":
                                bad_symbols = re.sub("[a-zA-Z0-9_.]", "", taxonomy_field)
                                self.logger.error(f"Policy `{policy}` filter `{policy_filter}` taxonomy field "
                                                  f"`{taxonomy_field}` have unexpected symbols "
                                                  f"`{bad_symbols}`")
                                all_good = False
                            if type(policy_filter[taxonomy_field]) is list:
                                list_fields.append(taxonomy_field)
                                for list_field in policy_filter[taxonomy_field]:
                                    if type(list_field) not in [str, int]:
                                        self.logger.error(
                                            f"Policy `{policy}` filter `{policy_filter}` have taxonomy field "
                                            f"`{taxonomy_field}` and it is list but `{list_field}` is not a"
                                            f" string or int."
                                        )
                                        all_good = False
                            if (policy_filter[taxonomy_field]
                                    and type(policy_filter[taxonomy_field]) not in [str, int, bool, list]):
                                self.logger.error(f"Policy `{policy}` filter `{policy_filter}` by taxonomy field "
                                                  f"`{taxonomy_field}` search value in unexpected datatype. "
                                                  f"Expecting datatypes [str, int, bool, None, list]")
                                all_good = False
                        if len(list_fields) > 1:
                            self.logger.error(f"Policy `{policy}` filter `{policy_filter}` have two or more "
                                              f"list_fields {list_fields}. Maximum list field is 1. "
                                              f"Matrix multiplication does not make sense in the case of filtering."
                                              f" Rewrite filter.")
                            all_good = False
        if all_good:
            self.logger.info("All policies is good")
        else:
            exit(1)

    def _policies_rebuild(self):
        self.rebuilt_policies = []
        self.small_policies = {}
        for policy in self.filtered_policies.keys():
            for index, pol_filter in enumerate(self.filtered_policies[policy]):
                event_filter = ""
                list_field = ""
                list_field_list = []
                for taxonomy_field in pol_filter:
                    if type(pol_filter[taxonomy_field]) in [str, int]:
                        event_filter += taxonomy_field + " = " + '"' + pol_filter[taxonomy_field] + '" and '
                    elif type(pol_filter[taxonomy_field]) is bool or pol_filter[taxonomy_field] is None:
                        if pol_filter[taxonomy_field]:
                            event_filter += taxonomy_field + " and "
                        else:
                            event_filter += " not " + taxonomy_field + " and "
                    elif type(pol_filter[taxonomy_field]) is list:
                        list_field = taxonomy_field
                        list_field_list = pol_filter[taxonomy_field]
                if list_field:
                    if policy not in self.small_policies.keys():
                        self.small_policies.update({policy: {"count": 0, "filters": [], "full_filters": [],
                                                             "list_field": [list_field],
                                                             "small_keys": []}})
                    else:
                        if "list_field" not in self.small_policies[policy].keys():
                            self.small_policies[policy].update({"list_field": [list_field], "small_keys": []})
                        elif list_field not in self.small_policies[policy]["list_field"]:
                            self.small_policies[policy]["list_field"].append(list_field)
                    for field in list_field_list:
                        temp_event_filter = event_filter
                        temp_event_filter += list_field + " = " + '"' + field + '"'
                        full_filter = ("filter({}) | select(time, event_src.host) | sort(time desc) | group(key: "
                                       "[event_src.asset, event_src.host], agg: COUNT(*) as Cnt) | sort(Cnt desc) | "
                                       "limit(100000)").format(temp_event_filter)
                        self.rebuilt_policies.append(
                            {"name": policy, "number": index, "list_field": list_field, "list_value": field,
                             "filter": temp_event_filter, "full_filter": full_filter})
                        self.small_policies[policy]["count"] += 1
                        self.small_policies[policy]["filters"].append(temp_event_filter)
                        self.small_policies[policy]["full_filters"].append(full_filter)
                        self.small_policies[policy]["small_keys"].append(f"{index}_{field}")
                else:
                    event_filter = event_filter[:-5]  # delete and in the end
                    full_filter = ("filter({}) | select(time, event_src.host) | sort(time desc) | group(key: "
                                   "[event_src.asset, event_src.host], agg: COUNT(*) as Cnt) | sort(Cnt desc) | "
                                   "limit(100000)").format(event_filter)
                    self.rebuilt_policies.append({"name": policy, "number": index, "filter": event_filter,
                                                  "full_filter": full_filter})
                    if policy not in self.small_policies.keys():
                        self.small_policies.update({policy: {"count": 1, "filters": [event_filter],
                                                             "full_filters": [full_filter],
                                                             "small_keys": [str(index)]}})
                    else:
                        self.small_policies[policy]["count"] += 1
                        self.small_policies[policy]["filters"].append(event_filter)
                        self.small_policies[policy]["full_filters"].append(full_filter)
                        self.small_policies[policy]["small_keys"].append(str(index))

    def filter_policies(self, pol_blacklist, pol_whitelist=None, pol_spec=None):
        self.filtered_policies = {}
        for policy in self.policies_by_file.keys():
            if type(self.policies_by_file[policy]) is not list:
                raise ValueError
            for policy_filter in self.policies_by_file[policy]:
                if type(policy_filter) is not dict:
                    raise ValueError
            if pol_blacklist:
                if type(pol_blacklist) is str:
                    if re.search(pol_blacklist, policy):
                        self.filtered_policies.update({policy: self.policies_by_file[policy]})
                elif type(pol_blacklist) is list:
                    for reg_black in pol_blacklist:
                        if re.search(reg_black, policy):
                            self.filtered_policies.update({policy: self.policies_by_file[policy]})
                else:
                    self.logger.error(
                        "problem in addition_self.filtered_policies one of default_politics_blacklist is not a list")
                    exit(1)
            elif pol_whitelist:
                if type(pol_whitelist) is str:
                    if not re.search(pol_whitelist, policy):
                        self.filtered_policies.update({policy: self.policies_by_file[policy]})
                elif type(pol_whitelist) is list:
                    for reg_black in pol_whitelist:
                        if not re.search(reg_black, policy):
                            self.filtered_policies.update({policy: self.policies_by_file[policy]})
                else:
                    self.logger.error(
                        "problem in addition_self.filtered_policies one of default_politics_whitelist is not a list")
                    exit(1)
            else:
                self.filtered_policies.update({policy: self.policies_by_file[policy]})
        if pol_spec:
            for add_pol in pol_spec.keys():
                self.filtered_policies.update({add_pol: pol_spec[add_pol]})
        if not self.filtered_policies:
            self.logger.info(f"No filtered_policies")
        self._policies_rebuild()


def policies_rebuild(policies):
    re_policies = []
    small_policies = {}
    for policy in policies.keys():
        for index, pol_filter in enumerate(policies[policy]):
            event_filter = ""
            list_field = ""
            list_field_list = []
            for taxonomy_field in pol_filter:
                if type(pol_filter[taxonomy_field]) in [str, int]:
                    event_filter += taxonomy_field + " = " + '"' + pol_filter[taxonomy_field] + '" and '
                elif type(pol_filter[taxonomy_field]) is bool or pol_filter[taxonomy_field] is None:
                    if pol_filter[taxonomy_field]:
                        event_filter += taxonomy_field + " and "
                    else:
                        event_filter += " not " + taxonomy_field + " and "
                elif type(pol_filter[taxonomy_field]) is list:
                    if list_field:
                        print("two or more attributes is list in " + policy + " [" + str(index) + "]:",
                              list_field, "and", pol_filter[taxonomy_field])
                        print("algorithm can't multiply lists, rewrite by two elements in policy")
                        raise ValueError
                    else:
                        list_field = taxonomy_field
                        list_field_list = pol_filter[taxonomy_field]
                else:
                    print("strange datatype in " + policy + " [" + str(index) + "]:", taxonomy_field, "-",
                          type(pol_filter[taxonomy_field]))
                    print(pol_filter[taxonomy_field])
                    raise ValueError
            if list_field:
                if policy not in small_policies.keys():
                    small_policies.update({policy: {"count": 0, "filters": [], "full_filters": [],
                                                    "list_field": [list_field]}})
                else:
                    if "list_field" not in small_policies[policy].keys():
                        small_policies[policy].update({"list_field": [list_field]})
                    elif list_field not in small_policies[policy]["list_field"]:
                        small_policies[policy]["list_field"].append(list_field)
                for field in list_field_list:
                    temp_event_filter = event_filter
                    if type(field) not in [str, int]:
                        print("strange datatype element of lis " + policy + " [" + str(index) + "]:", list_field, "has",
                              field, "-", type(field))
                        raise ValueError
                    temp_event_filter += list_field + " = " + '"' + field + '"'
                    full_filter = ("filter({}) | select(time, event_src.host) | sort(time desc) | group(key: "
                                   "[event_src.asset, event_src.host], agg: COUNT(*) as Cnt) | sort(Cnt desc) | "
                                   "limit(100000)").format(temp_event_filter)
                    re_policies.append({"name": policy, "number": index, "list_field": list_field, "list_value": field,
                                        "filter": temp_event_filter, "full_filter": full_filter})
                    small_policies[policy]["count"] += 1
                    small_policies[policy]["filters"].append(temp_event_filter)
                    small_policies[policy]["full_filters"].append(full_filter)
            else:
                event_filter = event_filter[:-5]  # delete and in the end
                full_filter = ("filter({}) | select(time, event_src.host) | sort(time desc) | group(key: "
                               "[event_src.asset, event_src.host], agg: COUNT(*) as Cnt) | sort(Cnt desc) | "
                               "limit(100000)").format(event_filter)
                re_policies.append({"name": policy, "number": index, "filter": event_filter,
                                    "full_filter": full_filter})
                if policy not in small_policies.keys():
                    small_policies.update({policy: {"count": 1, "filters": [event_filter],
                                                    "full_filters": [full_filter]}})
                else:
                    small_policies[policy]["count"] += 1
                    small_policies[policy]["filters"].append(event_filter)
                    small_policies[policy]["full_filters"].append(full_filter)
    # print(json.dumps(re_policies, indent=4, ensure_ascii=False))
    # print(json.dumps(small_policies, indent=4, ensure_ascii=False))
    return re_policies, small_policies


def event_policies_check(asset_filter):
    policies_path = Path("configs/event_policies.json")
    policies = {}
    while True:
        try:
            if policies_path.is_file():
                with policies_path.open('r', encoding='utf-8') as policies_file:
                    temp_policies = json.load(policies_file)
                for policy in temp_policies.keys():
                    if type(temp_policies[policy]) is not list:
                        raise ValueError
                    for policy_filter in temp_policies[policy]:
                        if type(policy_filter) is not dict:
                            raise ValueError
                    if asset_filter:
                        if "default_politics_blacklist" in asset_filter.keys():
                            if not asset_filter["default_politics_blacklist"]:
                                policies.update({policy: temp_policies[policy]})
                            elif type(asset_filter["default_politics_blacklist"]) is str:
                                if re.search(asset_filter["default_politics_blacklist"], policy):
                                    policies.update({policy: temp_policies[policy]})
                            elif type(asset_filter["default_politics_blacklist"]) is list:
                                for reg_black in asset_filter["default_politics_blacklist"]:
                                    if re.search(reg_black, policy):
                                        policies.update({policy: temp_policies[policy]})
                            else:
                                print("problem in addition_policies one of default_politics_blacklist is not a list")
                                exit(1)
                        elif ("default_politics_whitelist" in asset_filter.keys()
                              and asset_filter["default_politics_whitelist"]):
                            if type(asset_filter["default_politics_whitelist"]) is str:
                                if not re.search(asset_filter["default_politics_whitelist"], policy):
                                    policies.update({policy: temp_policies[policy]})
                            elif type(asset_filter["default_politics_whitelist"]) is list:
                                for reg_black in asset_filter["default_politics_whitelist"]:
                                    if not re.search(reg_black, policy):
                                        policies.update({policy: temp_policies[policy]})
                            else:
                                print("problem in addition_policies one of default_politics_whitelist is not a list")
                                exit(1)
                        else:
                            policies.update({policy: temp_policies[policy]})
                    else:
                        policies.update({policy: temp_policies[policy]})
                if asset_filter and "specific_politics" in asset_filter.keys():
                    for add_pol in asset_filter["specific_politics"].keys():
                        policies.update({add_pol: asset_filter["specific_politics"][add_pol]})
            # print(json.dumps(policies, indent=4, ensure_ascii=False))

            if not policies:
                print("no policies")
                return {}, [], {}
            else:
                new_policies, small_policies = policies_rebuild(policies)
                return policies, new_policies, small_policies
        except ValueError:
            input("some problems in configs/event_policies.json, please check it and press enter")
            event_policies_check(asset_filter)
