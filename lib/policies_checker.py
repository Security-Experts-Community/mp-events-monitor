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
        mandatory_policies: Any
    else:
        policies_by_file: dict[str, list[dict[str, Union[str, list[str]]]]]
        filtered_policies: dict[str, list[dict[str, str | list[str]]]]
        rebuilt_policies: list[dict[str, str | int | dict[str, dict[str, Union[str, int]]]]]
        small_policies: dict[str, dict[str, str | list[Union[str,int]]]]
        mandatory_policies: list[str]
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

    def filter_policies(self, pol_blacklist=None, pol_whitelist=None, pol_spec=None, mand_pols=None):
        self.filtered_policies = {}
        self.mandatory_policies = []
        if type(pol_blacklist) is str:
            pol_blacklist = [pol_blacklist]
        elif type(pol_blacklist) is not list:
            self.logger.error(
                "problem in addition_self.filtered_policies one of default_politics_blacklist is not a list. Clear "
                "default_politics_blacklist")
            pol_blacklist = []
        if type(pol_whitelist) is str:
            pol_whitelist = [pol_whitelist]
        elif not pol_whitelist:
            pol_whitelist = []
        elif type(pol_whitelist) is not list:
            self.logger.error(
                "problem in addition_self.filtered_policies one of default_politics_whitelist is not a list. Clear "
                "default_politics_whitelist")
            pol_whitelist = []
        if pol_blacklist != []:
            for policy in self.policies_by_file.keys():
                if pol_blacklist:
                    for reg_black in pol_blacklist:
                        if re.search(reg_black, policy):
                            not_in_whitelist = True
                            if pol_whitelist:
                                for reg_white in pol_whitelist:
                                    if re.search(reg_white, policy):
                                        not_in_whitelist = False
                            if not_in_whitelist:
                                self.filtered_policies.update({policy: self.policies_by_file[policy]})
                            if mand_pols:
                                for mand_pol in mand_pols:
                                    if re.search(mand_pol, policy):
                                        self.mandatory_policies.append(policy)
                            break
                elif pol_whitelist:
                    not_in_whitelist = True
                    for reg_white in pol_whitelist:
                        if re.search(reg_white, policy):
                            not_in_whitelist = False
                    if not_in_whitelist:
                        self.filtered_policies.update({policy: self.policies_by_file[policy]})
                else:
                    self.filtered_policies.update({policy: self.policies_by_file[policy]})

        if pol_spec:
            for add_pol in pol_spec.keys():
                self.filtered_policies.update({add_pol: pol_spec[add_pol]})
                if add_pol not in self.mandatory_policies:
                    self.mandatory_policies.append(add_pol)
        if not self.filtered_policies:
            self.logger.info(f"No filtered_policies")
        self._policies_rebuild()
