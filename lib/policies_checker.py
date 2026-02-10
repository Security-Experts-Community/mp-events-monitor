import json
import logging
import re
import sys
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, List, Union

old_python = False
if sys.version.find("3.7.") == 0:
    old_python = True
    from typing import Any


class EventPolicies:
    if old_python:
        policies_by_file: Any
        rebuilt_policies: Any
        small_policies: Any
        mandatory_policies: Any
    else:
        policies_by_file: Dict[str, Dict[str, Dict[str, list[str]]]]
        rebuilt_policies: list[
            dict[str, str | int | dict[str, dict[str, Union[str, int]]]]
        ]
        small_policies: Dict[str, Dict[str, Dict[str, list[str]]]]
        mandatory_policies: list[str]
    policies_path: Path
    loger: logging.Logger

    def __init__(self, policies_path: Path, logger: logging.Logger):
        self.policies_path = policies_path
        self.logger = logger
        try:
            self.logger.info(
                f"Loading policies from: {self.policies_path} for checking"
            )
            with self.policies_path.open("r", encoding="utf-8") as policies_file:
                self.policies_by_file = json.load(policies_file)
        except JSONDecodeError as Err:
            self.logger.error(f"Policies file {self.policies_path} is not JSON: {Err}")
            exit(1)

    def check_policies(self):
        all_good = True

        if not self.policies_by_file:
            self.logger.error("Policies file is empty")
            exit(1)
        for policy in self.policies_by_file.keys():
            if type(self.policies_by_file[policy]) is not dict:
                self.logger.error(f"Policy `{policy}` is not a dict")
                all_good = False
            else:
                for policy_filter in self.policies_by_file[policy].keys():
                    if type(self.policies_by_file[policy][policy_filter]) is not dict:
                        self.logger.error(
                            f"Policy `{policy}` filter {policy_filter} is not a dict"
                        )
                        all_good = False
                    else:
                        for pack in self.policies_by_file[policy][policy_filter]:
                            if (
                                type(self.policies_by_file[policy][policy_filter][pack])
                                is not list
                            ):
                                self.logger.error(
                                    f"Policy `{policy}` filter {policy_filter} pack {pack} is not a list"
                                )
                                all_good = False
                            else:
                                for corr_name in self.policies_by_file[policy][
                                    policy_filter
                                ][pack]:
                                    if type(corr_name) is not str:
                                        self.logger.error(
                                            f"Policy `{policy}` filter {policy_filter} pack {pack} corr {corr_name} "
                                            f"is not a string"
                                        )
                                        all_good = False
        if all_good:
            self.logger.info("All policies is good")
        else:
            exit(1)

    def check_policies_type(self, pol_blacklist=None, pol_whitelist=None):
        if type(pol_blacklist) is str:
            pol_blacklist = [pol_blacklist]
        elif type(pol_blacklist) is not list:
            self.logger.error(
                f"problem in filtering one of default_politics_blacklist is not a list or str. "
                f"Type {type(pol_blacklist)}. Value: {pol_blacklist}"
            )
            self.logger.error("Clear default_politics_blacklist")
            pol_blacklist = []
        if type(pol_whitelist) is str:
            pol_whitelist = [pol_whitelist]
        elif not pol_whitelist:
            pol_whitelist = []
        elif type(pol_whitelist) is not list:
            self.logger.error(
                f"problem in filtering one of default_politics_whitelist is not a list or str. "
                f"Type {type(pol_whitelist)}. Value: {pol_whitelist}"
            )
            self.logger.error("Clear default_politics_whitelist")
            pol_whitelist = []
        return pol_blacklist, pol_whitelist

    def filter_policies(
        self, pol_blacklist=None, pol_whitelist=None, pol_spec=None, mand_pols=None
    ):
        self.mandatory_policies = []
        self.small_policies = {}
        self.rebuilt_policies = []
        pol_blacklist, pol_whitelist = self.check_policies_type(
            pol_blacklist, pol_whitelist
        )
        for policy in self.policies_by_file.keys():
            pol_in_whitelist = False
            pol_in_blacklist = False
            pol_in_mandatory = False
            if mand_pols:
                for mand_pol in mand_pols:
                    if re.search(mand_pol, policy):
                        self.mandatory_policies.append(policy)
                        pol_in_mandatory = True
                        pol_in_blacklist = True
                        break
            if pol_whitelist and not pol_in_mandatory:
                for reg_white in pol_whitelist:
                    if re.search(reg_white, policy):
                        pol_in_whitelist = True
                        break
            if pol_blacklist and not pol_in_mandatory:
                for reg_black in pol_blacklist:
                    if re.search(reg_black, policy):
                        pol_in_blacklist = True
                        break
            if not pol_in_whitelist and (pol_in_blacklist or not pol_blacklist):
                for index, event_filter in enumerate(self.policies_by_file[policy]):
                    full_filter = (
                        f"filter({event_filter}) | select(event_src.host) | group(key: "
                        f"[event_src.asset, event_src.host], agg: COUNT(*) as Cnt) | sort(Cnt desc) | "
                        f"limit(100000)"
                    )
                    self.rebuilt_policies.append(
                        {
                            "name": policy,
                            "number": index,
                            "filter": event_filter,
                            "full_filter": full_filter,
                        }
                    )
                self.small_policies[policy] = self.policies_by_file[policy]
        if pol_spec:
            # можно было бы добавить pol_spec в self.policies_by_file, но тогда переменная поменяется для всех вообще
            for add_pol in pol_spec.keys():
                for index, event_filter in enumerate(pol_spec[add_pol]):
                    full_filter = (
                        f"filter({event_filter}) | select(time, event_src.host) | sort(time desc) | group(key: "
                        f"[event_src.asset, event_src.host], agg: COUNT(*) as Cnt) | sort(Cnt desc) | "
                        f"limit(100000)"
                    )
                    self.rebuilt_policies.append(
                        {
                            "name": add_pol,
                            "number": index,
                            "filter": event_filter,
                            "full_filter": full_filter,
                        }
                    )
                if add_pol not in self.mandatory_policies:
                    self.mandatory_policies.append(add_pol)
                self.small_policies[add_pol] = pol_spec[add_pol]
        if not self.small_policies:
            self.logger.info(f"No filtered_policies")


if __name__ == "__main__":
    test_logger: logging.Logger = logging.getLogger("PoliciesChecker")
    logging.basicConfig(level="DEBUG")
    ep = EventPolicies(Path("../configs/event_policies.json"), test_logger)
    ep.check_policies()
    ep.filter_policies(["w os"], None, {}, ["w os"])
    # ep.filter_policies(["w os"], None, {"w os Win Ess common12": {
    #     "event_src.subsys = \"Security\" and msgid = \"4624\""}}, ["w os"])
    # ep.filter_policies([], ".*", {}, [])
    # ep.filter_policies(["w os"], 'w os Win Ess task', None, ["w os Win sysmon"])
    # ep.filter_policies([], ["os", '^p'], None, ["w os"])
    # print(json.dumps(ep.filtered_policies, indent=4))
    print(ep.small_policies.keys())
