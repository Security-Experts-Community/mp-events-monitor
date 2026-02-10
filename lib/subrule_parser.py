import json
import os

import yaml


def localize_pack(pack, loc_dict):
    for item in loc_dict["categories"]:
        if item["id"] == pack:
            return item["name"]

    return pack


with open(r"D:\Work\repo\knowledgebase\_extra\slices.yaml") as exclude_file:
    cfg = yaml.safe_load(exclude_file)
    file_blacklist = cfg["KnowledgebaseSlices"]["SIEM-Public"]["Excludes"]["Files"]
    for i in range(len(file_blacklist)):
        file_blacklist[i] = file_blacklist[i].replace("packages/", "")

with open("configs\\packages_names.json", "r", encoding="utf-8") as f_packs:
    packs_names = json.load(f_packs)

print(file_blacklist)

meta_corr = []

for dirpath, dirnames, filenames in os.walk("D:\\Work\\repo\\knowledgebase\\packages"):
    for filename in filenames:
        if filename.endswith(".co"):
            need_insert = True
            for bad_pack in file_blacklist:
                if bad_pack in dirpath:
                    need_insert = False
            if need_insert:
                filepath = os.path.join(dirpath, "metainfo.yaml")
                if os.path.exists(filepath):
                    meta_corr.append(filepath)

subrules_to_rules = {}
for item in meta_corr:
    curr_rule = item.split("\\")[-2]
    curr_pack = item.split("\\")[-4]
    with open(item, "r", encoding="utf-8") as f:
        rule_meta = yaml.safe_load(f)

    if "ContentRelations" in rule_meta:
        try:
            dependencies = rule_meta["ContentRelations"]["Uses"]["SIEMKB"]["Auto"][
                "CorrelationRules"
            ]
            # print(rule_meta["ContentRelations"]["Uses"]["SIEMKB"]["Auto"]["CorrelationRules"])
            for subrule in dependencies:
                tmp = dependencies[subrule]
                if tmp not in subrules_to_rules.keys():
                    subrules_to_rules[tmp] = {curr_pack: [curr_rule]}
                else:
                    if curr_pack in subrules_to_rules[tmp].keys():
                        subrules_to_rules[tmp][curr_pack].append(curr_rule)
                    else:
                        subrules_to_rules[tmp][curr_pack] = [curr_rule]
        except:
            pass

with open("configs\\subrules.json", "w", encoding="utf-8") as f_out:
    f_out.write(json.dumps(subrules_to_rules, indent=4, ensure_ascii=False))


with open("configs\\event_policies.json", "r", encoding="utf-8") as f_in:
    queries = json.load(f_in)

queries_copy = queries

for item in queries:
    for filter in queries[item]:
        for pack in queries[item][filter]:
            for rule in queries[item][filter][pack]:
                if rule in subrules_to_rules.keys():
                    for pack_name in subrules_to_rules[rule]:
                        if pack_name == pack:
                            for corr in subrules_to_rules[rule][pack_name]:
                                if corr not in queries_copy[item][filter][pack]:
                                    queries_copy[item][filter][pack].append(corr)

queries_copy = {}
for item in queries:
    queries_copy[item] = queries[item].copy()
    queries_copy[item] = {}

    for filter in queries[item]:
        queries_copy[item][filter] = {}
        print(file_blacklist)

        for pack in queries[item][filter]:
            new_pack = localize_pack(pack, packs_names)
            queries_copy[item][filter][new_pack] = queries[item][filter][pack]

        for bad in file_blacklist:
            if bad in queries_copy[item][filter].keys():
                del queries_copy[item][filter][bad]


with open("configs\\event_policies.json", "w", encoding="utf-8") as f_out_2:
    queries = f_out_2.write(json.dumps(queries_copy, indent=4, ensure_ascii=False))
