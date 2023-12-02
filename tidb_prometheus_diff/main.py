#!/usr/bin/python
# encoding=utf8
import os
import logging as log
import yaml
from tabulate import tabulate
import argparse


def get_alerts(filename):
    """
    :param filename: yaml文件名，需要符合一定形式
    :return:返回alert的元组列表
    """
    base_name = os.path.basename(filename)
    result = []
    result_map = yaml.load(open(filename, "r").read(), yaml.FullLoader)
    for each_item in result_map["groups"][0]["rules"]:
        result.append((each_item["alert"], each_item["expr"], each_item["for"], base_name))
    return result


def get_alerts_diff(a1, a2):
    """
    判断两个告警表达式的差异
    :param a1:第一个告警表达式元组列表
    :param a2: 第二个告警表达式元组列表
    :return: (alert,r1_expr,r2_expr,r1_for,r2_for)，当不存在时则记为NotFound
    """
    a1_map = {}
    a2_map = {}
    for r in a1:
        if r[0] not in a1_map:
            a1_map[r[0]] = r
    for r in a2:
        if r[0] not in a2_map:
            a2_map[r[0]] = r

    # alert在a1中存在，a2中不存在的情况
    result_list = []
    alert1 = list(map(lambda x: x[0], a1))
    alert2 = list(map(lambda x: x[0], a2))
    alert1_except_alert2 = [x for x in alert1 if x not in alert2]
    for each_alert in alert1_except_alert2:
        result_list.append(
            (a1_map[each_alert][3], each_alert, "NotFound", "√", "", "")
        )
    # alert 在a2中存在,a1中不存在
    alert2_except_alert1 = [x for x in alert2 if x not in alert1]
    for each_alert in alert2_except_alert1:
        result_list.append(
            (a2_map[each_alert][3], each_alert, "√", "NotFound", "", "")
        )

    # alert在a1、a2中都存在
    alert1_union_alert2 = [x for x in alert1 if x in alert2]
    for each_alert in alert1_union_alert2:
        r1 = a1_map[each_alert]
        r2 = a2_map[each_alert]
        if "".join(r1[1].split()) != "".join(r2[1].split()) or r1[2] != r2[2]:
            result_list.append(
                (a1_map[each_alert][3], r[0], r1[1], r2[1], r1[2], r2[2])
            )
    return result_list


def get_all_alerts_diff(path1, path2,
                        filter=["tikv.rules.yml", "tidb.rules.yml", "blacker.rules.yml", "ticdc.rules.yml",
                                "pd.rules.yml", "node.rules.yml", "tiflash.rules.yml"]):
    result_list = []
    yaml_fields1 = sorted([f for f in os.listdir(path1) if f in filter])
    yaml_fields2 = sorted([f for f in os.listdir(path2) if f in filter])
    if len(yaml_fields1) != len(yaml_fields2):
        raise Exception(f"路径:{path1},{path2}中待对比的yaml文件名需相同")
    for i in range(len(yaml_fields1)):
        if yaml_fields1[i] != yaml_fields2[i]:
            raise Exception(f"路径:{path1},{path2}中待对比的yaml文件名需相同")
        log.debug(f"filename:{yaml_fields1[i]}")
        file_name = yaml_fields1[i]
        abs_f1 = os.path.join(path1, file_name)
        abs_f2 = os.path.join(path2, file_name)
        a1 = get_alerts(abs_f1)
        a2 = get_alerts(abs_f2)
        for each_a in get_alerts_diff(a1, a2):
            result_list.append(each_a)
    return result_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="prometheus文件对比工具")
    parser.add_argument("-a", "--path1", help="prometheus配置文件路径")
    parser.add_argument("-b", "--path2", help="prometheus配置文件路径")
    args = parser.parse_args()
    # log.basicConfig(level=log.DEBUG)
    data = get_all_alerts_diff(args.path1, args.path2)
    headers = ["filename", "alert", "a(expr)", "b(expr)", "a(for)", "b(for)"]
    print(tabulate(data, headers=headers, tablefmt="simple_grid"))
