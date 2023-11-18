#!/usr/bin/python
# encoding=utf8
from packages import yaml

def get_dict(files):
    result_dict = {}
    for each_file in files:
        try:
            for each_rule in yaml.load(open(each_file),yaml.loader)['groups'][0]['rules']:
                result_dict[each_rule['alert']] = [each_file,each_rule]
        except Exception as e:
            print(e)
    return result_dict

def diff_dict(dict1,dict2):
    diff_list = []
    for k in dict1:
        each_item = [dict1[k][0],k,'e','e','e','e','e']
        if k not in dict2:
            each_item[2] = 'a'
        else:
            #检查每一项是否一致
            all_eq = True
            if dict1[k]['expr'] != dict2[k]['expr']:
                each_item[3] = 'c'
            if dict1[k]['for'] != dict2[k]['for']:
                each_item[4] = 'c'

if __name__ == "__main__":
    fname = r'test_data/tidb.rules.yml'
    dict1 = {}
    f = open(fname)
    outf = yaml.load(f,yaml.Loader)
    for each_rule in outf['groups'][0]["rules"]:
        dict1[each_rule['alert']] = [each_rule,'']
