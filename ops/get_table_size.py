#!/usr/bin/python
# encoding=utf8
# 必须开启如下配置文件参数否则计算可能不准（默认情况下即开启）
# rocksdb.defaultcf.enable-compaction-guard=true
# rocksdb.writecf.enable-compaction-guard=true
# tidb.split-table=true
import argparse
import json
import logging as log
import os.path
import subprocess
import sys
import tempfile
import threading

# 判断python的版本
isV3 = float(sys.version[:3]) > 2.7

if not isV3:
    import urllib as request
    from Queue import Queue
else:
    import urllib.request as request
    from queue import Queue

region_queue = Queue(100)  # 内容为（dbname,tabname,region_id）的元组


def command_run(command, use_temp=False, timeout=30):
    def _str(input):
        if isV3:
            if isinstance(input, bytes):
                return str(input, 'UTF-8')
            return str(input)
        return str(input)

    mutable = ['', '', None]
    # 用临时文件存放结果集效率太低，在tiup exec获取sstfile的时候因为数据量较大避免卡死建议开启，如果在获取tikv region property时候建议采用PIPE方式，效率更高
    if use_temp:
        out_temp = None
        out_fileno = None
        if isV3:
            out_temp = tempfile.SpooledTemporaryFile(buffering=100 * 1024)
        else:
            out_temp = tempfile.SpooledTemporaryFile(bufsize=100 * 1024)
        out_fileno = out_temp.fileno()

        def target():
            mutable[2] = subprocess.Popen(command, stdout=out_fileno, stderr=out_fileno, shell=True)
            mutable[2].wait()

        th = threading.Thread(target=target)
        th.start()
        th.join(timeout)
        # 超时处理
        if th.is_alive():
            mutable[2].terminate()
            th.join()
            if mutable[2].returncode == 0:
                mutable[2].returncode = 9
            result = "Timeout Error!"
        else:
            out_temp.seek(0)
            result = out_temp.read()
        out_temp.close()
        return _str(result), mutable[2].returncode
    else:
        def target():
            mutable[2] = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            mutable[0], mutable[1] = mutable[2].communicate()

        th = threading.Thread(target=target)
        th.start()
        th.join(timeout)
        if th.is_alive():
            mutable[2].terminate()
            th.join()
            if mutable[2].returncode == 0:
                mutable[2].returncode = 1
        return _str(mutable[0]) + _str(mutable[1]), mutable[2].returncode


def format_size(size):
    if size < (1 << 10):
        return "%.2fB" % (size)
    elif size < (1 << 20):
        return "%.2fKB" % (size / (1 << 10))
    elif size < (1 << 30):
        return "%.2fMB" % (size / (1 << 20))
    else:
        return "%.2fGB" % (size / (1 << 30))


def check_env():
    result, recode = command_run("command -v tiup")
    if recode != 0:
        raise Exception("cannot find tiup:%s" % (result))
    return True


class Node:
    def __init__(self):
        self.id = ""
        self.role = ""
        self.host = ""
        self.service_port = 0
        self.status_port = 0
        self.data_dir = ""


class Store:
    def __init__(self):
        self.id = 0
        self.address = ""


# 一张表中可能包含多个分区信息，多个分区可能共用一个region
# 一张表中可能包含多个索引信息，数据和索引可能共用一个region
# 在计算时做如下考虑：
# 当一张表中存在多个分区，则多个分区的region信息去重作为表的region信息，并根据region的sstfile文件和计算其大小。
# 当一张表中存在多个索引时，多个索引的region信息去重作为索引的region信息，并根据region的sstfile文件和计算大小。
# 当sstfile文件为空时（通过tikv-ctl查询region的property信息查不到），根据表或者索引总大小*region总数/region中sstfile不为空的region数来计算最终总大小
# 整个表的大小不一定等于数据大小+索引大小，因为数据和索引存在共用regino的情况

class TableInfo:
    def __init__(self,cf_info = None):
        self.dbname = ""
        self.tabname = ""
        # 同一张表的索引和数据可能放到同一个region上，在求表总大小时候需要去掉
        self.data_region_map = {}  # key:region,value:region
        self.index_name_list = []
        self.partition_name_list = []
        self.index_region_map = {}
        self.all_region_map = {}  # 表和索引的region（包括重合部分),获取property时就用变量
        self.sstfiles_withoutsize_map = {}  # key:(node_id,sstname);value:SSTFile region property中存在，但是在实际物理文件中不存在的sstfile，去重
        self.cf_info = cf_info

    def is_partition(self):
        return len(self.partition_name_list) > 1

    def get_index_cnt(self):
        if len(self.partition_name_list) == 0:
            return 0
        return len(self.index_name_list) / len(self.partition_name_list)

    # predict=True的情况下会将sstfile为空的region进行预估
    # 预估提供两种方案：1、对于没有size的sst按照每一个8MB方式填充；2、计算出总的sst文件的大小算出每一个sst文件的平均值，利用平均值填充没有size的sst
    #prams:cf_info，如果当前sst文件只计算了writecf的，那么需要对defaultcf的sst文件进行预估
    def _get_xx_size(self, region_map, predict=True):
        # predict_method-> 1: sstfile_size=8MB;2: sstfile_size = avg(sstfiles_size)
        predict_method = 2
        # 已有的数据大小
        total_size = 0
        sstfile_dictinct_map = {}  # 避免sstfile被多个region重复计算
        for region in region_map.values():
            if len(region.sstfile_list) == 0:
                continue
            for sstfile in region.sstfile_list:
                sstfile_dictinct_map[(sstfile.sst_node_id, sstfile.sst_name)] = sstfile.sst_size
        total_sstfiles_cnt = len(sstfile_dictinct_map)  # 包含没有大小的sstfile文件
        for size in sstfile_dictinct_map.values():
            total_size += size
        if predict:
            sstfiles_withoutsize_cnt = len(self.sstfiles_withoutsize_map)
            sstfiles_withsize_cnt = total_sstfiles_cnt - sstfiles_withoutsize_cnt
            if sstfiles_withsize_cnt != 0:
                if predict_method == 1:
                    total_size += sstfiles_withoutsize_cnt * (8 << 20)
                # todo 如果算平均时候sstfile全部去重来计算，total_sstfiles_cnt是表中所有的region property出来的sst去重,sstfiles_withoutsize_cnt是去重后的没有大小的sst个数，但是全部按照去重做可能精准度不如全部都不去重的算
                elif predict_method == 2:
                    total_size =  total_size * total_sstfiles_cnt / sstfiles_withsize_cnt
                else:
                    log.error("no this predict_method:%s,return total_size without predict" % (predict_method))
        if self.cf_info is not None:
            if self.cf_info.writecf_sstfiles_total_size != 0 and self.cf_info.defaultcf_sstfiles_total_size != 0:
                #这里按照defaultcf的总大小的比例算，而不是defaultcf的总sstfile个数比例算
                total_size = total_size * (self.cf_info.defaultcf_sstfiles_total_size + self.cf_info.writecf_sstfiles_total_size) / self.cf_info.writecf_sstfiles_total_size
            else:
                log.error("tabname:%s cf_info writecf size =0  or  defaultcf =0" % (self.tabname))
        return total_size

    def get_all_data_size(self):
        return self._get_xx_size(self.data_region_map)

    def get_all_index_size(self):
        return self._get_xx_size(self.index_region_map)

    def get_all_table_size(self):
        return self._get_xx_size(self.all_region_map)


# 一个region可能包含多个sstfile
class Region:
    def __init__(self):
        self.region_id = 0
        self.leader_id = 0
        self.leader_store_id = 0
        self.leader_store_node_id = ""
        # 通过property查询，为空说明未查询到
        self.sstfile_list = []  # SSTFile


class SSTFile:
    def __init__(self):
        self.sst_name = ""
        self.sst_size = 0
        self.sst_node_id = ""
        self.region_id_list = []  # 当前sstfile包含哪些region_id


class TiDBCluster:
    roles = ["alertmanager", "grafana", "pd", "prometheus", "tidb", "tiflash", "tikv"]

    def __init__(self, cluster_name):
        self.cluster_name = cluster_name
        self.cluster_version = ""
        self.tidb_nodes = []
        self._get_clusterinfo()
        self._check_env()
        self._sstfiles_list = []
        self._get_store_sstfiles_bystoreall_once = False  # 是否调用过get_store_sstfiles_bystoreall方法，如果调用过则说明_sstfiles_list包含所有的sstfile文件信息，不需要重复执行
        self._table_region_map = {}  # 所有表的region信息
        self._stores = []  # stores列表
        #通过region properties打印的信息中包含sst_files（不包含writecf.sst_files和defaultcf.sst_files），该值在源码中只包含了writecf的大小，需要估算defaultcf的大小
        #新版本情况：https://github.com/tikv/tikv/blob/790c744e582d4fddfab2b884b40d7d5af14a47e1/src/server/debug.rs#L918
        #老版本情况：https://github.com/tikv/tikv/blob/09a7e1efb40386d804f42ef6ba593f6b85924973/src/server/debug.rs#L918
        self.property_only_writecf_mode = False #目前根据region的property结果来判断，todo 最好按照tidb的版本来判断


    def _get_clusterinfo(self):
        log.debug("TiDBCluster._get_clusterinfo")
        display_command = "tiup cluster display %s" % (self.cluster_name)
        result, recode = command_run(display_command)
        log.debug("tiup display command:%s" % (display_command))
        if recode != 0:
            raise Exception("tiup display error:%s" % result)
        for each_line in result.splitlines():
            log.debug("each_line:" + each_line)
            each_line_fields = each_line.split()
            each_line_fields_len = len(each_line_fields)
            if each_line.startswith("Cluster name:"):
                self.cluster_name = each_line_fields[each_line_fields_len - 1]
            elif each_line.startswith("Cluster version:"):
                self.cluster_version = each_line_fields[each_line_fields_len - 1]
            elif each_line_fields_len == 8 and each_line_fields[1] in TiDBCluster.roles:
                node = Node()
                node.id = each_line_fields[0]
                node.role = each_line_fields[1]
                node.host = each_line_fields[2]
                ports = each_line_fields[3].split("/")
                log.debug(ports)
                if len(ports) == 1:
                    node.service_port = int(ports[0])
                elif len(ports) > 1:
                    node.service_port = int(ports[0])
                    node.status_port = int(ports[1])
                node.data_dir = each_line_fields[6]
                self.tidb_nodes.append(node)

    def _check_env(self):
        cmd = "tiup ctl:%s tikv --version" % (self.cluster_version)
        log.debug("TiDBCluster._check_env,cmd:%s" % (cmd))
        result, recode = command_run(cmd)
        if recode != 0:
            raise Exception("tikv-ctl check error,cmd:%s,message:%s" % (cmd, result))

    # 返回数据库列表
    def get_dblist(self, ignore=['performance_schema', 'metrics_schema', 'information_schema', 'mysql']):
        log.debug("TiDBCluster.get_dblist")
        db_list = []
        req = ""
        for node in self.tidb_nodes:
            if node.role == "tidb":
                req = "http://%s:%s/schema" % (node.host, node.status_port)
                break
        if req == "":
            raise Exception("cannot find db list,%s" % (req))
        log.debug("get_dblist.request:%s" % (req))
        rep = request.urlopen(req)
        if rep.getcode() != 200:
            raise Exception(req)
        rep_data = rep.read()
        if rep_data == "":
            raise Exception("%s,data is None" % (req))
        json_data = json.loads(rep_data)
        for each_db in json_data:
            each_dbname = each_db["db_name"]["L"]
            if each_dbname not in ignore:
                db_list.append(each_dbname)
        log.info("db_list:%s" % (",".join(db_list)))
        return db_list

    # 获取表名列表
    def get_tablelist4db(self, dbname):
        log.debug("TiDBCluster.get_tablelist4db")
        tabname_list = []
        req = ""
        for node in self.tidb_nodes:
            if node.role == "tidb":
                req = "http://%s:%s/schema/%s" % (node.host, node.status_port, dbname)
                break
        if req == "":
            raise Exception("cannot find table list for db,%s" % (req))
        log.debug("get_tablelist4db.request:%s" % (req))
        rep = request.urlopen(req)
        if rep.getcode() != 200:
            raise Exception(req)
        rep_data = rep.read()
        if rep_data == "":
            raise Exception("database:%s no tables" % (dbname))
        json_data = json.loads(rep_data)
        for each_table in json_data:
            tabname_list.append(each_table["name"]["L"])
        log.info("tabname_list:%s" % (",".join(tabname_list)))
        return tabname_list

    # 返回 dbname+"."+tabname为key，TableInfo为value的字典
    # 在多数据库获取时候一定要先获取完成所有数据库的region信息
    def _get_regions4tables(self, dbname, tabname_list):
        self._table_region_map = {}
        log.debug("TiDBCluster.get_regions4tables")
        req = ""
        # table_region_map = {} #key:dbname+"."+tabname,value:TableInfo
        stores = self.get_all_stores()
        log.debug("tabname_list:%s" % (",".join(tabname_list)))
        for tabname in tabname_list:
            table_info = TableInfo(self.get_cf_info())
            table_info.dbname = dbname
            table_info.tabname = tabname
            for node in self.tidb_nodes:
                if node.role == "tidb":
                    req = "http://%s:%s/tables/%s/%s/regions" % (node.host, node.status_port, dbname, tabname)
                    break
            if req == "":
                log.error("cannot find regions,%s" % (req))
                # return table_region_map
                return self._table_region_map
            log.info("get table:%s region info:%s" % (dbname + "." + tabname, req))
            try:
                rep = request.urlopen(req)
            except Exception as e:
                log.error("url error %s,message:%s,tablename: %s may not exists!" % (e, req, dbname + "." + tabname))
                continue
            if rep.getcode() != 200:
                log.error("cannot find regions,%s" % (req))
                continue
            rep_data = rep.read()
            if rep_data == "":
                log.error("table:%s,no regions" % (tabname))
                continue
            json_data = json.loads(rep_data)
            json_data_list = []
            if isinstance(json_data, dict):
                json_data_list.append(json_data)
            elif isinstance(json_data, list):
                json_data_list = json_data
            else:
                log.error("table:%s's json data is not dict or list,source data:%s,json data:%s" % (
                    dbname + "." + tabname, rep_data, json_data))
            try:
                for each_partition in json_data_list:
                    # 获取数据信息
                    table_info.partition_name_list.append(each_partition["name"])
                    for each_region in each_partition["record_regions"]:
                        region = Region()
                        region.region_id = each_region["region_id"]
                        region.leader_id = each_region["leader"]["id"]
                        region.leader_store_id = each_region["leader"]["store_id"]
                        for store in stores:
                            if store.id == region.leader_store_id:
                                region.leader_store_node_id = store.address
                                break
                        table_info.data_region_map[region.region_id] = region
                        table_info.all_region_map[region.region_id] = region
                    # 获取索引信息
                    for each_index in each_partition["indices"]:
                        table_info.index_name_list.append(each_index["name"])
                        for each_region in each_index["regions"]:
                            region = Region()
                            region.region_id = each_region["region_id"]
                            region.leader_id = each_region["leader"]["id"]
                            region.leader_store_id = each_region["leader"]["store_id"]
                            for store in stores:
                                if store.id == region.leader_store_id:
                                    region.leader_store_node_id = store.address
                                    break
                            table_info.index_region_map[region.region_id] = region
                            table_info.all_region_map[region.region_id] = region
            except Exception as e:
                log.error(log.error("table:%s's json data format error,source data:%s,json data:%s,messges:%s" % (
                    dbname + "." + tabname, rep_data, json_data, e)))
            # table_region_map[dbname + "." + tabname] = table_info
            log.info("dbname:%s,tabname:%s data_region_count:%d,index_region_count:%d,table_region_count:%d" % (
                dbname, tabname, len(table_info.data_region_map), len(table_info.index_region_map),
                len(table_info.all_region_map)))
            self._table_region_map[dbname + "." + tabname] = table_info
        return self._table_region_map

    def get_phy_tables_size(self, dbname, tabname_list, parallel=1):
        log.debug("TiDBCluster.get_phy_tables_size")
        table_map = {}  # 打印每一张表的大小
        sstfile_map = {}  # key:sstfile绝对路径,value:sstfile大小
        table_region_map = self._get_regions4tables(dbname, tabname_list)  # 获取列表的region相关信息
        log.info("<----start get tables size---->")
        log.info("get sstfiles...")

        # 获取region信息,并将结果写入region_queue
        def put_regions_to_queue(table_region_map, dbname, tabname_list, region_queue, parallel):
            tabname_list_region_count = 0
            for tabname in tabname_list:
                full_tabname = dbname + "." + tabname
                if full_tabname not in table_region_map:
                    log.error("table:%s not maybe not exists!" % (full_tabname))
                    continue
                for region_id in table_region_map[full_tabname].all_region_map.keys():
                    region_queue.put((dbname, tabname, region_id))
                    log.debug("put region into region_queue:%s" % (region_id))
                    tabname_list_region_count += 1
            for i in range(parallel):
                # signal close region_queue
                log.debug("put region into region_queue:None")
                region_queue.put(None)

        region_thread = threading.Thread(target=put_regions_to_queue,
                                         args=(table_region_map, dbname, tabname_list, region_queue, parallel))
        log.info("put_regions_to_queue")
        region_thread.start()
        threads = []
        log.info("region_queue->get_leader_region_sstfiles_muti")
        for i in range(parallel):
            t = threading.Thread(target=self.get_leader_region_sstfiles_muti,
                                 args=(table_region_map, region_queue, i))
            t.start()
            threads.append(t)
        for i in threads: i.join()
        log.info("region_queue->get_leader_region_sstfiles_muti done")
        region_thread.join()
        log.info("put_regions_to_queue done")
        # 获取sstfile的物理大小信息
        # 如果当前table_region_map中包含的sst文件数量比较小，则直接下发sst文件名去tikv上查找sst文件的物理大小，如果比较多则直接去tikv获取全部的sst文件信息
        fetchall_flag = True

        # region_max:当所有表的region数大于此值后直接根据 region获取sstfile大小
        # sstfile_max: 当所有表的涉及到的sstfile数大于此值后直接根据region获取sstfile大小
        def useFetchall(table_region_map, region_max, sstfile_max):
            temp_region_cnt = 0
            temp_sstfiles_cnt = 0
            try:
                for k in table_region_map:
                    for region_id in table_region_map[k].all_region_map:
                        if temp_region_cnt > region_max:
                            return True
                        temp_region_cnt += 1
                        for each_sstfile in table_region_map[k].all_region_map[region_id].sstfile_list:
                            if temp_sstfiles_cnt > sstfile_max:
                                return True
                            temp_sstfiles_cnt += 1
            except Exception as e:
                log.error("useFetchall method error:%s" % (e))
            return False

        fetchall_flag = useFetchall(table_region_map, 200, 5000)
        if fetchall_flag:
            sstfile_list = self.get_store_sstfiles_bystoreall()
        # 如果不一次性全部获取则需要去各个节点获取sstfile的大小信息
        else:
            sstfile_list = self.get_store_sstfiles_bysstfilelist(
                [each_sstfile for k in table_region_map for region_id in table_region_map[k].all_region_map for
                 each_sstfile in table_region_map[k].all_region_map[region_id].sstfile_list])

        for sstfile in sstfile_list:
            sstfile_map[(sstfile.sst_node_id, sstfile.sst_name)] = sstfile.sst_size
        log.info(
            "total sstfiles count:%d,size in memory:%s" % (len(sstfile_map), format_size(sys.getsizeof(sstfile_map))))
        log.info("get sstfiles,done.")
        # 在sstfile_map中查查找table_region_map中的sstfile文件并填充数据
        # k为full表名
        for k in table_region_map:
            for region_id in table_region_map[k].all_region_map:
                i = 0
                for each_sstfile in table_region_map[k].all_region_map[region_id].sstfile_list:
                    key = (each_sstfile.sst_node_id, each_sstfile.sst_name)
                    region = table_region_map[k].all_region_map[region_id]
                    if key not in sstfile_map:
                        # table_region_map[k].sstfiles_withoutsize_cnt += 1
                        table_region_map[k].sstfiles_withoutsize_map[
                            (each_sstfile.sst_node_id, each_sstfile.sst_name)] = each_sstfile
                        log.debug("table:%s,region:%d,node_id:%s,sstfilename:%s cannot find in sstfile_map" % (
                            k, region_id, region.leader_store_node_id, each_sstfile.sst_name))
                    else:
                        table_region_map[k].all_region_map[region_id].sstfile_list[i].sst_size = sstfile_map[key]
                    i += 1
        # table_region_map中已经有完整的sstfile相关数据
        for tabinfo in table_region_map.values():
            dbname = tabinfo.dbname
            tabname = tabinfo.tabname
            full_tabname = dbname + "." + tabname
            log.info("tabname:%s sstfiles_withoutsize_count:%d" % (full_tabname, len(tabinfo.sstfiles_withoutsize_map)))
            table_map[full_tabname] = {
                "dbname": tabinfo.dbname,
                "tabname": tabinfo.tabname,
                "is_partition": "False" if tabinfo.is_partition() == False else "True-" + str(
                    len(tabinfo.partition_name_list)),
                "index_count": tabinfo.get_index_cnt(),
                "data_size": tabinfo.get_all_data_size(),
                "index_size": tabinfo.get_all_index_size(),
                "table_size": tabinfo.get_all_table_size(),
            }
        log.info("<----end get tables size---->")
        return table_map

    def get_all_stores(self):
        if len(self._stores) != 0:
            return self._stores
        req = ""
        stores = []
        for node in self.tidb_nodes:
            if node.role == "pd":
                req = "http://%s:%s/pd/api/v1/stores" % (node.host, node.service_port)
                break
        if req == "":
            log.error("cannot find stores,%s" % (req))
            return stores
        rep = request.urlopen(req)
        if rep.getcode() != 200:
            raise Exception(req)
        json_data = json.loads(rep.read())
        for each_store in json_data["stores"]:
            store = Store()
            store.id = each_store["store"]["id"]
            store.address = each_store["store"]["address"]
            stores.append(store)
        self._stores = stores
        return self._stores

    # 根据sstfile文件名去tikv上获取文件大小
    # 入参：[SSTFile]
    def get_store_sstfiles_bysstfilelist(self, sstfiles):
        log.info("tikv-property method:get_store_sstfiles_bysstfilelist,sstfiles count:%d" % (len(sstfiles)))
        if len(self._sstfiles_list) != 0 and self._get_store_sstfiles_bystoreall_once is True:
            return self._sstfiles_list
        sstfiles_node_map = {}  # key:node_id,value:sstfile_list
        result_sstfiles = []
        for each_sstfile in sstfiles:
            if each_sstfile.sst_node_id in sstfiles_node_map:
                sstfiles_node_map[each_sstfile.sst_node_id].append(each_sstfile)
            else:
                sstfiles_node_map[each_sstfile.sst_node_id] = [each_sstfile]
        for each_node_id, sstfiles in sstfiles_node_map.items():
            data_dir = ""
            host = ""
            for node in self.tidb_nodes:
                if each_node_id == node.id:
                    data_dir = node.data_dir
                    host = node.host
            if data_dir == "":
                log.error("cannot find node_id:%s sstfile's data dir" % (each_node_id))
                continue
            sstfile_path = os.path.join(data_dir, "db")
            cmd = '''tiup cluster exec %s --command='cd %s;for ssf in %s ;do stat -c "%s" $ssf ;done' -N %s ''' % (
                self.cluster_name, sstfile_path, " ".join([sstf.sst_name for sstf in sstfiles]), "%n:%s", host)
            result, recode = command_run(cmd, use_temp=True, timeout=600)
            log.debug(cmd)
            if recode != 0:
                raise Exception("get sst file info error,cmd:%s,message:%s" % (cmd, result))
            inline = False
            for each_line in result.splitlines():
                if each_line.startswith("stdout:"):
                    inline = True
                    continue
                if inline:
                    each_line_fields = each_line.split(":")
                    each_line_fields_len = len(each_line_fields)
                    if each_line_fields_len != 2 or each_line.find(".sst:") == -1: continue
                    sstfile = SSTFile()
                    sstfile.sst_name = each_line_fields[0]
                    sstfile.sst_size = int(each_line_fields[1])
                    sstfile.sst_node_id = each_node_id
                    result_sstfiles.append(sstfile)
        return result_sstfiles

    # 获取所有tikv的sstfiles列表
    def get_store_sstfiles_bystoreall(self):
        log.info("tikv-property method:get_store_sstfiles_bystoreall")
        if len(self._sstfiles_list) != 0 and self._get_store_sstfiles_bystoreall_once is True:
            return self._sstfiles_list
        result_sstfiles = []
        for node in self.tidb_nodes:
            if node.role != "tikv": continue
            cmd = '''tiup cluster exec %s --command='find %s/db/*.sst |xargs stat -c "%s"|grep -Po "\d+\.sst:\d+"' -N %s''' % (
                self.cluster_name, node.data_dir, "%n:%s", node.host)
            result, recode = command_run(cmd, use_temp=True, timeout=600)
            log.debug(cmd)
            if recode != 0:
                raise Exception("get sst file info error,cmd:%s,message:%s" % (cmd, result))
            inline = False
            for each_line in result.splitlines():
                if each_line.startswith("stdout:"):
                    inline = True
                    continue
                if inline:
                    each_line_fields = each_line.split(":")
                    each_line_fields_len = len(each_line_fields)
                    if each_line_fields_len != 2 or each_line.find(".sst:") == -1: continue
                    sstfile = SSTFile()
                    sstfile.sst_name = each_line_fields[0]
                    sstfile.sst_size = int(each_line_fields[1])
                    sstfile.sst_node_id = node.id
                    result_sstfiles.append(sstfile)
        self._sstfiles_list = result_sstfiles
        self._get_store_sstfiles_bystoreall_once = True
        return result_sstfiles

    # 获取提供的region信息，多线程获取property信息
    # 入参：
    # table_region_map为以dbname+"."+tabname为key，TableInfo为value的字典
    # region_queue中获取region信息（dbname,tablename,region_id)，修改table_region_map，补充sstfile相关信息
    def get_leader_region_sstfiles_muti(self, table_region_map, region_queue, thread_id=0):
        log.debug("thread_id:%d,get_leader_region_sstfiles_muti start" % (thread_id))
        while True:
            data = region_queue.get()
            if data is None:
                log.debug("thread_id:%d,get_leader_region_sstfiles_muti done" % (thread_id))
                return
            # (tabname, leader_node_id, region_id) = data
            (dbname, tabname, region_id) = data
            full_tabname = dbname + "." + tabname
            table_info = table_region_map[full_tabname]
            region = table_info.all_region_map[region_id]
            leader_node_id = region.leader_store_node_id
            sstfiles = []
            cmd = "tiup ctl:%s tikv --host %s region-properties -r %d" % (
                self.cluster_version, leader_node_id, region_id)
            result, recode = command_run(cmd)
            # cannot find region when region split or region merge
            if recode != 0:
                log.warn("cmd:%s,message:%s" % (cmd, result))
            else:
                for each_line in result.splitlines():
                    if each_line.find("sst_files:") > -1:
                        #如果tikv-ctl region properties的结果中包含sst_files开头的说明打印的结果只包含了writecf的sst文件
                        if each_line.find("sst_files:") == 0 and not self.property_only_writecf_mode:
                            self.property_only_writecf_mode = True
                        each_line_fields = each_line.split(":")
                        each_line_fields_len = len(each_line_fields)
                        if each_line_fields_len == 2 and each_line_fields[1] != "":
                            for sstfilename in [x.strip() for x in each_line_fields[1].split(",")]:
                                if sstfilename == "":
                                    continue
                                sstfile = SSTFile()
                                sstfile.sst_name = sstfilename
                                sstfile.region_id_list.append(region_id)
                                sstfile.sst_node_id = leader_node_id
                                sstfiles.append(sstfile)
            if len(sstfiles) == 0:
                log.debug(
                    "region-properties:tabname:%s,region:%d's sstfile cannot found,cmd:%s" % (tabname, region_id, cmd))
            table_region_map[full_tabname].all_region_map[region_id].sstfile_list = sstfiles
            region_queue.task_done()

    def get_cf_info(self):
        if self.property_only_writecf_mode is not True:
            return None
        node_id = ""
        for nd in self.tidb_nodes:
            if nd.role == "prometheus":
                node_id = nd.id
                break
        return CFInfo(node_id)



#从writecf、defaucf的sstfile个数和大小信息
#单例模式
class CFInfo(object):
    _instance = {}
    def __new__(cls,prometheus_node_id):
        if prometheus_node_id not in cls._instance:
            cls._instance[prometheus_node_id] = super().__new__(cls)
        return cls._instance[prometheus_node_id]
    def __init__(self,prometheus_node_id):
        self.prometheus_node_id = prometheus_node_id
        self.defaultcf_sstfiles_count = 0
        self.defaultcf_sstfiles_total_size = 0
        self.writecf_sstfiles_count = 0
        self.writecf_sstfiles_total_size = 0
        self._get_sstfiles_info()
    def _get_sstfiles_info(self):
        tikv_engine_num_files_at_level_url = 'http://%s/api/v1/query?query=sum%28tikv_engine_num_files_at_level%29by%28cf%29' % (self.prometheus_node_id)
        num_files_data,err = get_jsondata_from_url(tikv_engine_num_files_at_level_url)
        if err is not None:
            log.error(err)
        else:
            try:
                for each_item in num_files_data["data"]["result"]:
                    cf_type = each_item["metric"]["type"]
                    cf_nums_str = each_item["value"][1]
                    if cf_type == "write":
                        self.writecf_sstfiles_count = int(cf_nums_str)
                    elif cf_type == "default":
                        self.defaultcf_sstfiles_count = int(cf_nums_str)
            except Exception as e:
                log.error(e)
        tikv_engine_size_bytes_url = 'http://%s/api/v1/query?query=sum%28tikv_engine_size_bytes%29by%28cf%29' % (self.prometheus_node_id)
        tikv_engine_size_data,err = get_jsondata_from_url(tikv_engine_size_bytes_url)
        if err is not None:
            log.error(err)
        else:
            try:
                for each_item in tikv_engine_size_data["data"]["result"]:
                    cf_type = each_item["metric"]["type"]
                    cf_size_str = each_item["value"][1]
                    if cf_type == "write":
                        self.writecf_sstfiles_total_size = int(cf_size_str)
                    elif cf_type == "default":
                        self.defaultcf_sstfiles_total_size = int(cf_size_str)
            except Exception as e:
                log.error(e)

#return:json data,error
def get_jsondata_from_url(url):
    if url == "":
        return "","url is none"
    try:
        rep = request.urlopen(url)
    except Exception as e:
        return "",str(e)
    rep_data = rep.read()
    if rep_data == "":
        return "","response from %s is none" % (url)
    return json.loads(rep_data)

class OutPutShow():
    def __init__(self):
        self.title_list = []  # 标题
        self.data_list = []  # 数据列表，二维列表
        self.max_width_map = {}  # 记录每一列的每一个值的长度最大值，用于展现
        self._output_format = ""

    def _check(self):
        for i in range(len(self.title_list)):
            self.max_width_map[i] = len(str(self.title_list[i]))
        if len(self.data_list) != 0:
            for each_row in self.data_list:
                for i in range(len(each_row)):
                    col_len = len(str(each_row[i]))
                    if i in self.max_width_map:
                        if self.max_width_map[i] < col_len:
                            self.max_width_map[i] = col_len
                    else:
                        self.max_width_map[i] = col_len
                if not isinstance(each_row, list):
                    log.error("输出结果非二维列表")
                    return False
                else:
                    if len(each_row) != len(self.title_list):
                        log.error("当前行数据列数和标题列数不一致,data:%s" % (each_row))
                        return False

        return True

    def _format(self):
        if self._output_format != "":
            return self._output_format
        format_list = []
        for i in range(len(self.max_width_map)):
            format_list.append("%-" + str(self.max_width_map[i] + 2) + "s")
        return "".join(format_list)

    def show(self, with_title=True):
        if not self._check():
            log.error("output show check error")
            return
        log.info("output format:%s" % (self._format()))
        # 打印标题
        if with_title:
            print(self._format() % tuple(self.title_list))
        # 打印数据
        for each_line_list in self.data_list:
            if isinstance(each_line_list, list):
                print(self._format() % tuple(each_line_list))


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='get table size')
    arg_parser.add_argument('-c', '--cluster', type=str, required=True, help='tidb cluster name')
    arg_parser.add_argument('-d', '--dbname', type=str, required=True, help='database name,* mains all databases')
    arg_parser.add_argument('-t', '--tabnamelist', type=str, required=True,
                            help='table name,* mains all tables for database,muti table should like this "t1,t2,t3"')
    arg_parser.add_argument('-p', '--parallel', default=1, type=int, help='parallel')
    arg_parser.add_argument('--loglevel', default="info", type=str, help='critical,error,warn,info,debug')
    args = arg_parser.parse_args()
    cname, dbname, tabnamelist, parallel, loglevel, level = args.cluster, args.dbname, args.tabnamelist, args.parallel, args.loglevel, log.INFO
    if loglevel == "info":
        level = log.INFO
    elif loglevel == "warn":
        level = log.WARN
    elif loglevel == "debug":
        level = log.DEBUG
    elif loglevel == "error":
        level = log.ERROR
    elif loglevel == "critical":
        level = log.CRITICAL
    log_filename = sys.argv[0] + ".log"
    log.basicConfig(filename=log_filename, filemode='a', level=level,
                    format='%(asctime)s - %(name)s-%(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s')
    db_list = []
    cluster = TiDBCluster(cname)
    if dbname == "*":
        db_list = cluster.get_dblist()
    else:
        db_list = [dbname]
    print_output = OutPutShow()
    print_output.title_list = ["DataBase", "TabName", "Partition", "IndexCnt", "DataSize", "DataSizeF", "Indexsize",
                               "IndexsizeF", "Tablesize", "TablesizeF"]
    for each_db in db_list:
        tabname_list = [x.strip() for x in tabnamelist.split(",")]
        if len(tabname_list) == 1 and tabname_list[0] == "*":
            tabname_list = cluster.get_tablelist4db(each_db)
        tables_map = cluster.get_phy_tables_size(each_db, tabname_list, parallel)
        for full_tabname, val in sorted(tables_map.items(), reverse=True, key=lambda x: x[1]["table_size"]):
            print_output.data_list.append(
                [val["dbname"], val["tabname"], val["is_partition"], val["index_count"], val["data_size"],
                 format_size(val["data_size"]),
                 val["index_size"], format_size(val["index_size"]), val["table_size"], format_size(val["table_size"])])

    print_output.show()
