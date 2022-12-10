#!/usr/bin/python
# encoding=utf8
#必须开启如下配置文件参数否则计算可能不准（默认情况下即开启）
#rocksdb.defaultcf.enable-compaction-guard=true
#rocksdb.writecf.enable-compaction-guard=true
#tidb.split-table=true
import sys, subprocess, logging as log
if float(sys.version[:3]) <= 2.7:
    import urllib as request
else:
    import urllib.request as request
import json
import argparse
import threading
from queue import Queue
# python3
property_queue = Queue(20)
region_queue = Queue(20)
def command_run(command, timeout=30):
    proc = subprocess.Popen(command, bufsize=409600, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    #poll_seconds = .250
    #deadline = time.time() + timeout
    #while time.time() < deadline and proc.poll() is None:
    #    time.sleep(poll_seconds)
    #if proc.poll() is None:
    #    if float(sys.version[:3]) >= 2.6:
    #        proc.terminate()
    stdout, stderr = proc.communicate()
    if float(sys.version[:3]) <= 2.7:
        return str(stdout) + str(stderr), proc.returncode
    return str(stdout, 'UTF-8') + str(stderr, 'UTF-8'), proc.returncode


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


class Region:
    def __init__(self):
        self.region_id = 0
        self.leader_id = 0
        self.leader_store_id = 0
        self.leader_store_node_id = ""


class SSTFile:
    def __init__(self):
        self.sst_name = ""
        self.sst_size = ""
        self.sst_node_id = ""


class TiDBCluster:
    roles = ["alertmanager", "grafana", "pd", "prometheus", "tidb", "tiflash", "tikv"]

    def __init__(self, cluster_name):
        self.cluster_name = cluster_name
        self.cluster_version = ""
        self.tidb_nodes = []
        self._get_clusterinfo()
        self._check_env()

    def _get_clusterinfo(self):
        display_command = "tiup cluster display %s" % (self.cluster_name)
        result, recode = command_run(display_command)
        log.debug("tiup display command:%s" % (display_command))
        log.debug("result:" + result)
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
        result, recode = command_run(cmd)
        if recode != 0:
            raise Exception("tikv-ctl check error,cmd:%s,message:%s" % (cmd, result))

    def get_regions4table(self, dbname, tabname):
        req = ""
        regions = []
        stores = self.get_all_stores()
        for node in self.tidb_nodes:
            if node.role == "tidb":
                req = "http://%s:%s/tables/%s/%s/regions" % (node.host, node.status_port, dbname, tabname)
                break
        if req == "":
            log.error("cannot find regions,%s" % (req))
            return regions
        rep = request.urlopen(req)
        if rep.getcode() != 200:
            raise Exception(req)
        json_data = json.loads(rep.read())
        for each_region in json_data["record_regions"]:
            region = Region()
            region.region_id = each_region["region_id"]
            region.leader_id = each_region["leader"]["id"]
            region.leader_store_id = each_region["leader"]["store_id"]
            for store in stores:
                if store.id == region.leader_store_id:
                    region.leader_store_node_id = store.address
            regions.append(region)
        return regions

    #return total_size,sst_file_list which include sst_node_id,sst_name
    def get_phy_table_size(self, dbname, tabname,parallel=1):
        total_size = 0
        sst_file_list = []
        sstfile_map = {}
        sstfile_map_filter = {} #当表中已经存在该sst后则不重复计算
        for sstfile in self.get_store_sstfiles_bystoreall():
            sstfile_map[sstfile.sst_node_id, sstfile.sst_name] = sstfile.sst_size
        if parallel == 1:
            for region in self.get_regions4table(dbname, tabname):
                for each_sstfile in self.get_leader_region_sstfiles(region.leader_store_node_id, region.region_id):
                    key = (region.leader_store_node_id, each_sstfile)
                    if key in sstfile_map:
                        if key in sstfile_map_filter:
                            continue
                        else:
                            sstfile_map_filter[key] = None
                        total_size += int(sstfile_map[key])
                        sst_file_list.append(key)
        elif parallel > 1:
            #Parameter Reference
            total_size_list = [0]
            def get_regions(dbname,tabname):
                for region in self.get_regions4table(dbname, tabname):
                    log.debug("put region into region_queue:%s" % (region.region_id))
                    region_queue.put((region.leader_store_node_id,region.region_id))
                for i in range(parallel):
                    #signal close region_queue
                    log.debug("put region into region_queue:None")
                    region_queue.put(None)
            region_thread = threading.Thread(target=get_regions,args=(dbname,tabname))
            region_thread.start()
            threads = []
            for i in range(parallel):
                t = threading.Thread(target=self.get_leader_region_sstfiles_muti,args=(region_queue,))
                t.start()
                threads.append(t)
            def get_size_from_property_queue(property_queue,total_size_list):
                log.debug("get_size_from_property_queue")
                none_cnt = 0
                while True:
                    data = property_queue.get()
                    if data is None:
                        none_cnt = none_cnt + 1
                        if none_cnt >= parallel:
                            return
                        else:
                            continue
                    (leader_store_node_id,sstfiles) = data
                    for each_sstfile in sstfiles:
                        key = (leader_store_node_id, each_sstfile)
                        if key in sstfile_map:
                            if key in sstfile_map_filter:
                                continue
                            else:
                                sstfile_map_filter[key] = None
                            total_size_list[0] += int(sstfile_map[key])
                            sst_file_list.append(key)
                    property_queue.task_done()
            t1 = threading.Thread(target=get_size_from_property_queue,args=(property_queue,total_size_list))
            t1.start()
            for t in threads:t.join()
            region_thread.join()
            t1.join()
            total_size = total_size_list[0]
        return total_size,sst_file_list

    def get_all_stores(self):
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
        return stores

    def get_store_sstfiles_bystoreall(self):
        sstfiles = []
        for node in self.tidb_nodes:
            if node.role != "tikv": continue
            cmd = '''tiup cluster exec %s --command='find %s/db/*.sst |xargs stat -c "%s"|grep -Po "\d+\.sst:\d+"' -N %s''' % (
                self.cluster_name, node.data_dir, "%n:%s", node.host)
            result, recode = command_run(cmd, timeout=600)
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
                    sstfile.sst_size = each_line_fields[1]
                    sstfile.sst_node_id = node.id
                    sstfiles.append(sstfile)
        return sstfiles

    # leader region
    def get_leader_region_sstfiles(self, leader_node_id, region_id):
        sstfiles = []
        cmd = "tiup ctl:%s tikv --host %s region-properties -r %d" % (self.cluster_version, leader_node_id, region_id)
        result, recode = command_run(cmd)
        # cannot find region when region split or region merge
        if recode != 0:
            log.warn("cmd:%s,message:%s" % (cmd, result))
        else:
            for each_line in result.splitlines():
                if each_line.find(".sst_files:") > 0:
                    each_line_fields = each_line.split(":")
                    each_line_fields_len = len(each_line_fields)
                    if each_line_fields_len == 2 and each_line_fields[1] != "":
                        sstfiles.extend([x.strip() for x in each_line_fields[1].split(",")])
        return sstfiles

    def get_leader_region_sstfiles_muti(self, region_queue):
        log.debug("get_leader_region_sstfiles_muti")
        while True:
            data = region_queue.get()
            if data is None:
                log.debug("get_leader_region_sstfiles_muti:region_queue is None")
                property_queue.put(None)
                return
            (leader_node_id,region_id) = data
            sstfiles = []
            cmd = "tiup ctl:%s tikv --host %s region-properties -r %d" % (self.cluster_version, leader_node_id, region_id)
            result, recode = command_run(cmd)
            # cannot find region when region split or region merge
            if recode != 0:
                log.warn("cmd:%s,message:%s" % (cmd, result))
            else:
                for each_line in result.splitlines():
                    if each_line.find(".sst_files:") > 0:
                        each_line_fields = each_line.split(":")
                        each_line_fields_len = len(each_line_fields)
                        if each_line_fields_len == 2 and each_line_fields[1] != "":
                            sstfiles.extend([x.strip() for x in each_line_fields[1].split(",")])
            property_queue.put((leader_node_id,sstfiles))
            region_queue.task_done()

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='get table size')
    arg_parser.add_argument('-c','--cluster',type=str,help='tidb cluster name')
    arg_parser.add_argument('-d','--dbname',type=str,help='database name')
    arg_parser.add_argument('-t','--tabname',type=str,help='table name')
    arg_parser.add_argument('-p','--parallel',default=1,type=int,help='parallel')
    arg_parser.add_argument('--loglevel',default="info",type=str,help='info,warn,debug')
    arg_parser.add_argument('--extend',type=bool,default=False,help="show table's sstfiles")
    args = arg_parser.parse_args()
    cname = args.cluster
    dbname = args.dbname
    tabname = args.tabname
    parallel = args.parallel
    loglevel = args.loglevel
    level = log.INFO
    extend = args.extend
    cluster = TiDBCluster(cname)
    if loglevel == "info":
        level = log.INFO
    elif loglevel == "warn":
        level = log.WARN
    elif loglevel == "debug":
        level = log.DEBUG
    log.basicConfig(level = level,format = '%(asctime)s - %(name)s-%(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s')
    tabsize,sstfile_list = cluster.get_phy_table_size(dbname,tabname,parallel)
    output = ("Cluster:%s,table_schema:%s,table_name:%s,size:%s" % (cname,dbname,tabname,tabsize))
    if extend:
        output = output + ",sstfiles:%s" % (",".join([x+":"+y for x,y in sstfile_list]))
    print(output)
