#!/usr/bin/python
# encoding=utf8
import os
import sys, subprocess, time, logging as log
if float(sys.version[:3]) <= 2.7:
    import urllib as request
else:
    import urllib.request as request
import json
import argparse

# python3

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
    # todo 补充tiup ctl:<version> tikv的检测，但version需要再TiDBCluster实例中获取
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

    def get_phy_table_size(self, dbname, tabname):
        total_size = 0
        sstfile_map = {}
        for sstfile in self.get_store_sstfiles_bystoreall():
            sstfile_map[sstfile.sst_node_id, sstfile.sst_name] = sstfile.sst_size
        for region in self.get_regions4table(dbname, tabname):
            for each_sstfile in self.get_leader_region_sstfiles(region.leader_store_node_id, region.region_id):
                key = (region.leader_store_node_id, each_sstfile)
                if key in sstfile_map:
                    total_size += int(sstfile_map[key])
        return total_size

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

if __name__ == "__main__":
    log.basicConfig(level=log.INFO)
    arg_parser = argparse.ArgumentParser(description='get table size')
    arg_parser.add_argument('-c','--cluster',type=str,help='tidb cluster name')
    arg_parser.add_argument('-d','--dbname',type=str,help='database name')
    arg_parser.add_argument('-t','--tabname',type=str,help='table name')

    args = arg_parser.parse_args()
    cname = args.cluster
    dbname = args.dbname
    tabname = args.tabname
    cluster = TiDBCluster(cname)
    tabsize = cluster.get_phy_table_size(dbname,tabname)
    print("Cluster:%s,table_schema:%s,table_name:%s,size:%s" % (cname,dbname,tabname,tabsize))