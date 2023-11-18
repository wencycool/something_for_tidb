#!/usr/bin/python
# encoding=utf8
import os.path
import subprocess
import sys
import tempfile
import threading
import socket
import logging
#在tiup中控机中执行该脚本，向集群中各个节点的/db/dbawork/info中写入 cluster.info说明文件，主要记录tiup cluster display信息
#方便每一个节点都能清楚的知道其tiup中控机是谁，节点的其余节点是谁。
#注意：集群中的任意一个节点所在OS只能属于该集群，否则可能会导致/db/dbawork/info中的cluster.info信息被覆盖。
#     直接取名叫cluster.info而并不是<cluster>.info以集群名称命名的主要原因是避免修改集群名称后导致在目录中看到多个集群info文件。
#放入crontab中时候必须引入用户环境变量
#每天晚上03点执行一次
#00 03 * * *  . /home/tidb/.bash_profile && /usr/bin/python xxx.py
# 判断python的版本
isV3 = float(sys.version[:3]) > 2.7
#return: result,recode
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

def check_env():
    result, recode = command_run("command -v tiup")
    if recode != 0:
        raise Exception("cannot find tiup:%s" % (result))
    return True

#获取tidb集群列表
#return: cluster_list,err
def get_tidb_cluster_list():
    cluster_list = []
    result,recode = command_run("tiup cluster list 2>/dev/null")
    if recode != 0:
        return [],recode
    else:
        pflag = False
        for each_line in result.split("\n"):
            if each_line.startswith("----"):
                pflag = True
                continue
            if pflag:
                cluster_list.append(each_line.split([0]))
        return cluster_list,None

#获取ip和主机名对应关系
#return:map[ip]hostname,err
def get_ip_host_map(cluster_name):
    ip_host_map = {}
    cmd = "tiup cluster exec %s --command \"hostname\" 2>/dev/null" % (cluster_name)
    result,recode = command_run(cmd)
    if recode != 0:
        return result,recode
    i = 0
    value_line = -99999999
    for each_line in result.split("\n"):
        i = i + 1
        if each_line.startswith("Outputs of"):
            fields = each_line.split()
            ip = fields[len(fields) - 1].split(":")[0]
        if each_line.startswith("stdout:"):
            value_line = i
        if value_line + 1 == i:
            ip_host_map[ip] = each_line
    return ip_host_map,None

#获取集群display信息
def get_cluster_display(cluster_name):
    cmd = "tiup cluster display %s 2>/dev/null" % (cluster_name)
    return command_run(cmd)

#带主机名
def get_cluster_display_detail(cluster_name):
    output = ""
    result,recode = get_cluster_display(cluster_name)
    if recode != 0:
        return result,recode
    ip_host_map,recode = get_ip_host_map(cluster_name)
    if recode is not None:
        return ip_host_map,recode
    pflag = False
    for each_line in result.split("\n"):
        if each_line.startswith("ID                    Role"):
            each_line = "%-30s" % ("HOSTNAME") + each_line
        if each_line.startswith("--"):
            each_line = "%-30s" % ("--------") + each_line
            output = output + each_line + "\n"
            pflag = True
            continue
        elif each_line.startswith("Total nodes"):
            pflag = False
        if pflag:
            ip = each_line.split()[0].split(":")[0]
            if ip in ip_host_map:
                each_line = "%-30s" % (ip_host_map[ip]) + each_line
            else:
                each_line = "%-30s" % ("None") + each_line
        if each_line == "":
            continue
        output = output + each_line + "\n"
    return output,None

#获取主机名
def get_hostname():
    return socket.gethostname()

#return map[ip]result,err
def command_run_cluster_exec(cluster_name,command):
    ip_result_map = {}
    cmd = "tiup cluster exec %s --command \"%s \" 2>/dev/null" % (cluster_name,command)
    result,recode = command_run(cmd)
    if recode != 0:
        return result,recode
    i = 0
    value_start = -99999999
    value = ""
    pflag = False
    output_first = True
    for each_line in result.split("\n"):
        i = i + 1
        if value_start == i:
            pflag = True
        if each_line.startswith("Outputs of"):
            if not output_first:
                pflag = False
                ip_result_map[ip] = value
            fields = each_line.split()
            ip = fields[len(fields) - 1].split(":")[0]
            output_first = False
        if each_line.startswith("stdout:"):
            value_start = i + 1
        if pflag:
            value = value + each_line + "\n"
    ip_result_map[ip] = value
    return ip_result_map,None



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        filename="/tmp/cluster_info.log",
                        filemode="a",
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S')
    #proj_name = os.path.realpath(sys.argv[0])
    cluster_list,err = get_tidb_cluster_list()
    if err is not None:
        logging.error(err)
        exit(1)
    for cluster_name in cluster_list:
        logging.debug("cluster_name:%s" % (cluster_name))
        file_name = "cluster.info"
        local_dir = "/tmp"
        local_file = os.path.join(local_dir,file_name)
        target_dir = "/db/dbawork/info"
        logging.debug("local_dir:%s" % (local_dir))
        logging.debug("local_file:%s" % (local_file))
        logging.debug("target_dir:%s" % (target_dir))
        result,err = get_cluster_display_detail(cluster_name)
        if err is not None:
            logging.info(str(err))
        for each_line in result.split("\n"):
            logging.debug(each_line)
        #判断目标文件夹是否存在，目标文件夹的挂载点是否/db/dbawork，属主是否tidb，如果均是才能进行分发
        result,recode = command_run_cluster_exec(cluster_name,"ls -ld %s ||mkdir %s" % (target_dir,target_dir))
        if recode is not None:
            result,recode = command_run_cluster_exec(cluster_name,"ls -ld %s" % (target_dir))
            if recode is not None:
                logging.error(result)
                exit(1)
        logging.debug("get cluster display information with hostname")
        target_file = os.path.join(target_dir,file_name)
        result,recode = get_cluster_display_detail(cluster_name)
        if recode is not None:
            logging.error(result)
            exit(1)
        logging.debug("save data to %s" % (local_file))
        with open(local_file,mode='w') as f:
            f.write("tiup hostname:      " + get_hostname() + "\n")
            f.write(result)
        logging.debug("push file to remote:%s" % (target_file))
        result,recode = command_run("tiup cluster push %s %s %s" % (cluster_name,local_file,target_file))
        if recode != 0:
            logging.error(result)
            exit(1)
        logging.debug("success!")
