#!/usr/bin/env python3
import tempfile, threading, subprocess
import gzip, os, base64, re
from email.policy import default
from typing import List
import logging
import argparse

def command_run(command: str, use_temp: bool=False, timeout: int=30, stderr_to_stdout: bool=True) -> (str, int):
    """
    Run a command and return the output and return code.
    Args:
        command: The command to run.
        use_temp: Whether to use a temporary file to store the output.
        timeout: The timeout of the command.
        stderr_to_stdout: Whether to redirect stderr to stdout.
    Returns:
        The output and return code of the command.
    """

    def _str(input):
        if isinstance(input, bytes):
            return str(input, 'UTF-8')
        return str(input)

    mutable = ['', '', None]
    # 用临时文件存放结果集效率太低，在tiup exec获取sstfile的时候因为数据量较大避免卡死建议开启，如果在获取tikv region property时候建议采用PIPE方式，效率更高
    if use_temp:
        out_temp = None
        out_fileno = None
        out_temp = tempfile.SpooledTemporaryFile(buffering=100 * 1024)
        out_fileno = out_temp.fileno()

        def target():
            # 标准输出结果集比较大输出到文件，错误输出到PIPE
            mutable[2] = subprocess.Popen(command, stdout=out_fileno, stderr=subprocess.PIPE, shell=True)
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
        if stderr_to_stdout:
            return _str(result) + _str(mutable[2].stderr.read()), mutable[2].returncode
        else:
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
        if stderr_to_stdout:
            return _str(mutable[0]) + _str(mutable[1]), mutable[2].returncode
        else:
            return _str(mutable[0]), mutable[2].returncode

# 定义错误处理，当执行命令失败时，返回错误信息
class TiUPExecError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message


def tiup_cluster_exec_command(cname: str, role_list: List[str], shell_cmd: str) -> list[(str, str)]:
    """
    Run a command in the specified cluster.
    Args:
        cname: The name of the cluster.
        role_list: The list of roles to run the command, e.g. ["pd", "tikv"].
        shell_cmd: The command to run.
    Returns:
        The output and error of the command.
        list[(str, str)]: (ip, output)
    """

    compressed_shell_cmd = base64.b64encode(gzip.compress(shell_cmd.encode('utf-8'))).decode()
    role_str = ""
    if role_list:
        role_str = "-R "
        role_str += ",".join(role_list)
    stdout, recode = command_run(f"tiup cluster exec {cname} --command 'echo -n \"{compressed_shell_cmd}\"|base64 -d|gzip -d|bash' {role_str} 2>&1")
    if recode == 0:
        ipv4_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
        re_compile = re.compile(rf"^Run command on.*\nOutputs of.*on ({ipv4_pattern}):\nstdout:\n", re.MULTILINE)
        re_split = re_compile.split(stdout)
        if len(re_split) == 1:
            raise TiUPExecError(stdout)
        o_list = re_split[1:]
        # 对o_list中每一个值去空格
        o_list = [x.strip() for x in o_list]
        return list(zip(o_list[::2], o_list[1::2]))
    else:
        raise TiUPExecError(stdout)

def tiup_cluster_push_file(cname: str,role_list: List[str], local_path: str, remote_path: str):
    """
    向集群的指定role推送文件
    Args:
        cname: The name of the cluster.
        role_list: The list of roles to run the command, e.g. ["pd", "tikv"].
        local_path: 本地文件，必须是文件
        remote_path: 远程文件，如果是文件夹则文件夹必须存在，会将local_path相同文件推送到目标文件夹中，如果是文件则直接按照新文件名生成
    """
    role_str = ""
    if role_list:
        role_str = "-R "
        role_str += ",".join(role_list)
    stdout,recode = command_run(f"tiup cluster push {cname} {local_path} {remote_path} -R {role_str}")
    if recode !=0:
        raise TiUPExecError(f"push file error,msg:{stdout},code:{recode}")


def tiup_cluster_names(ignore: List[str] = None)->List[str]:
    """
    获取当前tiup上所有集群名称
    Args:
        ignore: 忽略的集群名称列表,e.g. ["tidb-test","tidb-test2"]
    """
    clusters = []
    cmd = "tiup cluster list 2>/dev/null"
    stdout, recode = command_run(cmd)
    if recode != 0:
        raise TiUPExecError(f"list cluster error,msg:{stdout},code:{recode}")
    can_print = False
    for each_line in stdout.split("\n"):
        if each_line.startswith("----"):
            can_print = True
            continue
        elif each_line.strip() == "":
            continue
        if can_print:
            each_line_split = each_line.split()
            if len(each_line_split) != 5:
                raise TiUPExecError(f"each cluster line not eq 5,line split:{each_line_split}")
            cname = each_line_split[0]
            if ignore and cname in ignore:
                continue
            clusters.append(cname)
    return clusters

def custom_separated_list(value):
    return re.split(r'[,\s;]+', value.strip())

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser(description="TiUP批量集群操作工具",
     epilog="""示例:
       执行命令:
         python tiup_operator.py exec --cname tidb-test --role pd --shell-cmd "hostname"\n
       推送文件:
         python tiup_operator.py push --cname tidb-test --role pd --local-path /path/to/local/file --remote-path /path/to/remote/file\n
             """
     )
    parser.add_argument("--cname",  help="集群名称，e.g. tidb-test,tidb-test2，不指定表示当前所有集群", type=custom_separated_list, required=False)
    parser.add_argument("--ignore", help="忽略的集群名称列表,e.g. tidb-test,tidb-test2", type=custom_separated_list, required=False)
    parser.add_argument("--role", help="指定的角色列表,e.g. pd tikv，不指定代表整个集群", type=custom_separated_list, required=False)

    # 创建子命令分别表示执行命令和推送文件
    subparsers = parser.add_subparsers(dest="subparser_name")
    exec_parser = subparsers.add_parser("exec", help="执行shell命令")
    exec_parser.add_argument("--shell-cmd", help="需要执行的shell命令，如果是文件名且文件存在则解析为shell命令", required=True)
    push_parser = subparsers.add_parser("push", help="推送文件")
    push_parser.add_argument("--local-path", help="本地文件路径", required=True)
    push_parser.add_argument("--remote-path", help="远程文件路径，如果未指定则目标文件名同local-path文件名，所在文件夹必须存在", required=True)

    args = parser.parse_args()

    clusters = tiup_cluster_names()
    if args.cname:
        # 检查cname是否在集群列表中
        for each_cname in args.cname:
            if each_cname not in clusters:
                raise ValueError(f"{each_cname} not in clusters")
        clusters = args.cname
    if args.ignore:
        for each_ignore in args.ignore:
            if each_ignore not in clusters:
                raise ValueError(f"{each_ignore} not in clusters")
        clusters = list(set(clusters) - set(args.ignore))
    if not clusters:
        logging.error("no cluster to operate")
        exit(1)
    logging.info(f"cluster to operate:{clusters}")

    if args.subparser_name == "exec":
        if os.path.isfile(args.shell_cmd):
            with open(args.shell_cmd, 'r', encoding='utf-8') as f:
                args.shell_cmd = f.read()
        for each_cname in clusters:
            try:
                for ip, output in tiup_cluster_exec_command(each_cname, args.role, args.shell_cmd):
                    logging.info(f"cluster:{each_cname},role:{args.role},ip:{ip},output:{output}")
                    # todo 可写扩展代码逻辑来处理结果输出
            except TiUPExecError as e:
                logging.error(f"cluster:{each_cname},role:{args.role},error:{e}")
    elif args.subparser_name == "push":
        for each_cname in clusters:
            try:
                tiup_cluster_push_file(each_cname, args.role, args.local_path, args.remote_path)
                logging.info(f"push file success,cluster:{each_cname},role:{args.role},local_path:{args.local_path},remote_path:{args.remote_path}")
            except TiUPExecError as e:
                logging.error(f"push file error,cluster:{each_cname},role:{args.role},local_path:{args.local_path},remote_path:{args.remote_path},error:{e}")
    else:
        parser.print_help()
        exit(1)

