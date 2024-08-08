"""
反亲和规则检查器
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
import requests


class ECSInfo:
    """
    定义虚拟机信息类，用户查找虚拟机的主机名和IP
    """

    def __init__(self, vm_hostname, vm_ip, physical_hostname, physical_ip):
        """
        :type vm_hostname: str
        :type vm_ip: str
        :type physical_hostname: str
        :type physical_ip: str
        """
        self.vm_hostname = vm_hostname
        self.vm_ip = vm_ip
        self.physical_hostname = physical_hostname
        self.physical_ip = physical_ip


class TiDBClusterInfo:
    """
    定义TiDB集群信息类
    """

    def __init__(self, cluster_name, version, location_labels, isolation_level):
        """
        :type cluster_name: str
        :type version: str
        :type location_labels: List[str]
        :type isolation_level: str
        """
        self.cluster_name = cluster_name  # 集群名称
        self.version = version  # 集群版本
        self.location_labels = location_labels  # 集群拓扑层级
        self.isolation_level = isolation_level  # 最小强制拓扑隔离级别
        self.roles: Dict[str, List['TiDBRoleInfo']] = {}  # 角色信息

    def add_role_info(self, role_info: 'TiDBRoleInfo'):
        if role_info.role_type not in self.roles:
            self.roles[role_info.role_type] = []
        self.roles[role_info.role_type].append(role_info)


class RoleType:
    TIDB = "tidb"
    TIKV = "tikv"
    PD = "pd"
    TIFLASH = "tiflash"
    OTHER = "other"


class TiDBRoleInfo:
    """
    定义TiDB角色信息类
    """

    def __init__(self, role_type, role_id, host_ip, labels, ecs_info):
        """
        :type role_type: str
        :type role_id: str
        :type host_ip: str
        :type labels: Dict[str, str]
        :type ecs_info: ECSInfo
        """
        self.role_type = role_type  # 角色类型
        self.role_id = role_id  # 角色ID
        self.host_ip = host_ip  # 主机IP
        self.labels = labels  # 节点标签（用于反亲和检查）
        self.ecs_info = ecs_info  # 虚拟机信息


class AntiAffinityRule(ABC):
    @abstractmethod
    def check(self, cluster_info):
        """
        :type cluster_info: TiDBClusterInfo
        :rtype: List[str]
        """
        pass


# TiDB节点的反亲和规则实现
class TiDBAntiAffinityRule(AntiAffinityRule):
    def check(self, cluster_info):
        """
        :type cluster_info: TiDBClusterInfo
        :rtype: List[str]
        """
        violations = []
        tidb_nodes = cluster_info.roles.get(RoleType.TIDB, [])
        physical_hosts = set(node.ecs_info.physical_hostname for node in tidb_nodes)
        if len(physical_hosts) < 2:
            violations.append("TiDB nodes are not spread across at least 2 physical hosts.")
        # todo 添加更多规则
        return violations


# PD节点的反亲和规则实现
class PDAntiAffinityRule(AntiAffinityRule):
    def check(self, cluster_info):
        """
        :type cluster_info: TiDBClusterInfo
        :rtype: List[str]
        """
        violations = []
        pd_nodes = cluster_info.roles.get(RoleType.PD, [])
        physical_hosts = set(node.ecs_info.physical_hostname for node in pd_nodes)
        if len(physical_hosts) < 3:
            violations.append("PD nodes are not spread across at least 3 physical hosts.")
        for host in physical_hosts:
            count = sum(1 for node in pd_nodes if node.ecs_info.physical_hostname == host)
            if count > 1:
                violations.append(f"More than one PD node on the same host: {host}.")
        return violations


# TiKV节点的反亲和规则实现
class TiKVAntiAffinityRule(AntiAffinityRule):
    def check(self, cluster_info):
        """
        :type cluster_info: TiDBClusterInfo
        :rtype: List[str]
        """
        violations = []
        tikv_nodes = cluster_info.roles.get(RoleType.TIKV, [])
        for node in tikv_nodes:
            if node.labels.get(cluster_info.isolation_level) != node.ecs_info.physical_hostname:
                violations.append(
                    f"TiKV node {node.role_id} on host {node.ecs_info.physical_hostname} violates the isolation level.")
        # todo 添加更多规则
        return violations


# 反亲和校验器
class AntiAffinityChecker:
    def __init__(self):
        self.rules = {
            RoleType.TIDB: TiDBAntiAffinityRule(),
            RoleType.PD: PDAntiAffinityRule(),
            RoleType.TIKV: TiKVAntiAffinityRule(),
            # todo 添加tiflash规则
        }

    def check(self, cluster_info):
        """
        :type cluster_info: TiDBClusterInfo
        :rtype: Dict[str, List[str]]
        """
        violations = {}
        for role_type, rule in self.rules.items():
            if role_type in cluster_info.roles:
                issues = rule.check(cluster_info)
                if issues:
                    violations[role_type] = issues
        return violations


# 从API获取集群信息
def get_pd_config(pd_address: str) -> Dict:
    response = requests.get(f"http://{pd_address}/pd/api/v1/config")
    response.raise_for_status()
    return response.json()


def get_tikv_config(tikv_address: str) -> Dict:
    response = requests.get(f"http://{tikv_address}:20180/config")
    response.raise_for_status()
    return response.json()


def main():
    pd_address = "192.168.31.201:2379"
    tikv_address = "192.168.31.201:20180"

    # 获取PD的配置信息
    pd_config = get_pd_config(pd_address)

    # 创建TiDBClusterInfo对象
    cluster_info = TiDBClusterInfo(
        cluster_name="test-cluster",
        version="v6.1.0",
        location_labels=pd_config['replication']['location-labels'],
        isolation_level=pd_config['replication']['isolation-level']
    )

    # 添加集群节点信息
    ecs_info1 = ECSInfo("vm1", "10.0.0.1", "host1", "192.168.0.1")
    role_info1 = TiDBRoleInfo(RoleType.TIDB, "tidb1", "10.0.0.1", {}, ecs_info1)
    cluster_info.add_role_info(role_info1)

    ecs_info2 = ECSInfo("vm2", "10.0.0.2", "host1", "192.168.0.1")
    role_info2 = TiDBRoleInfo(RoleType.TIDB, "tidb2", "10.0.0.2", {}, ecs_info2)
    cluster_info.add_role_info(role_info2)

    ecs_info3 = ECSInfo("vm3", "10.0.0.3", "host2", "192.168.0.2")
    role_info3 = TiDBRoleInfo(RoleType.PD, "pd1", "10.0.0.3", {}, ecs_info3)
    cluster_info.add_role_info(role_info3)

    # 创建反亲和检查器
    checker = AntiAffinityChecker()

    # 检查是否存在反亲和违规
    violations = checker.check(cluster_info)

    # 输出结果
    for role, issues in violations.items():
        print(f"{role} violations:")
        for issue in issues:
            print(f"  - {issue}")


if __name__ == "__main__":
    main()
