import pymysql
from typing import List


class Variable:
    def __init__(self):
        self.type = ""  # 如果是系统参数则为variable,如果是集群参数则为：tidb,pd,tikv,tiflash
        self.name = ""
        self.value = ""


def get_variables(conn):
    """
    获取数据库中所有变量
    ：param conn: 数据库连接
    ：type conn: pymysql.connections.Connection
    :rtype: List[Variable]
    """
    variables: List[Variable] = []
    cursor = conn.cursor()
    cursor.execute("show global variables")
    for row in cursor:
        variable = Variable()
        variable.type = "variable"
        variable.name = row[0]
        variable.value = row[1]
        variables.append(variable)
    cursor.execute("show config")
    # 过滤器，如果一个参数出现过则不再添加
    var_filter = {}
    for row in cursor:
        if (row[0], row[2]) in var_filter:
            continue
        var_filter[(row[0], row[2])] = True
        variable = Variable()
        variable.type = row[0]
        variable.name = row[2]
        variable.value = row[3]
        variables.append(variable)
    cursor.close()
    return variables


class ColumnCollation:
    def __init__(self):
        self.table_schema = ""
        self.table_name = ""
        self.column_name = ""
        self.collation_name = ""


def get_column_collations(conn):
    """
    获取数据库中所有列的排序规则
    ：param conn: 数据库连接
    ：type conn: pymysql.connections.Connection
    :rtype: List[ColumnCollation]
    """
    collations: List[ColumnCollation] = []
    cursor = conn.cursor()
    cursor.execute("select table_schema,table_name,column_name,collation_name from information_schema.columns where COLLATION_NAME !='utf8mb4_bin' and table_schema not in ('mysql','INFORMATION_SCHEMA','PERFORMANCE_SCHEMA')")
    for row in cursor:
        collation = ColumnCollation()
        collation.table_schema = row[0]
        collation.table_name = row[1]
        collation.column_name = row[2]
        collation.collation_name = row[3]
        collations.append(collation)
    cursor.close()
    return collations


class UserPrivilege:
    def __init__(self):
        self.user = ""
        self.host = ""
        self.privilege: [str] = []  # 按照权限名称排序


def get_user_privileges(conn):
    """
    获取数据库中所有用户的权限
    ：param conn: 数据库连接
    ：type conn: pymysql.connections.Connection
    :rtype: List[UserPrivilege]
    """
    privileges: List[UserPrivilege] = []
    cursor = conn.cursor()
    cursor.execute("select user,host from mysql.user where user !=''")
    for row in cursor:
        privilege = UserPrivilege()
        privilege.user = row[0]
        privilege.host = row[1]
        cursor_inner = conn.cursor()
        cursor_inner.execute(f"show grants for '{privilege.user}'@'{privilege.host if not None else '%'}'")
        for row_inner in cursor_inner:
            privilege.privilege.append(row_inner[0])
        cursor_inner.close()
        # 对privilege排序
        privilege.privilege.sort()
        privileges.append(privilege)
    cursor.close()
    return privileges


class VersionNotMatchError(Exception):
    def __init__(self, message):
        self.message = message


class NodeVersion:
    """
    节点版本信息,如果集群中各节点版本不一致，则抛出异常，如果同一节点类型的git_hash不一致，则抛出异常
    """
    def __init__(self):
        self.node_type = ""  # 节点类型
        self.version = ""  # 版本号
        self.git_hash = ""  # git hash，用于判断补丁版本


def get_node_versions(conn):
    """
    获取数据库中所有节点的版本信息
    ：param conn: 数据库连接
    ：type conn: pymysql.connections.Connection
    :rtype: List[NodeVersion]
    """
    versions: List[NodeVersion] = []
    all_node_versions = []
    cursor = conn.cursor()
    cursor.execute("select type,version,git_hash from information_schema.cluster_info")
    for row in cursor:
        version = NodeVersion()
        version.node_type = row[0]
        version.version = row[1]
        version.git_hash = row[2]
        all_node_versions.append(version)
    cursor.close()
    # 检查版本是否一致
    version_map = {}
    base_version = ""
    for version in all_node_versions:
        if base_version == "":
            base_version = version.version
        if base_version != version.version:
            raise VersionNotMatchError("Version not match")
        if version.node_type not in version_map:
            version_map[version.node_type] = version
        else:
            # 对同一类型节点，检查git hash是否一致
            if version_map[version.node_type].git_hash != version.git_hash:
                raise VersionNotMatchError(f"Git hash not match for {version.node_type}")
    for version in all_node_versions:
        versions.append(version)
    return versions


if __name__ == "__main__":
    conn = pymysql.connect(host="192.168.31.201", port=4000, user="root", password="123", charset="utf8mb4", database="information_schema")
    variables = get_variables(conn)
    for variable in variables:
        print(f"{variable.type} {variable.name} {variable.value}")
    collations = get_column_collations(conn)
    for collation in collations:
        print(f"{collation.table_schema} {collation.table_name} {collation.column_name} {collation.collation_name}")
    privileges = get_user_privileges(conn)
    for privilege in privileges:
        print(f"{privilege.user}@{privilege.host} {','.join(privilege.privilege)}")
    versions = get_node_versions(conn)
    for version in versions:
        print(f"{version.node_type} {version.version} {version.git_hash}")
    from slow_query import get_slow_query_info
    from datetime import datetime
    start_time = datetime.strptime("2024-08-01 20:40:08", "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime("2024-08-10 19:40:08", "%Y-%m-%d %H:%M:%S")
    slow_queries = get_slow_query_info(conn, start_time, end_time)
    for slow_query in slow_queries:
        print(f"{slow_query.digest} {slow_query.plan_digest} {slow_query.query} {slow_query.plan} {slow_query.exec_count} {slow_query.succ_count} {slow_query.sum_query_time} {slow_query.avg_query_time} {slow_query.sum_total_keys} {slow_query.avg_total_keys} {slow_query.sum_process_keys} {slow_query.avg_process_keys} {slow_query.min_time} {slow_query.max_time} {slow_query.mem_max} {slow_query.disk_max} {slow_query.avg_result_rows} {slow_query.max_result_rows} {slow_query.plan_from_binding}")
    conn.close()
