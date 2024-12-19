
import sqlite3
from .report_base import header, footer, generate_html_table, generate_html_chart

def fetch_data(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except sqlite3.OperationalError as e:
        return [], []
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    cursor.close()
    return column_names, rows

def report_queries():
    """
    打印的查询列表
    :return:
    """
    queries = {}
    queries["节点信息"] = [
        "table",
        "SELECT * FROM tidb_nodeinfo",
        "查询每个节点的信息，包括节点的IP地址、端口、状态、版本、启动时间"
    ]
    return queries

def report(in_file, out_file):
    """
    从sqlite3中获取信息生成html报表
    :param in_file: sqlite3文件路径
    :type in_file: str
    :param out_file: 输出html文件路径
    :type out_file: str
    """
    conn = sqlite3.connect(in_file)
    conn.text_factory = str  # Set character set to UTF-8
    queries = {
        "节点信息": ["table","SELECT type as 类型 FROM tidb_nodeinfo", "查询每个节点的信息，包括节点的IP地址、端口、状态、版本、启动时间、上线时间、下线时间、节点类型、节点角色、节点状态、节点状态描述"],
        "Os Info": ["table","SELECT * FROM tidb_osinfo", "查询每个节点的操作系统信息，包括CPU数和内存大小"],
        "Disk Info": ["table","SELECT * FROM tidb_diskinfo where used_percent >0.4", "查询每个节点的磁盘信息，包括磁盘的挂载点、磁盘大小、磁盘使用率（大于70%）"],
        "当前活动连接数汇总": ["table","SELECT * FROM tidb_activesessioncount", "总活动连接数信息"],
        "当前活动连接数": ["table","SELECT * FROM tidb_activeconnectioninfo", "查询每个节点的活动连接数"],
        "Memory Info": ["chart","SELECT time,used_percent,used_percent+0.5 FROM tidb_memoryusagedetail", "查询每个节点的内存信息，包括内存大小、内存使用率"],
        "Variables": ["table","SELECT * FROM tidb_variable", "查询tidb的配置信息,包括集群变量和系统全局变量"],
        "Column Collations": ["table","SELECT * FROM tidb_columncollation", "查询表字段上的排序规则，如果不是utf8mb4_bin则会列出（可能会导致索引失效）"],
        "User Privileges": ["table","SELECT * FROM tidb_userprivilege", "查询用户权限信息，包括用户的权限和角色,多个权限则排序后按照逗号分隔"],
        "Slow Queries":  ["table","SELECT * FROM tidb_slowquery", "查询慢查询信息，包括慢查询的sql语句和执行时间，按照Digest和Plan_digest进行分组聚合"],
        "Statement History": ["table","select * from tidb_statementhistory", "查询tidb的历史sql语句，包括sql语句和执行时间，选择大于50ms且执行次数top30的语句，avg_latency为平均执行时间（秒）。假设某种 SQL 每分钟都出现，那 statements_summary_history 中会保存这种 SQL 最近 12 个小时的数据。但如果某种 SQL 只在每天 00:00 ~ 00:30 出现，则 statements_summary_history 中会保存这种 SQL 24 个时间段的数据，每个时间段的间隔都是 1 天，所以会有这种 SQL 最近 24 天的数据。"],
        "Duplicate Indexes": ["table","SELECT * FROM tidb_duplicateindex",  "查询表上的冗余索引，state为DUPLICATE_INDEX表示冗余索引（最左前缀覆盖），state为SUSPECTED_DUPLICATE_INDEX表示疑似冗余索引"],

    }
    queries = report_queries()
    html_content = header()
    html_content += "<body>\n"
    html_content += "<div class='sidebar'>\n"
    html_content += "<h2>导航</h2>\n"
    for title in queries.keys():
        html_content += f"<a href='#{title.replace(' ', '_')}'>{title}</a>\n"
    html_content += "</div>\n"

    html_content += "<div class='table-container'>\n"
    for idx, (title, query_list) in enumerate(queries.items()):
        query = query_list[1]
        describe = query_list[2]
        column_names, rows = fetch_data(conn, query)
        table_id = f"table_{idx}"
        html_content += f"<h2 id='{title.replace(' ', '_')}'>{title}</h2>\n"
        html_content += f"<small style='color: black; font-size: small;'>{describe}</small><br></br>\n"
        if query_list[0] == "chart":
            # column_names第一列为时间，后面的列为数据
            html_content += generate_html_chart(title,column_names, rows, title)
        else:
            html_content += generate_html_table(table_id, column_names, rows)
    html_content += "</div>\n"
    html_content += "</body>\n"
    html_content += footer()

    with open(out_file, 'w', encoding='utf-8') as file:
        file.write(html_content)

    conn.close()


def main():
    report('../default.sqlite3', 'output.html')


if __name__ == "__main__":
    main()
