import pathlib
import sqlite3
from pathlib import Path

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


def generate_html_table(table_id, column_names, rows):
    if not column_names or not rows:
        return "<p>No data available</p>\n"
    html_table = f"<table id='{table_id}' class='display'>\n"
    html_table += "  <thead><tr>\n"
    for column_name in column_names:
        html_table += f"    <th>{column_name}</th>\n"
    html_table += "  </tr></thead>\n"

    html_table += "  <tbody>\n"
    for row in rows:
        html_table += "  <tr>\n"
        for cell in row:
            if isinstance(cell, str):
                cell = cell.replace("└─", "&boxur;").replace("\n", "<br>").replace("│", "&boxv;").replace("├─", "&boxvr;")
            html_table += f"    <td><pre><span class=\"custom-font\">{cell}</span></pre></td>\n"
        html_table += "  </tr>\n"
    html_table += "  </tbody>\n"
    html_table += "</table>\n"
    return html_table

# column_names第一列为时间，后面的列为数据
def generate_html_chart1(column_names, rows, title="", legends=None):
    # 提取时间和分类数据
    if not column_names or not rows:
        return "<p>No data available</p>\n"
    times = [row[0] for row in rows]
    series_data = {column_names[i]: [row[i] for row in rows] for i in range(1, len(column_names))}

    # 图例：如果未提供 legends，则默认使用列名
    if legends is None:
        legends = column_names[1:]

    # 生成数据系列的 JS 配置
    series = []
    colors = ['#007BFF', '#28A745', '#FF5733', '#FFC300', '#DAF7A6']  # 预定义颜色
    for idx, (key, values) in enumerate(series_data.items()):
        series.append(f"""
            {{
                name: '{legends[idx]}',
                data: {values},
                type: 'line',
                smooth: true,
                lineStyle: {{
                    color: '{colors[idx % len(colors)]}',
                    width: 2
                }},
                itemStyle: {{
                    color: '{colors[idx % len(colors)]}'
                }}
            }}
        """)

    # HTML 模板
    html_template = f"""
    
    <div id="chart"></div>
    <style>
       
        #chart {{
            width: 80%;
            height: 500px;
            margin: 20px auto;
            border: 1px solid #ccc;
            background-color: #fff;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        }}
    </style>
    <script>
        // 初始化图表
        var chart = echarts.init(document.getElementById('chart'));

        // 配置项
        var option = {{
            title: {{
                text: '{title}',
                left: 'center',
                textStyle: {{
                    fontSize: 18,
                    color: '#333'
                }}
            }},
            tooltip: {{
                trigger: 'axis',
                backgroundColor: 'rgba(50, 50, 50, 0.7)',
                borderColor: '#ccc',
                borderWidth: 1,
                textStyle: {{
                    color: '#fff'
                }}
            }},
            legend: {{
                data: {legends},
                top: '10%'
            }},
            xAxis: {{
                type: 'category',
                data: {times},
                axisLine: {{
                    lineStyle: {{
                        color: '#888'
                    }}
                }},
                axisLabel: {{
                    rotate: 30,
                    color: '#555'
                }}
            }},
            yAxis: {{
                type: 'value',
                name: 'Connections',
                axisLine: {{
                    lineStyle: {{
                        color: '#888'
                    }}
                }},
                axisLabel: {{
                    color: '#555'
                }}
            }},
            series: [{','.join(series)}],
            dataZoom: [{{
                type: 'slider',
                show: true,
                xAxisIndex: [0]
            }}]
        }};

        // 使用配置项生成图表
        chart.setOption(option);
    </script>
"""
    return html_template


def generate_html_chart(chart_id, column_names, rows, title="", legends=None):
    import json

    if legends is None:
        legends = column_names[1:]

    x_axis = [row[0] for row in rows]
    series_data = []
    color_palette = ['#5470C6', '#91CC75', '#EE6666', '#73C0DE', '#3BA272', '#FC8452']

    for i, legend in enumerate(legends):
        series_data.append({
            "name": legend,
            "type": "line",
            "data": [row[i + 1] for row in rows],
            "smooth": True,
            "lineStyle": {"width": 2},
            "itemStyle": {"color": color_palette[i % len(color_palette)]}
        })

    option = {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 16, "fontWeight": "bold"}},
        "tooltip": {"trigger": "axis"},
        "legend": {"data": legends, "top": "8%", "textStyle": {"fontSize": 12}},
        "grid": {"left": "5%", "right": "5%", "bottom": "10%", "containLabel": True},
        "xAxis": {"type": "category", "data": x_axis, "axisLabel": {"rotate": 30}},
        "yAxis": {"type": "value"},
        "dataZoom": [{"type": "slider", "start": 0, "end": 100}, {"type": "inside"}],
        "series": series_data
    }

    html_template = f"""
    <div id="{chart_id}" style="width: 100%; height: 400px; margin: 0 auto; border: 1px solid #ccc; border-radius: 8px; box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);"></div>
    <script>
        var chartDom = document.getElementById('{chart_id}');
        var myChart = echarts.init(chartDom);
        var option = {json.dumps(option)};
        myChart.setOption(option);
        window.addEventListener('resize', function() {{
            myChart.resize();
        }});
    </script>
    """
    return html_template


def header(local=True):
    """
    生成html的头部
    :param local: 是否使用本地的js和css文件
    """
    # 找到js目录下所有的js文件
    # css_files = list(Path(__file__).parent.joinpath("js").glob('*.css'))
    css_files = ['js/jquery.dataTables.min.css', 'js/fixedHeader.dataTables.min.css', 'js/responsive.dataTables.min.css']
    # 生成jscript代码，嵌入到html中
    if local:
        csss = ""
        for css_file in css_files:
            css_file=Path(__file__).parent.joinpath(css_file)
            with open(css_file, 'r', encoding='utf-8') as file:
                css_content = file.read()
            csss += f"<style>{css_content}</style>\n"
    else:
        csss = """
        <link rel="stylesheet" href="https://cdn.datatables.net/1.11.3/css/jquery.dataTables.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/fixedheader/3.1.9/css/fixedHeader.dataTables.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/responsive/2.2.7/css/responsive.dataTables.min.css">
        """
    # 将echart放到前面，避免无法渲染
    java_scripts = ""
    if local:
        js_file="js/echarts.min.js"
        js_file=Path(__file__).parent.joinpath(js_file)
        with open(js_file, 'r', encoding='utf-8') as file:
            echarts_content = file.read()
        java_scripts += f"<script>{echarts_content}</script>\n"
    else:
        java_scripts = """
        <script src="https://cdn.jsdelivr.net/npm/echarts/dist/echarts.min.js"></script>
        """


    header = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Interactive Table</title>
        <style>
        .custom-font {
            font-family: 'Courier New', monospace; /* 设置字体 */
            <!--  font-family: 'SimSun', serif; /* 设置为宋体 */ -->
        }
        th, td {
            min-width: 100px; /* 默认宽度 */
            white-space: nowrap;
            border: 1px solid #ddd; /* Add border for table cells */
        }
        thead th {
            background-color: #696969; /* 设置标题行背景颜色为真空灰 */
            color: #ffffff; /* 设置标题字体颜色为白色 */
        }
        </style>
        """ + csss + """
        <style>
        body {
            font-family: Arial, sans-serif;
        }
        .table-container {
            margin-left: 220px; /* Space for the sidebar */
        }
        .sidebar {
            position: fixed;
            top: 0;
            left: 0;
            width: 200px;
            height: 100%;
            background-color: #333;
            padding-top: 20px;
            color: white;
            z-index: 1000; /* Ensure sidebar is on top */
        }
        .sidebar a {
            padding: 10px 15px;
            text-decoration: none;
            font-size: 18px;
            color: white;
            display: block;
        }
        .sidebar a:hover {
            background-color: #575757;
        }
        h2 {
            margin-top: 60px; /* Provide space between the title and the sticky header */
        }
        table.dataTable thead th, table.dataTable tbody td {
            box-sizing: border-box;
        }
        </style>
        """ + java_scripts + """
        </head>
        <body>
        """
    return header


def footer(local=True):
    """
    生成html的尾部
    :param local: 是否使用本地的js和css文件
    :return:
    """
    # 找到js目录下所有的js文件，要确保顺序，因为有依赖关系
    # js_files = list(pathlib.Path('js').glob('*.js'))
    js_files = ['js/jquery-3.6.0.min.js', 'js/jquery-ui.min.js', 'js/jquery.dataTables.min.js',
                'js/dataTables.fixedHeader.min.js', 'js/dataTables.responsive.min.js']
    # 生成jscript代码，嵌入到html中
    jscripts = ""
    if local:
        for js_file in js_files:
            js_file = Path(__file__).parent.joinpath(js_file)
            with open(js_file, 'r', encoding='utf-8') as file:
                js_content = file.read()
            jscripts += f"<script>{js_content}</script>\n"
    else:
        jscripts = """
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.12.1/jquery-ui.min.js"></script>
        <script src="https://cdn.datatables.net/1.11.3/js/jquery.dataTables.min.js"></script>
        <script src="https://cdn.datatables.net/fixedheader/3.1.9/js/dataTables.fixedHeader.min.js"></script>
        <script src="https://cdn.datatables.net/responsive/2.2.7/js/dataTables.responsive.min.js"></script>
        """
    footer = jscripts + """
    <script>
        $(document).ready(function() {
            // Initialize DataTables for each table
            $('table.display').DataTable({
                "fixedHeader": true,
                "paging": true,
                "ordering": true,
                "searching": true,
                "responsive": true
            });
            // Set default column width based on the longest cell content
            $('table.display').each(function() {
                var table = $(this);
                table.find('th').each(function(index) {
                    var maxWidth = 300; // 设置最大宽度
                    var maxLength = 0;
                    table.find('tr').each(function() {
                        var cell = $(this).find('td').eq(index);
                        if (cell.length) {
                            var cellText = cell.text();
                            if (cellText.length > maxLength) {
                                maxLength = cellText.length;
                            }
                        }
                    });
                    var width = Math.min(maxLength * 10, maxWidth); // 计算宽度并限制最大宽度
                    $(this).css('width', width + 'px');
                });
            });
            // Enable column resizing using jQuery UI
            $('th').resizable({
                handles: "e",
                minWidth: 100, // 设置最小宽度,
                resize: function(event, ui) {
                    var sizerID = "#" + $(this).attr("id") + "-sizer";
                    $(sizerID).width(ui.size.width);
                }
            });
        });
        </script>
    </body>
    </html>
    """
    return footer


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
        "Node Info": ["table","SELECT * FROM tidb_nodeinfo", "查询每个节点的信息，包括节点的IP地址、端口、状态、版本、启动时间、上线时间、下线时间、节点类型、节点角色、节点状态、节点状态描述"],
        "Os Info": ["table","SELECT * FROM tidb_osinfo", "查询每个节点的操作系统信息，包括CPU数和内存大小"],
        "Disk Info": ["table","SELECT * FROM tidb_diskinfo where used_percent >0.4", "查询每个节点的磁盘信息，包括磁盘的挂载点、磁盘大小、磁盘使用率（大于70%）"],
        "Memory Info": ["chart","SELECT time,used_percent,used_percent+0.5 FROM tidb_memoryusagedetail", "查询每个节点的内存信息，包括内存大小、内存使用率"],
        "Variables": ["table","SELECT * FROM tidb_variable", "查询tidb的配置信息,包括集群变量和系统全局变量"],
        "Column Collations": ["table","SELECT * FROM tidb_columncollation", "查询表字段上的排序规则，如果不是utf8mb4_bin则会列出（可能会导致索引失效）"],
        "User Privileges": ["table","SELECT * FROM tidb_userprivilege", "查询用户权限信息，包括用户的权限和角色,多个权限则排序后按照逗号分隔"],
        "Slow Queries":  ["table","SELECT * FROM tidb_slowquery", "查询慢查询信息，包括慢查询的sql语句和执行时间，按照Digest和Plan_digest进行分组聚合"],
        "Statement History": ["table","select * from tidb_statementhistory", "查询tidb的历史sql语句，包括sql语句和执行时间，选择大于50ms且执行次数top30的语句，avg_latency为平均执行时间（秒）。假设某种 SQL 每分钟都出现，那 statements_summary_history 中会保存这种 SQL 最近 12 个小时的数据。但如果某种 SQL 只在每天 00:00 ~ 00:30 出现，则 statements_summary_history 中会保存这种 SQL 24 个时间段的数据，每个时间段的间隔都是 1 天，所以会有这种 SQL 最近 24 天的数据。"],
        "Duplicate Indexes": ["table","SELECT * FROM tidb_duplicateindex",  "查询表上的冗余索引，state为DUPLICATE_INDEX表示冗余索引（最左前缀覆盖），state为SUSPECTED_DUPLICATE_INDEX表示疑似冗余索引"],

    }

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
