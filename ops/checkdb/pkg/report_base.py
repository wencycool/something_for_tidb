from pathlib import Path
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
                cell = cell.replace("└─", "&boxur;").replace("\n", "<br>").replace("│", "&boxv;").replace("├─",
                                                                                                          "&boxvr;")
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


def generate_html_chart(chart_id, column_names, rows, title="", dimension_level=0):
    """
        生成HTML图表
        :param chart_id: 图表的HTML元素ID
        :type chart_id: str
        :param column_names: 列名列表
        :type column_names: list
        :param rows: 数据行列表
        :type rows: list
        :param title: 图表标题
        :type title: str
        :param dimension_level: 维度级别（0, 1 或 2）
        :type dimension_level: int
        :return: HTML图表字符串
        :rtype: str
        """
    import json
    from collections import defaultdict

    if dimension_level not in [0, 1, 2]:
        raise ValueError("dimension_level 仅支持 0、1 或 2")

    time_col = column_names[0]  # 时间列
    dimension_cols = column_names[1:1 + dimension_level] if dimension_level > 0 else []
    metric_names = column_names[1 + dimension_level:]

    x_axis = sorted(set(row[0] for row in rows))
    grouped_data = defaultdict(lambda: defaultdict(list))

    for row in rows:
        time = row[0]
        if dimension_level == 0:
            dimension_key = ""
        else:
            dimension_key = " - ".join(str(row[i]) for i in range(1, 1 + dimension_level))

        for i, metric in enumerate(metric_names):
            grouped_data[dimension_key][metric].append({"time": time, "value": row[1 + dimension_level + i]})

    color_palette = ['#5470C6', '#91CC75', '#EE6666', '#73C0DE', '#3BA272', '#FC8452']
    series_data = []
    color_index = 0

    for dimension_key, metrics in grouped_data.items():
        for metric_name, values in metrics.items():
            name = f"{metric_name}" if dimension_level == 0 else f"{dimension_key} - {metric_name}"
            series_data.append({
                "name": name,
                "type": "line",
                "data": [v["value"] for v in values],
                "smooth": True,
                "lineStyle": {"width": 2},
                "itemStyle": {"color": color_palette[color_index % len(color_palette)]}
            })
            color_index += 1

    option = {
        "title": {"text": title, "left": "center"},
        "tooltip": {"trigger": "axis"},
        "legend": {"data": [s["name"] for s in series_data], "top": "8%"},
        "grid": {"left": "5%", "right": "5%", "bottom": "10%", "containLabel": True},
        "toolbox": {
            "feature": {
                "saveAsImage": {},
                "restore": {},
                "dataZoom": {"yAxisIndex": "none"},
                "magicType": {"type": ["line", "bar"]},
            }
        },
        "xAxis": {
            "type": "category",
            "data": x_axis,
            "axisLabel": {"rotate": 30}
        },
        "yAxis": {"type": "value"},
        "dataZoom": [{"type": "slider", "start": 0, "end": 100}, {"type": "inside","disabled": True}],
        "series": series_data
    }

    # Ensure the option is properly serialized for use in JavaScript
    option_json = json.dumps(option)

    html_template = f"""
        <style>
            /* 禁止鼠标滚轮缩放图表 */
            #chart-container {{
                position: relative;
            }}
            #chart-container .echarts {{
                pointer-events: none;
            }}
            /* 禁止图表滚轮缩放 */
            #chart-container .echarts {{
                touch-action: none; /* 禁止触摸缩放 */
                -ms-touch-action: none; /* 禁止MS触摸缩放 */
            }}
        </style>
        <div id="chart-container">
            <div id="{chart_id}" style="width: 100%; height: 400px; margin: 0 auto; border: 1px solid #ccc; border-radius: 8px; box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);"></div>
        </div>
        <script>
            // 初始化图表
            var chartDom = document.getElementById('{chart_id}');
            var myChart = echarts.init(chartDom);
            var option = {option_json};
            myChart.setOption(option);
            window.addEventListener('resize', function() {{
                myChart.resize();
            }});
        </script>
    """
    return html_template

def generate_html_chart_with_instance_and_mount(chart_id, column_names, rows, title=""):
    import json
    from collections import defaultdict

    # 提取字段
    time_col = column_names[0]
    instance_col = column_names[1]
    mount_col = column_names[2]
    metric_names = column_names[3:]

    # 按 instance 和 mount_point 分组
    grouped_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for row in rows:
        time = row[0]
        instance = row[1]
        mount = row[2]
        for i, metric in enumerate(metric_names):
            grouped_data[instance][mount][metric].append({"time": time, "value": row[3 + i]})

    # 提取唯一的 instance 和 mount_point
    instances = sorted(grouped_data.keys())
    mounts_by_instance = {instance: sorted(grouped_data[instance].keys()) for instance in instances}

    # 默认展示第一个 instance 和 mount_point 的数据
    default_instance = instances[0]
    default_mount = mounts_by_instance[default_instance][0]

    # 生成默认图表数据
    def generate_series_data(instance, mount):
        color_palette = ['#5470C6', '#91CC75', '#EE6666', '#73C0DE', '#3BA272', '#FC8452']
        series_data = []
        color_index = 0
        for metric, values in grouped_data[instance][mount].items():
            series_data.append({
                "name": metric,
                "type": "line",
                "data": [v["value"] for v in values],
                "smooth": True,
                "lineStyle": {"width": 2},
                "itemStyle": {"color": color_palette[color_index % len(color_palette)]}
            })
            color_index += 1
        return series_data

    x_axis = sorted(set(row[0] for row in rows))
    option = {
        "title": {"text": title, "left": "center"},
        "tooltip": {"trigger": "axis"},
        "legend": {"top": "10%"},
        "grid": {"left": "5%", "right": "5%", "bottom": "10%", "containLabel": True},
        "toolbox": {
            "feature": {
                "saveAsImage": {},
                "restore": {},
                "dataZoom": {"yAxisIndex": "none"},
                "magicType": {"type": ["line", "bar"]},
            }
        },
        "xAxis": {
            "type": "category",
            "data": x_axis,
            "axisLabel": {"rotate": 30}
        },
        "yAxis": {"type": "value"},
        "dataZoom": [{"type": "slider", "start": 0, "end": 100}, {"type": "inside", "disabled": True}],
        "series": generate_series_data(default_instance, default_mount)
    }

    option_json = json.dumps(option)
    grouped_data_json = json.dumps(grouped_data)
    mounts_by_instance_json = json.dumps(mounts_by_instance)

    html_template = f"""
        <div id="chart-container" style="position: relative;">
            <!-- Instance 下拉框 -->
            <div style="position: absolute; top: 10px; left: 10px; z-index: 1000;">
                <label for="instance-selector">Instance:</label>
                <select id="instance-selector" style="width: 150px;">
                    {"".join(f'<option value="{instance}">{instance}</option>' for instance in instances)}
                </select>
            </div>
            <!-- Mount Point 下拉框 -->
            <div style="position: absolute; top: 10px; left: 300px; z-index: 1000;">
                <label for="mount-selector">Mount Point:</label>
                <select id="mount-selector" style="width: 150px;">
                    {"".join(f'<option value="{mount}">{mount}</option>' for mount in mounts_by_instance[default_instance])}
                </select>
            </div>
            <div id="{chart_id}" style="width: 100%; height: 400px; margin: 0 auto; border: 1px solid #ccc; border-radius: 8px; box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);"></div>
        </div>
        <script>
            var chartDom = document.getElementById('{chart_id}');
            var myChart = echarts.init(chartDom);
            var option = {option_json};

            var groupedData = {grouped_data_json};
            var mountsByInstance = {mounts_by_instance_json};

            function updateMountSelector(instance) {{
                var mountSelector = document.getElementById('mount-selector');
                mountSelector.innerHTML = mountsByInstance[instance].map(function(mount) {{
                    return `<option value="${{mount}}">${{mount}}</option>`;
                }}).join('');
            }}

            function updateChart(instance, mount) {{
                var newSeries = [];
                if (groupedData[instance] && groupedData[instance][mount]) {{
                    Object.keys(groupedData[instance][mount]).forEach(function(metric) {{
                        var values = groupedData[instance][mount][metric].map(function(item) {{
                            return item.value;
                        }});
                        newSeries.push({{
                            name: metric,
                            type: "line",
                            data: values,
                            smooth: true
                        }});
                    }});
                }}
                option.series = newSeries;
                myChart.setOption(option, true);
            }}

            document.getElementById('instance-selector').addEventListener('change', function(event) {{
                var selectedInstance = event.target.value;
                updateMountSelector(selectedInstance);
                var firstMount = mountsByInstance[selectedInstance][0];
                updateChart(selectedInstance, firstMount);
            }});

            document.getElementById('mount-selector').addEventListener('change', function(event) {{
                var selectedInstance = document.getElementById('instance-selector').value;
                var selectedMount = event.target.value;
                updateChart(selectedInstance, selectedMount);
            }});

            window.addEventListener('resize', function() {{
                myChart.resize();
            }});

            // 初始化图表
            myChart.setOption(option);
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
    css_files = ['js/jquery.dataTables.min.css', 'js/fixedHeader.dataTables.min.css',
                 'js/responsive.dataTables.min.css']
    # 生成jscript代码，嵌入到html中
    if local:
        csss = ""
        for css_file in css_files:
            css_file = Path(__file__).parent.joinpath(css_file)
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
        js_file = "js/echarts.min.js"
        js_file = Path(__file__).parent.joinpath(js_file)
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

