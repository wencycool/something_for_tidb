import pathlib
import sqlite3


def fetch_data(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    cursor.close()
    return column_names, rows


def generate_html_table(table_id, column_names, rows):
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


def header(local=True):
    """
    生成html的头部
    :param local: 是否使用本地的js和css文件
    """
    # 找到js目录下所有的js文件
    css_files = list(pathlib.Path('js').glob('*.css'))
    # 生成jscript代码，嵌入到html中
    if local:
        csss = ""
        for css_file in css_files:
            with open(css_file, 'r', encoding='utf-8') as file:
                css_content = file.read()
            csss += f"<style>{css_content}</style>\n"
    else:
        csss = """
        <link rel="stylesheet" href="https://cdn.datatables.net/1.11.3/css/jquery.dataTables.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/fixedheader/3.1.9/css/fixedHeader.dataTables.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/responsive/2.2.7/css/responsive.dataTables.min.css">
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
        th, td {
            white-space: nowrap;
            border: 1px solid #ddd; /* Add border for table cells */
        }
        th {
            cursor: pointer;
            background-color: #f2f2f2; /* Light grey background for the header */
        }
        table.dataTable thead th, table.dataTable tbody td {
            box-sizing: border-box;
        }
        </style>
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
            // Enable column resizing using jQuery UI
            $('th').resizable({
                handles: "e",
                minWidth: 50
            });
        });
        </script>
    </body>
    </html>
    """
    return footer


def main():
    conn = sqlite3.connect('dbinfo.db')
    conn.text_factory = str  # Set character set to UTF-8

    queries = {
        "Node Versions": "SELECT * FROM tidb_nodeversion",
        "Variables": "SELECT * FROM tidb_variable",
        "Column Collations": "SELECT * FROM tidb_columncollation",
        "User Privileges": "SELECT * FROM tidb_userprivilege",
        "Slow Queries": "SELECT * FROM tidb_slowquery",
        "Duplicate Indexes": "SELECT * FROM tidb_duplicateindex"
    }

    html_content = header()
    html_content += "<body>\n"
    html_content += "<div class='sidebar'>\n"
    html_content += "<h2>导航</h2>\n"
    for title in queries.keys():
        html_content += f"<a href='#{title.replace(' ', '_')}'>{title}</a>\n"
    html_content += "</div>\n"

    html_content += "<div class='table-container'>\n"
    for idx, (title, query) in enumerate(queries.items()):
        column_names, rows = fetch_data(conn, query)
        table_id = f"table_{idx}"
        html_content += f"<h2 id='{title.replace(' ', '_')}'>{title}</h2>\n"
        html_content += generate_html_table(table_id, column_names, rows)
    html_content += "</div>\n"
    html_content += footer()
    html_content += "</body>\n"

    with open('output.html', 'w', encoding='utf-8') as file:
        file.write(html_content)

    conn.close()


if __name__ == "__main__":
    main()
