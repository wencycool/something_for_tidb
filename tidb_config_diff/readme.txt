PS E:\PythonProjects\something_for_tidb\tidb_config_diff> python main.py -h
usage: main.py [-h] [-d DB] {collect,report} ...

参数对比工具

options:
  -h, --help        show this help message and exit
  -d DB, --db DB    sqlite3的存放地址

Subcommands:
  {collect,report}
    collect         搜集系统参数和集群参数
    report          参数对比输出
PS E:\PythonProjects\something_for_tidb\tidb_config_diff> python main.py collect -h
usage: main.py collect [-h] [-H HOST] [-P PORT] [-u USER] [-p [PASSWORD]]

options:
  -h, --help            show this help message and exit
  -H HOST, --host HOST  IP地址
  -P PORT, --port PORT  端口号
  -u USER, --user USER  用户名
  -p [PASSWORD], --password [PASSWORD]
                        密码
PS E:\PythonProjects\something_for_tidb\tidb_config_diff> python main.py report -h
usage: main.py report [-h] [-t TYPE] [-o OUTPUT] [-l] [--table1 TABLE1] [--table2 TABLE2] [--limit LIMIT]

options:
  -h, --help            show this help message and exit
  -t TYPE, --type TYPE  输出类型，支持excel,text,stdout
  -o OUTPUT, --output OUTPUT
                        输出文件
  -l, --list-tables     打印当前已经完成采集的系统表
  --table1 TABLE1       对比的第一个表
  --table2 TABLE2       对比的第二个表
  --limit LIMIT         打印输出行数,默认输出所有行


PS E:\PythonProjects\something_for_tidb\tidb_config_diff> python main.py collect -H 192.168.31.201 -P 4000 -u root -p
Enter your password:
PS E:\PythonProjects\something_for_tidb\tidb_config_diff> python main.py collect -H 192.168.31.201 -P 4001 -u root -p
Enter your password:

#查看当前采集了哪些参数表
PS E:\PythonProjects\something_for_tidb\tidb_config_diff> python main.py report -l
TABLE LIST:[tidb_cfg_v7_5_0,tidb_cfg_v7_1_2]

#默认情况下会找版本最高的2个参数表做比较（这里--limit参数只打印前N行，避免全部输出）
PS E:\PythonProjects\something_for_tidb\tidb_config_diff> python main.py report --limit 5
┌──────────┬─────────┬──────────┬───────────────────────────────────────┬───────────────────────────────┬────────────────────┐
│   Number │ scope   │ type     │ var_name                              │ var_value_v7.5.0              │ var_value_v7.1.2   │
├──────────┼─────────┼──────────┼───────────────────────────────────────┼───────────────────────────────┼────────────────────┤
│        1 │ global  │ variable │ tidb_allow_tiflash_cop                │ OFF                           │ NotFound           │
├──────────┼─────────┼──────────┼───────────────────────────────────────┼───────────────────────────────┼────────────────────┤
│        2 │ global  │ variable │ tidb_analyze_skip_column_types        │ json,blob,mediumblob,longblob │ NotFound           │
├──────────┼─────────┼──────────┼───────────────────────────────────────┼───────────────────────────────┼────────────────────┤
│        3 │ global  │ variable │ tidb_build_sampling_stats_concurrency │ 2                             │ NotFound           │
├──────────┼─────────┼──────────┼───────────────────────────────────────┼───────────────────────────────┼────────────────────┤
│        4 │ global  │ variable │ tidb_cloud_storage_uri                │                               │ NotFound           │
├──────────┼─────────┼──────────┼───────────────────────────────────────┼───────────────────────────────┼────────────────────┤
│        5 │ global  │ variable │ tidb_enable_async_merge_global_stats  │ ON                            │ NotFound           │
└──────────┴─────────┴──────────┴───────────────────────────────────────┴───────────────────────────────┴────────────────────┘


上述可以看到该参数名称在v7.5.0版本中有，v7.1.2中没有，说明v7.5.0中新增。


E:\PythonProjects\something_for_tidb\tidb_config_diff> python main.py report --limit 100,5
┌──────────┬─────────┬──────────┬───────────────────────────────────────────────┬────────────────────┬────────────────────┐
│   Number │ scope   │ type     │ var_name                                      │ var_value_v7.5.0   │ var_value_v7.1.2   │
├──────────┼─────────┼──────────┼───────────────────────────────────────────────┼────────────────────┼────────────────────┤
│      101 │ global  │ tiflash  │ raftstore-proxy.raftstore.allow-remove-leader │ NotFound           │ false              │
├──────────┼─────────┼──────────┼───────────────────────────────────────────────┼────────────────────┼────────────────────┤
│      102 │ global  │ variable │ tidb_remove_orderby_in_subquery               │ ON                 │ OFF                │
├──────────┼─────────┼──────────┼───────────────────────────────────────────────┼────────────────────┼────────────────────┤
│      103 │ global  │ tidb     │ performance.enable-stats-cache-mem-quota      │ true               │ false              │
├──────────┼─────────┼──────────┼───────────────────────────────────────────────┼────────────────────┼────────────────────┤
│      104 │ global  │ tidb     │ performance.force-init-stats                  │ true               │ false              │
├──────────┼─────────┼──────────┼───────────────────────────────────────────────┼────────────────────┼────────────────────┤
│      105 │ global  │ tidb     │ performance.lite-init-stats                   │ true               │ false              │
└──────────┴─────────┴──────────┴───────────────────────────────────────────────┴────────────────────┴────────────────────┘

上述可以看到没有发现NotFound字样，说明在两个版本中都存在，只是默认值发生了变化。