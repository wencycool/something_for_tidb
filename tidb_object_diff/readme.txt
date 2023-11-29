
包含对
表的对比(不包含字段，字段由sync_diff_inspector对比表结构时进行对比),主要对比table_schema,table_name,table_type,auto_increment,tidb_pk_type这几列
索引对比
Sequence对比
约束对比
用户对比（包含系统在内的所有用户）
参数对比（重点的show config参数和show variable参数），参数过滤放在Variable类中的variable_filters和config_filters变量中，可按需删减

对比结果只输出有差异的部分，+表示存在，只看+部分即可，比如：
[TABLE]                                                                                               source    target  difference
tpch10.nation                                                                                           +         -         -

这个输出结果中source中包含+，说明在上游中包含该表，下游不包含该表。difference表示上下游都存在该对象，但是内容不一样，下面举一个用户对比的例子：
[USER]                                                                                                source    target  difference
'test'@'%'                                                                                              +         -         -
'root'@'%'                                                                                              -         -         +

可以看到'test'@'%'用户在上游存在（source中包含+）下游不存在，'root'@'%'用户在上下游都存在但是存在差异（可能是密码不一致或者权限不一致），需要人工进行排查。



帮助：
PS E:\PythonProjects\something_for_tidb\tidb_object_diff> python main.py -h
usage: main.py [-h] {check,dump-seq} ...

ticdc检查工具

options:
  -h, --help        show this help message and exit

Subcommands:
  {check,dump-seq}
    check           表结构对比检查
    dump-seq        导出sequence
PS E:\PythonProjects\something_for_tidb\tidb_object_diff> python main.py check -h
usage: main.py check [-h] --src-host SRC_HOST --tgt-host TGT_HOST [--tgt-port TGT_PORT] [--src-port SRC_PORT] [--user USER] [--password [PASSWORD]] [--schema-list SCHEMA_LIST]

options:
  -h, --help            show this help message and exit
  --src-host SRC_HOST   上游IP地址
  --tgt-host TGT_HOST   下游IP地址
  --tgt-port TGT_PORT   端口号,默认4000
  --src-port SRC_PORT   端口号,默认4000
  --user USER, -u USER  用户名
  --password [PASSWORD], -p [PASSWORD]
                        密码
  --schema-list SCHEMA_LIST, -s SCHEMA_LIST
                        schema列表，指定多个用分隔符隔开，比如：db1,db2,db3，默认包含所有schema
PS E:\PythonProjects\something_for_tidb\tidb_object_diff> python main.py dump-seq -h
usage: main.py dump-seq [-h] -H HOST [-P PORT] [-u USER] [-p [PASSWORD]]

options:
  -h, --help            show this help message and exit
  -H HOST, --host HOST  IP地址
  -P PORT, --port PORT  端口号
  -u USER, --user USER  用户名
  -p [PASSWORD], --password [PASSWORD]
                        密码


输出示例：
PS E:\PythonProjects\something_for_tidb\tidb_object_diff> python main.py check --src-host="192.168.31.201" --src-port=4000 --tgt-host="192.168.31.201" --tgt-port=4001 --user root -p -s "tpch10"
Enter your password:
2023-11-29 22:22:01 - INFO - schema列表为:['tpch10']
2023-11-29 22:22:01 - INFO - 检查表情况，
[TABLE]                                                                                               source    target  difference
tpch10.nation                                                                                           +         -         -
tpch10.region                                                                                           +         -         -
tpch10.part                                                                                             +         -         -
tpch10.supplier                                                                                         +         -         -
tpch10.partsupp                                                                                         +         -         -
tpch10.customer                                                                                         +         -         -
tpch10.orders                                                                                           +         -         -
tpch10.lineitem                                                                                         +         -         -
tpch10.orders_bak                                                                                       +         -         -
2023-11-29 22:22:01 - INFO - 查看索引差异
[INDEX]                                                                                               source    target  difference
tpch10.customer.PRIMARY                                                                                 +         -         -
tpch10.lineitem.PRIMARY                                                                                 +         -         -
tpch10.nation.PRIMARY                                                                                   +         -         -
tpch10.orders.PRIMARY                                                                                   +         -         -
tpch10.orders_bak.PRIMARY                                                                               +         -         -
tpch10.part.PRIMARY                                                                                     +         -         -
tpch10.partsupp.PRIMARY                                                                                 +         -         -
tpch10.region.PRIMARY                                                                                   +         -         -
tpch10.supplier.PRIMARY                                                                                 +         -         -
2023-11-29 22:22:01 - INFO - 查看Sequence差异
[SEQUENCE]                                                                                            source    target  difference
2023-11-29 22:22:01 - INFO - 查看约束差异
[CONSTRAINTS]                                                                                         source    target  difference
tpch10.nation.PRIMARY                                                                                   +         -         -
tpch10.region.PRIMARY                                                                                   +         -         -
tpch10.part.PRIMARY                                                                                     +         -         -
tpch10.supplier.PRIMARY                                                                                 +         -         -
tpch10.partsupp.PRIMARY                                                                                 +         -         -
tpch10.customer.PRIMARY                                                                                 +         -         -
tpch10.orders.PRIMARY                                                                                   +         -         -
tpch10.lineitem.PRIMARY                                                                                 +         -         -
tpch10.orders_bak.PRIMARY                                                                               +         -         -
2023-11-29 22:22:01 - INFO - 查看用户差异
[USER]                                                                                                source    target  difference
'test'@'%'                                                                                              +         -         -
'root'@'%'                                                                                              -         -         +
2023-11-29 22:22:01 - INFO - 查看重点参数差异
[Variable]                                                                                            source    target  difference
variable.tidb_analyze_skip_column_types                                                                 +         -         -

# 导出sequence（在原来的基础上+10000）
PS E:\PythonProjects\something_for_tidb\tidb_object_diff> python main.py dump-seq -H 192.168.31.201 -p
Enter your password:
drop sequence if exists `test`.`seq2`;create sequence `test`.`seq2` start with 10018 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle comment='';
drop sequence if exists `test`.`seq1`;create sequence `test`.`seq1` start with 20020 minvalue 1 maxvalue 9223372036854775806 increment by 1 nocache cycle comment='test';
drop sequence if exists `test`.`sequence1`;create sequence `test`.`sequence1` start with 20016 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1 nocycle comment='';
