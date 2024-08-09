# 索引去重检查
## 规则
### 获取索引信息
```sql
select table_schema,table_name,index_name,key_name,group_concat(column_name order by seq_in_index asc separator ',') as column_names from information_schema.tidb_indexes group by table_schema,table_name,key_name order by table_schema,table_name,key_name,column_names;
```
### 比较索引
- 一定冗余索引
  - 两个索引包含的列，字段顺序完全相同
  - 两个索引，一个索引是另一个索引的前缀索引
- 疑似冗余索引
  - 两个索引包含的列，字段完全相同，但字段顺序不同
  - 两个索引包含的列，A索引的前缀字段和B索引的所有字段完全相同，但A索引的前缀字段顺序和B索引的字段顺序不同
### 输出冗余索引
