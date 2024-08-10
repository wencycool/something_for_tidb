use test;
create table t1 (a int, b int, c int, d int,e int,f int);
# 模拟冗余索引
# case1: 两个索引字段顺序相同，字段相同
alter table t1 add index case1_idx1(a,b);
alter table t1 add index case1_idx2(a,b);
# case2: 两个索引字段顺序相同，字段存在最左前缀包含关系
alter table t1 add index case2_idx1(a,c);
alter table t1 add index case2_idx2(a,c,b);
alter table t1 add index case2_idx3(a,c,b,d,e);
# case3: 两个索引字段顺序不同，字段相同
alter table t1 add index case3_idx1(b,c);
alter table t1 add index case3_idx2(c,b);
# case4: 两个索引字段顺序不同，字段存在最左前缀包含关系
alter table t1 add index case4_idx1(b,a,c);
alter table t1 add index case4_idx2(b,a,c,d,e);
alter table t1 add index case4_idx3(c,b,a,d,e);
# case5: 索引字段个数超过5个
alter table t1 add index case5_idx1(a,b,c,d,e,f);


