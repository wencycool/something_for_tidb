"""
查找重复索引，重复索引分为两种情况：
1. 顺序相同的情况下，称为一定重复索引，一个索引被另一个索引的最左前缀覆盖，例如：a,b,c和a,b,c,d
2. 顺序不同的情况下，称为疑似重复索引，一个索引被另一个索引的最左前缀覆盖，例如：a,b,c和b,a,c,d
"""
from typing import List
import pymysql

# 一个索引可以被标记为三种状态：未标记、一定重复索引、疑似重复索引
CONST_UNMARKED = "UNDUPLICATE_INDEX"  # 未标记
CONST_DUPLICATE_INDEX = "DUPLICATE_INDEX"  # 一定重复索引
CONST_SUSPECTED_DUPLICATE_INDEX = "SUSPECTED_DUPLICATE_INDEX"  # 疑似重复索引


class Index:
    """
    定义索引类
    该类包含索引的状态、表结构、表名、索引名、索引列
    """

    def __init__(self):
        self.state = CONST_UNMARKED
        self.table_schema = ""
        self.table_name = ""
        self.index_name = ""
        self.columns: List[str] = []  # 索引列，索引列的顺序即为索引的顺序，NOTE：这里不包括字段的排序方式，即ASC/DESC
        self.covered_by: Index = None  # 这里保存的是随机找到的一个覆盖该索引的索引，NOTE：不查找root索引的原因是root索引是动态的，比如这里认定的root索引，当”被认为的root
        # 索引“还有root索引时则这里的判断是错误的，为避免复杂度，这里简化处理。什么是root索引：比如当前索引A被索引B覆盖，B被C覆盖，那么A的root索引是C

    def __str__(self):
        return f"schema:{self.table_schema},table:{self.table_name},index:{self.index_name},columns:{self.columns}"

    def is_covered_by(self, index):
        """
         判断该类是否被另一个索引的最左前缀覆盖,如果覆盖则对当前类进行标记
        :param index:
        :type index: Index
        :return: 如果当前类被另一个索引的最左前缀覆盖则返回True，否则返回False
        :rtype: index
        """
        # 判断当前索引是否一定被另一个索引的最左前缀覆盖
        # NOTE: is是判断引用是否相等，==是判断值是否相等
        if len(self.columns) <= len(index.columns) and self.columns == index.columns[:len(self.columns)]:
            self.state = CONST_DUPLICATE_INDEX
            self.covered_by = index
            return True
        # 判断当前索引是否疑似被另一个索引的最左前缀覆盖
        if len(self.columns) <= len(index.columns) and sorted(list(set(self.columns))) == sorted(
                list(set(index.columns[:len(self.columns)]))):
            self.state = CONST_SUSPECTED_DUPLICATE_INDEX
            self.covered_by = index
            return True
        return False


class TableIndex:
    """
    定义一张表的索引类
    该类包含表的索引信息
    """

    def __init__(self):
        self.table_schema = ""
        self.table_name = ""
        self.indexes: List[Index] = []

    def is_index_count_exceed(self, max_index_count=5):
        """
        判断表上的索引个数是否超过max_index_count个
        :param max_index_count: 最大索引个数
        :type max_index_count: int
        :return: 如果表上的索引个数超过max_index_count个则返回True，否则返回False
        :rtype: bool
        """
        return len(self.indexes) > 5

    def get_duplicate_index_count(self):
        """
        获取表上一定重复索引个数
        :rtype: int
        """
        return len([index for index in self.indexes if index.state == CONST_DUPLICATE_INDEX])

    def get_suspected_duplicate_index_count(self):
        """
        获取表上疑似重复索引个数
        :rtype: int
        """
        return len([index for index in self.indexes if index.state == CONST_SUSPECTED_DUPLICATE_INDEX])

    def get_index_columns_count_exceed(self, max_columns_count=5):
        """
        获取表上索引字段超过max_columns_count个的索引个数
        :param max_columns_count: 最大索引字段个数
        :type max_columns_count: int
        :rtype: int
        """
        return len([index for index in self.indexes if len(index.columns) > max_columns_count])

    def analyze_indexes(self):
        """
        分析表上的索引，标记重复索引
        """
        for cursor in range(len(self.indexes)):
            for next_cursor in range(cursor + 1, len(self.indexes)):
                if self.indexes[cursor].is_covered_by(self.indexes[next_cursor]):
                    break


# 获取索引信息
def get_tableindexes(connect):
    """
    获取表上的索引信息
    :param connect: 数据库连接
    :type connect: pymysql.connections.Connection
    :rtype: List[TableIndex]
    """
    # 按照column_name分组聚合并按照seq_in_index升序组成字段列表，然后按照table_schema,table_name,len(column_names)升序，这样一来同一个表上的索引字段个数最少的排在前面，向下匹配来判断冗余索引即可
    get_index_sql = """select table_schema,table_name,key_name,group_concat(column_name order by seq_in_index 
    separator ',') as column_names from information_schema.tidb_indexes  where table_schema not in ('mysql',
    'INFORMATION_SCHEMA','PERFORMANCE_SCHEMA') group by table_schema,table_name,key_name order by table_schema,
    table_name,length(column_names) asc"""

    # 设置游标返回的数据类型为字典
    cursor = connect.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(get_index_sql)
    table_indexes: List[TableIndex] = []
    last_table = []
    for each_row in cursor:
        table_name = each_row["table_name"]
        table_schema = each_row["table_schema"]
        key_name = each_row["key_name"]
        column_names = each_row["column_names"].split(",")
        # 初始化索引类
        index = Index()
        index.table_schema = table_schema
        index.table_name = table_name
        index.index_name = key_name
        index.columns = column_names
        if last_table != [table_schema, table_name]:
            table_index = TableIndex()
            table_indexes.append(table_index)
            table_index.table_schema = table_schema
            table_index.table_name = table_name
        table_index.indexes.append(index)
        last_table = [table_schema, table_name]
    cursor.close()
    return table_indexes


if __name__ == "__main__":
    # 连接数据库
    connect = pymysql.connect(host='192.168.31.201', port=4000, user='root', password='123', charset='utf8mb4')
    # 获取表上的索引信息
    table_indexes = get_tableindexes(connect)
    # 遍历表上的索引信息
    for table_index in table_indexes:
        table_index.analyze_indexes()
        # 查看表上索引相关信息是否符合要求
        if table_index.is_index_count_exceed():
            print(f"schema:{table_index.table_schema},table:{table_index.table_name} has too many indexes")
        if table_index.get_duplicate_index_count() > 0:
            print(f"schema:{table_index.table_schema},table:{table_index.table_name} has duplicate indexes")
        if table_index.get_suspected_duplicate_index_count() > 0:
            print(f"schema:{table_index.table_schema},table:{table_index.table_name} has suspected duplicate indexes")
        if table_index.get_index_columns_count_exceed():
            print(f"schema:{table_index.table_schema},table:{table_index.table_name} has too many index columns")
        # 打印表上的索引信息
        for index in table_index.indexes:
            if index.state == CONST_DUPLICATE_INDEX:
                print(
                    f"schema:{index.table_schema},table:{index.table_name},index:{index.index_name} is duplicate index,covered by index:{index.covered_by.index_name}")
            elif index.state == CONST_SUSPECTED_DUPLICATE_INDEX:
                print(
                    f"schema:{index.table_schema},table:{index.table_name},index:{index.index_name} is suspected duplicate index,could be covered by index:{index.covered_by.index_name}")
