set @@tidb_metric_query_step = 3600;
set @@tidb_metric_query_range_duration = 30;
WITH ip_node_map as (SELECT ip_address,
                            GROUP_CONCAT(CONCAT(TYPE, '(', TYPE_COUNT, ')') ORDER BY TYPE) AS TYPES_COUNT
                     FROM (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1) AS ip_address,
                                  TYPE,
                                  COUNT(*)                          AS TYPE_COUNT
                           FROM INFORMATION_SCHEMA.CLUSTER_INFO
                           GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1), TYPE) node_type_count
                     GROUP BY ip_address),
     os_info AS (SELECT SUBSTRING_INDEX(INSTANCE, ':', 1)                         AS IP_ADDRESS,
                        MAX(CASE WHEN NAME = 'cpu-physical-cores' THEN VALUE END) AS CPU_CORES,
                        MAX(CASE WHEN NAME = 'capacity' THEN VALUE END)           AS MEMORY_CAPACITY,
                        MAX(CASE WHEN NAME = 'cpu-arch' THEN VALUE END)           AS CPU_ARCH
                 FROM INFORMATION_SCHEMA.CLUSTER_HARDWARE
                 WHERE (DEVICE_TYPE = 'cpu' AND DEVICE_NAME = 'cpu' AND NAME IN ('cpu-arch', 'cpu-physical-cores'))
                    OR (DEVICE_TYPE = 'memory' AND DEVICE_NAME = 'memory' AND NAME = 'capacity')
                 GROUP BY SUBSTRING_INDEX(INSTANCE, ':', 1)),
     ip_hostname_map as (select substring_index(instance, ':', 1) as ip_address,
                                value                             as hostname
                         from INFORMATION_SCHEMA.CLUSTER_SYSTEMINFO
                         where name = 'kernel.hostname'
                         group by ip_address, hostname)
select a.time,
       a.instance,
       substring_index(a.instance, ':', 1) as ip_address,
       d.hostname,
       c.TYPES_COUNT,
       round(a.value) as active_connection_count,
       b.value as total_connection_count
from METRICS_SCHEMA.tidb_get_token_total_count a
    join METRICS_SCHEMA.tidb_connection_count b on date_format(a.time,'%Y-%m-%d %H:00:00') = date_format(b.time,'%Y-%m-%d %H:00:00') and a.instance = b.instance
         join ip_node_map c on substring_index(a.instance, ':', 1) = c.ip_address
         join ip_hostname_map d on substring_index(a.instance, ':', 1) = d.ip_address
where a.time between date_sub(now(), interval 7 day) and now()
and   b.time between date_sub(now(), interval 7 day) and now();
