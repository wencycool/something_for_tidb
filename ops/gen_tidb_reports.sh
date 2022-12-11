#!/usr/bin/env bash

#每天执行一次脚本，放到crontab中
#部署在tidb-server节点的tidb用户下，且tidb用户需要这是login-path免密认证登录到mysql中
#默认清理15天前的数据，压缩一天前的数据
#00 02 * * * /db/dbawork/tools/gen_tidb_report.sh
#test模式：sh xxx.sh test ,会跳过循环和时间间隔完整执行一次脚本

##################init##################
typeset logoutputfile
#在crontab调用时候无法引用/etc/profile来找到环境变量，因此在执行脚本时候须指定环境变量
. /etc/profile
tidb_profile=`awk -F: '$1=="tidb" {print $(NF-1);exit}' /etc/passwd`/.bash_profile
if [ -f "${tidb_profile}" ]; then
  . ${tidb_profile}
fi
if [ -f /home/tidb/.bash_profile ]; then
    . /home/tidb/.bash_profile
fi
if [ -f /etc/profile ]; then
    . /etc/profile
fi
##################functions##################
function log() {
     echo "`date` $1"  >>$logoutputfile
 }

#将两个路径进行合并
function join_path() {
  typeset result
  typeset first=1
  while [[ $# -ne 0 ]]; do
      if [[ $first -eq 1 ]]; then
        result=$1
        first=0
      else
        result=${result}/${1}
      fi
      #去掉后面反斜杠
      if [[ ${result: -1} == "/" ]]; then
        result=${result%*/}
      fi
      shift
  done
  echo $result
}

function now() {
    echo "`date +"%Y-%m-%d-%H.%M.%S"`"
}

function now_hour() {
    echo "`date +"%Y-%m-%d-%H"`"
}

function now_day() {
    echo "`date +"%Y-%m-%d"`"
}

function mk_dirs() {
    for dir in $*;do
      cmd="mkdir -p $dir"
      result=`$cmd 2>&1`
      if [ "$?" -ne 0 ]; then
        log "$cmd error:$result"
      fi
    done
}

#创建多个目录
function init_dirs() {
  test -d $base_dir ||{
    log "basedir:$base_dir,is not path,[basedir must be /db/dbawork]"
        exit 1
  }
  mk_dirs $tools_dir $tmp_dir $reports_dir $snap_dir $statistics_dir $ddl_dir $cache_dir
}


#避免脚本重复执行，如果当前排队执行的相同shell超过2个则退出，否则等待正在执行的结束直到超时（超时时间为2小时）
function check_wait4lock() {
  shell_name=$0
  shell_count=`ps -ef|grep "$shell_name"|grep -v "grep"|wc -l`
  #shell_count的获取是采用子进程的方式，因此需要额外减1,
  let shell_count=${shell_count}-1
  if [[ ! -f "$lock_file" ]]; then
    echo 1 >$lock_file
    return
  #避免程序被异常终止导致lock_file残留,因此即使存在也要判断同名的脚本是否还在执行
  elif [[ $shell_count -lt 2 ]]; then
    echo 1 >$lock_file
    return
  else
    if [ `cat $lock_file` -lt 2 ]; then
      #开始等待2小时超时
      echo 2 >$lock_file
      log "shell:$shell_name is running,0 in queue,waiting(2hour),pid:$$..."
      #todo 添加等待逻辑
      _i=0
      while [[ "$_i" -lt 120 ]]; do
        if [[ ! -f "$lock_file" ]]; then
          echo 1 >$lock_file
          return
        fi
        let _i=${_i}+1
        sleep 60
      done
      log "wait timeout,exit..."
      exit 1
    else
      log "shell:$shell_name is running, 1 in queue,pid:$$ exit"
      exit 1
    fi
  fi
}

function release_lock() {
  rm -f $lock_file
}

function prune_logoutputfile() {
  #如果日志文件大于1GB则清空
  if [[ -f $logoutputfile && `stat -c "%s" $logoutputfile` -gt 1073741824 ]]; then
    echo "">$logoutputfile
  fi
}


#遇到错误则退出脚本
function check_mysql_env() {
  command -v mysql >/dev/null ||{
    log "command:mysql not found"
    exit 1
  }
  result=`mysql -e "select 1" 2>&1` || {
    log "$result"
    exit 1
  }
}

#执行mysql命令，获取文本结果集
function mysql_cmd() {
  cmd="mysql -e \"$@\""
  result=`bash -c "$cmd" 2>&1`
  recode=$?
  echo "${result}"
  return $recode
}

function mysql_cmd_noheader() {
  cmd="mysql -N -s -e \"$@\""
  result=`bash -c "$cmd" 2>&1`
  recode=$?
  echo "${result}"
  return $recode
}

#获取tidb的stats的url路径，和table_schema,table_name拼接后即可导出stats的json数据
function get_stats_baseurl() {
    baseurl=`ps -ef |awk '$8 ~ /bin\/tidb-server/  {for (i=1;i<=NF;i++){if ($i ~ /^--status=/){port=$i;gsub("--status=","",port);} if ($i ~ /--advertise-address=/){ip=$i;gsub("--advertise-address=","",ip);printf("curl -G http://%s:%s/stats/dump",ip,port);exit;}}}'`
    echo $baseurl
}

##################functions##################

##################variables##################
keep_days=15    #快照保留时间，保留15天前的
gzip_days=1     #快照压缩时间，压缩超过1天前的
interval=60     #每60秒执行一次循环获取数据，一直循环执行
typeset intervals_oneday
let intervals_oneday=3600*24/${interval}
snapshot_filename_head="tidb_`hostname`_snapshot.log"
#todo 这里的base_dir需要改成/db/dbawork
typeset base_dir=/db/dbawork
typeset tools_dir=`join_path ${base_dir} "tools"`
typeset tmp_dir=`join_path ${base_dir} "temp"`
typeset reports_dir=`join_path ${base_dir} "reports"`
typeset snap_dir=`join_path ${reports_dir} "snapshot"`
typeset statistics_dir=`join_path ${reports_dir} "statistics"`
typeset ddl_dir=`join_path ${reports_dir} "ddl"`
typeset cache_dir=`join_path ${base_dir} ".cache"` #临时缓存目录
typeset lock_file=`join_path ${cache_dir} "run.lock"` #避免脚本多次执行，脚本执行开始创建锁文件，结束后释放锁文件

logoutputfile=`join_path $tools_dir $(basename $0)`".log"
#是否开启test模式，在部署脚本时候可以通过test模式来检测脚本是否可正常运行
typeset test_mode=0
##################variables##################


##################main#######################
if [[ "$#" -gt 0 && "$1" == "test" ]]; then
    test_mode=1
    intervals_oneday=1
    interval=1
    echo "In test mode,just wait a moment..."
fi
check_mysql_env
test -d $snap_dir && test -d $statistics_dir && test -d $cache_dir|| {
  init_dirs
}

#避免脚本重复执行
check_wait4lock
#日志文件过大（大于1G）则清空日志文件
prune_logoutputfile
################获取snapshot信息################
loops=0

while [[ "$loops" -lt "$intervals_oneday" ]]; do
  test -d ${snap_dir} || {
    log "snapshot dir:$snap_dir is not a path"
    exit 1
  }
  snapshot_filename="${snapshot_filename_head}_`now_hour`"
  full_snapshot_filename=`join_path $snap_dir $snapshot_filename`
  echo -e "\n\n>>>Record Snapshot Start `now`\n"                                            >>${full_snapshot_filename}
  #PROCESSLIST
  mysql_cmd_noheader "select concat('Record information_schema.processlist:',now())"        >>${full_snapshot_filename}
  mysql_cmd "select * from information_schema.processlist\G"                                >>${full_snapshot_filename}
  echo "">>${full_snapshot_filename}

  #TIDB_TRX
  mysql_cmd_noheader "select concat('Record information_schema.tidb_trx:',now())"           >>${full_snapshot_filename}
  mysql_cmd "select * from information_schema.tidb_trx\G"                                   >>${full_snapshot_filename}
  echo "">>${full_snapshot_filename}
  #DATA_LOCK_WAITS
  mysql_cmd_noheader "select concat('Record information_schema.data_lock_waits:',now())"    >>${full_snapshot_filename}
  mysql_cmd "select * from information_schema.data_lock_waits limit 10000\G"                >>${full_snapshot_filename}
  echo "">>${full_snapshot_filename}
  #DEADLOCKS
  mysql_cmd_noheader "select concat('Record information_schema.deadlocks:',now())"          >>${full_snapshot_filename}
  mysql_cmd "select * from information_schema.deadlocks\G"                                  >>${full_snapshot_filename}
  echo "">>${full_snapshot_filename}
  #ADMIN SHOW DDL
  mysql_cmd_noheader "select concat('Record admin_show_ddl:',now())"                        >>${full_snapshot_filename}
  mysql_cmd "admin show ddl\G"                                                              >>${full_snapshot_filename}
  echo "">>${full_snapshot_filename}
  echo -e "\n<<<Record snapshot end`now`\n"                                                 >>${full_snapshot_filename}

  let loops=${loops}+1
  sleep $interval
done
#做数据清理

#压缩之前的数据1天前数据
log "Begin gzip $snap_dir ..."
log "find $snap_dir -maxdepth 1 -name "${snapshot_filename_head}_*" -type f -mtime +${gzip_days} |grep -Pv \"\.gz$\"|xargs gzip -f"
find $snap_dir -maxdepth 1 -name "${snapshot_filename_head}_*" -type f -mtime +${gzip_days} |grep -Pv "\.gz$"|xargs gzip -f
#清理15天前数据
log "Begin prune $snap_dir ..."
log "find $snap_dir -maxdepth 1 -name \"${snapshot_filename_head}_*\" -type f  -mtime +${keep_days} -exec rm -f {} \;"
find $snap_dir -maxdepth 1 -name "${snapshot_filename_head}_*" -type f  -mtime +${keep_days} -exec rm -f {} \;


################获取统计信息################
#获取表结构和统计信息数据
baseurl=`get_stats_baseurl`
table_list=`mysql_cmd_noheader "select table_schema,table_name from information_schema.tables where table_schema not in ('METRICS_SCHEMA','PERFORMANCE_SCHEMA','INFORMATION_SCHEMA','mysql','test')"`
statistics_filename_head="tidb_`hostname`_stats_"
while read table_schema table_name
do
  statistics_filename="${statistics_filename_head}_${table_schema}_${table_name}.json_`now_day`"
  full_statistics_filename=`join_path $statistics_dir $statistics_filename`
  $baseurl/$table_schema/$table_name 2>/dev/null                                                          >${full_statistics_filename}
  if [[ "$?" -eq 0 ]]; then
      gzip -f ${full_statistics_filename}
  fi
done <<<$table_list
#清理15天前数据
log "Begin prune $statistics_dir ..."
log "find $statistics_dir -maxdepth 1 -name \"${statistics_filename_head}_*.json*\" -type f  -mtime +${keep_days} -exec rm -f {} \;"
find $statistics_dir -maxdepth 1 -name "${statistics_filename_head}_*.json*" -type f  -mtime +${keep_days} -exec rm -f {} \;
release_lock
