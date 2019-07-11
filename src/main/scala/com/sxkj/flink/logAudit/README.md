# Log_audit



## To ALL 

 * 2019年05月10日 `release v1.0` 发布。
 


## start


```bash
#正式
cd /opt/flink/flink && sudo -u hdfs bin/flink run -d -m yarn-cluster -ynm kafka_LogAuditFlow -c com.wedata.stream.app.StartServer  /opt/Apps/log-audit/modules/log_audit-4.0.jar

#测试
cd /opt/flink/flink && sudo -u hdfs bin/flink run -d -m yarn-cluster -ynm kafka_LogAuditFlow -c com.wedata.stream.app.StartServer  /opt/Apps/log-audit/modules/log_audit-4.0.jar
```


## Databases 


```sql
--- 日志审计的基础信息表
drop table aud.log_audit_base_info;
create external table  aud.log_audit_base(
applicationId String,
queue String,
submitTime String,
startTime String,
finishTime String,
finalStatus String,
memorySeconds String,
vcoreSeconds String,
applicationType String
)
partitioned by (dt string) 
row format delimited FIELDS TERMINATED by '^'
LINES TERMINATED BY '\n'
stored as textfile location 'hdfs:///home/log_audit_data/log_audit_base';

alter table aud.log_audit_base set location 'hdfs:///apps/hive_external/log_audit/log_audit_base/'

ALTER TABLE aud.log_audit_base ADD IF NOT EXISTS PARTITION (dt='2019-04-25') LOCATION '/apps/hive_external/log_audit/log_audit_base/dt=2019-04-25'



--- 日志审计项目的SQL补全表
drop table aud.log_audit_supply_info2;
create external table aud.log_audit_supply(
applicationId String,
sql_info String
)partitioned by (dt string) 
row format delimited FIELDS TERMINATED by '^'
LINES TERMINATED BY '\n'
stored as textfile location 'hdfs:///home/log_audit_data/log_audit_supply';

select * from aud.log_audit_supply_info2;

alter table aud.log_audit_supply set location 'hdfs:///apps/hive_external/log_audit/log_audit_supply/'

ALTER TABLE aud.log_audit_supply ADD IF NOT EXISTS PARTITION (dt='2019-04-25') LOCATION '/apps/hive_external/log_audit/log_audit_supply/dt=2019-04-25'

---Sloth项目日志
create external table aud.log_audit_sloth_error(
task_name String,
error_detail String,
dt String,
error_type String
)
row format delimited FIELDS TERMINATED by '^'
stored as textfile
location 'hdfs:///apps/hive_external/log_audit/log_audit_sloth_error';

select * from aud.log_audit_sloth_error where dt='2019-04-25'


---用户行为记录表
drop table aud.log_audit_system_message;
create external table  aud.log_audit_system_message(
system String,
username String,
user_ip String,
work_dir String, 
start_time String,
host String,
tag String,
bash String
)
partitioned by (dt string) 
row format delimited FIELDS TERMINATED by '^'
LINES TERMINATED BY '\n'
stored as textfile location 'hdfs:///apps/hive_external/log_audit/log_audit_system';


---系统,用户名,登陆IP,所在目录,执行时间,节点,执行所在栈,命令

alter table aud.log_audit_system_message set location 'hdfs:///apps/hive_external/log_audit/log_audit_system/'

ALTER TABLE aud.log_audit_system_message ADD IF NOT EXISTS PARTITION (dt='2019-04-25') LOCATION '/apps/hive_external/log_audit/log_audit_system/dt=2019-04-25'

---impala日志统计表
drop table aud.log_audit_impala_log;
create external table  aud.log_audit_impala_log(
queryId String,
queryType String,
sqlStatement String,
queryState String,
useDatabase String, 
sqlUser String,
startTime String,
durationMillis String,
memoryAggregatePeak String,
queryStatus String,
isOom String,
networdAddress String,
sessionType String,
connectedUser String,
hostId String
)
partitioned by (dt string) 
row format delimited FIELDS TERMINATED by '^^'
LINES TERMINATED BY '\n'
stored as textfile location 'hdfs:///apps/hive_external/log_audit/log_audit_impala';
#更改分割符为^
alter table aud.log_audit_impala_log set SERDEPROPERTIES('field.delim'='^')

ALTER TABLE aud.log_audit_impala_log ADD IF NOT EXISTS PARTITION (dt='2019-05-09') LOCATION '/apps/hive_external/log_audit/log_audit_impala/dt=2019-05-09'


```



## log_audit_sloth-error

```bash
抓取错误日志描述
sloth日志在7/8/9三台机器上,聚集到7上再做处理
抓取sloth错误日志采用shell脚本处理,每天早上八点十分定时采集,然后处理为hive的字段

#!/bin/bash
current_day=`date "+%Y-%m-%d"`" 05:00:00"
file_list=`grep -nE 'ERROR|Error ' /data/disk13/logs/dispatch/logs/sh/*|awk -F: '{print$1}'|uniq`
mkdir /data/disk13/logs/dispatch/auto_error_logs/`date "+%Y-%m-%d"`
for file in $file_list
do
        file_timestamp=`stat -c %Y $file`
                current_day_timestamp=`date +%s -d "$current_day"`
        if [ "$file_timestamp" -gt "$current_day_timestamp" ];then
                cp $file /data/disk13/logs/dispatch/auto_error_logs/`date "+%Y-%m-%d"`
        else
                continue
        fi
done

处理为hive的字段
#!/bin/bash
current_day=`date "+%Y-%m-%d"`
error_type="retry_error"
for file in /data/disk13/logs/dispatch/auto_error_logs/$current_day/*
do
        file_error=`grep -nE 'ERROR|Error ' $file |sed ':a ; N;s/\n/ / ; t a ; '`
        file_name=`echo "$file"|perl -F/ -wane 'print $F[7]'`
        echo "$file_name^$file_error^$current_day^$error_type" >> /data/disk13/logs/dispatch/drawn_error2hive/error_$current_day
done
sleep 5m
beeline -u "jdbc:hive2://10.50.40.7:10000" -n bigdata.hive -p asdasd110.0 --showHeader=true --outputformat=csv -e "load data local inpath '/data/disk13/logs/dispatch/drawn_error2hive/error_$current_day' into table aud.log_audit_sloth_error;"
```



文件结构

http://172.17.0.60/bigdata-rt/data-flow-panther/blob/log-audit-d1/src/main/scala/com/sxkj/flink/logAudit/sloth-error-result.png



## kafkaTopic

```bash
#topic
log_audit_base_re #Resource抓Job执行日志
log_audit_supply  #Hdfs 采集到执行的SQL 
log_audit_system #采集集群上的用户行为日志
log_audit_impala #impala日志
log_audit_hiveserver2 #hiveserver2
```

## consumerGroup
```bash
log_audit_consumer #base
log_audit_supply_consumer #supply
log_audit_system_consumer #system
log_audit_impala_consumer #impala
log_audit_hiveserver2_consumer #hiveserver2

```

## hdfsPath
```bash
hdfs:///apps/hive_external/log_audit/
```


## flink-on-yarn使用流程


1.安装客户端：下载flink的安装包，将安装包上传到要安装JobManager的节点

2.进入Linux系统对安装包进行解压：解压后在节点上配置

3.修改安装目录下conf文件夹内的flink-conf.yaml配置文件，指定JobManager：
 [user@cdh4 conf]# vim flink-conf.yaml
 jobmanager.rpc.address:host
 
4.修改安装目录下conf文件夹内的slave配置文件，指定TaskManager：
  ```bash
  [user@cdh4 conf]# vim slaves
   slavehost
  ```
 
5.将配置好的Flink目录分发给其他的flink节点

6.明确虚拟机中已经设置好了环境变量HADOOP_HOME，启动了hdfs和yarn

7.在cdh某节点提交Yarn-Session，使用安装目录下bin目录中的yarn-session.sh脚本进行提交：
```bash
 ./bin/yarn-session.sh -n 1 -s 2 -jm 2048 -tm 2048 -nm flink_online -d
 ./bin/yarn-session.sh -n 1 -s 2 -jm 2048 -tm 2048 -nm flink_online -d
 ```
 
    其中：
     -n(--container)：TaskManager的数量。
     -s(--slots)： 每个TaskManager的slot数量，默认一个slot一个core，默认每个taskmanager的slot的个数为1，有时可以多一些taskmanager，做冗余。
     -jm：JobManager的内存（单位MB)。
     -tm：每个taskmanager的内存（单位MB)。
     -nm：yarn 的appName(现在yarn的ui上的名字)。 
     -d：后台执行。
    启动后查看Yarn的Web页面，可以看到刚才提交的会话

8.提交Jar到集群运行：
```bash

[user@cdh4 flink] cd /opt/log-audit-conn/flink/flink-1.7.2 && 
[user@cdh4 flink] sudo -u hdfs bin/flink run -m yarn-cluster -ynm yarnName -c className  jarPath
# 下面是实际使用命令
[user@cdh4 flink]cd /opt/log-audit-conn/flink/flink-1.7.2 && sudo -u hdfs bin/flink run -m yarn-cluster -ynm simple_kafka_LogAuditFlowSupply -c com.wedata.stream.app.LogAuditFlowSupply  /home/log_audit-1.0-SNAPSHOT-jar-with-dependencies.jar
```
查看结果：
在yarn的resourcemanager的web页面上可以看到正在running的任务，点击右侧的applicationMaster进入flink的web页面可以查看到flink任务执行情况



## TODO

* 支持自定义组建扫描日志

* 支持特殊操作告警

* 代码规范