## 文件目录

```
├── README.md
├── data-flow-panther
│     └── src/main
│            └── resources  (相关配置文件)
│                  ├── *.yml
│	               ├── *.properties
│	               └── *.conf
│            └── scala
│                  └── com/sxkj  （根目录）
│                        ├── test  （测试代码）
│                        ├── utils  （工具类）
│                        ├── pool  （连接类：mysql,redis,hbase,hive,hdfs）
│                        └── flink  （业务代码主类）
│                        	  ├── bus1 （业务名称1）
│                        	  └── bus2 （业务名称2）
│     └── ... （其他项目）
```
