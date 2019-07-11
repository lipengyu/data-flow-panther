package com.sxkj.utils

import org.apache.flink.streaming.api.scala.DataStream

import scala.util.matching.Regex
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory
/**
  * @Author:zhengxiaofei
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description 日志审计涉及到的流的转换
  */
object TransformationUtils {
  /**
    * @Description 转换收到的base的数据
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:57
    * @param transac
    * @return_type DataStream[String]
    * @updateInfo 
    */
  // 定义全局变量
  val logger = LoggerFactory.getLogger(this.getClass)
  def transBase(transac: DataStream[String]) ={
    val className = "org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary"
    //满足类匹配进行正则匹配和转换操作
    //val filterValue = FilterDataStream.getFilterStream(transac,className)
    val filterValue = transac.filter(x => x.contains(className))
    val csvLine= filterValue.map(line => {
    //正则匹配规则
    val pattern = new Regex("appId=(.+?),.*?,queue=(.+?),.*?,submitTime=(.+?),*,startTime=(.+?),*,finishTime=(.+?),*,finalStatus=(.+?),*,memorySeconds=(.+?),*,vcoreSeconds=(.+?),.*?,applicationType=(.+?),.*?",line)
    var temp_Line = "";
      for (m <- pattern.findAllIn(line).matchData; e <- m.subgroups) {
        temp_Line += e + "^"
      }
      temp_Line=temp_Line.substring(0, temp_Line.length - 1)

    //temp_Line += "\n"
      temp_Line

    }).setParallelism(PropertiesUtils.loadLogAuditProperties("mapParallelism").toInt).name("base Filter Map")
    csvLine
  }
  /**
    * @Description 转换接收到的系统操作数据
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:58
    * @param tranSysten
    * @return_type DataStream[String]
    * @updateInfo 
    */
  def transSystem(tranSysten:DataStream[String])={
    //去掉登陆 登出
    val filterValue = tranSysten.filter(x => x.contains("audit") && !x.contains("session closed")&& !x.contains("session opened")).map(r=> r.split(": "))

    var TimeLine = new Array[String](0)
    var ActionLine = new Array[String](0)
    var Bash = "";
    val dataStreamSink = filterValue.map(line => {
        var dataLine = ""
        try {
          if (line.length >= 3) {
            /**
              **/
            TimeLine = line(0).split(" ")
            ActionLine = line(1).replace("[", "").split(" ")
            Bash = line(2)
          }
          //      var Title = "系统,用户名,登陆IP,所在目录,执行时间,节点,执行所在栈,命令"
          if(TimeLine.length>=3&&ActionLine.length>=7&&ActionLine(1).length>=1&&Bash.length>=1){
            dataLine = s"${ActionLine(0)}^${ActionLine(1).split("/")(0)}^${ActionLine(5).split("/")(2).split(":")(0)}^${ActionLine(6)}^${TimeLine(0)}^${TimeLine(1)}^${TimeLine(2)}^$Bash"
          }

        }catch{
          case ex:Exception=>{
            logger.error(ex.getMessage + ":\t" + line)
          }
        }
        dataLine
      }).setParallelism(PropertiesUtils.loadLogAuditProperties("mapParallelism").toInt).name("system Filter Map")
      dataStreamSink
  }

  /**
    * @Description 转换supply的日志数据
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:59
    * @param stranSup
    * @return_type DataStream[String]
    * @updateInfo 
    */
  def  transSupply(tranSup:DataStream[String]) ={
    val filterValue = tranSup.filter(x => x.contains("^"))
    //log里面第一行的类名
    val supLine = filterValue.map(line => {
      line
    }).setParallelism(PropertiesUtils.loadLogAuditProperties("mapParallelism").toInt).name("supply Filter Map")
    supLine
  }

  /**
    * @Description 转换接收到的impala的日志信息
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:59
    * @param tranImala
    * @return_type DataStream[String]
    * @updateInfo 
    */
  def transImpala(tranImala:DataStream[String]) ={
    val filterValue = tranImala.filter(x => x.contains("^"))
    val msgLine = filterValue.map(line => {
      val str = line.substring(2, line.length - 1)
      str
    }).setParallelism(PropertiesUtils.loadLogAuditProperties("mapParallelism").toInt).name("impala Filter Map")
    msgLine
  }

  /**
    * @Description 转换接收到的hiveserver2的日志数据
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 16:00
    * @param tranHive
    * @return_type DataStream[String]
    * @updateInfo
    */
  def  transHiveServer2(tranHive:DataStream[String]) ={
    val filterValue = tranHive.filter(x => x.contains("^"))
    //log里面第一行的类名
    val hiveLine = filterValue.map(line => {
      val str = line.substring(2, line.length - 1)
      str
    }).setParallelism(PropertiesUtils.loadLogAuditProperties("mapParallelism").toInt).name("hiveserver2 Filter Map")
    hiveLine
  }
}
