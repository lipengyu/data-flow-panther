package com.sxkj.utils

import com.sxkj.sink.Sink2Hdfs
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
  * @Author:zhengxiaofei
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description 日志审计涉及到的流的过程
  */
object StreamUtil {
  /**
    * @Description 处理base的数据
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:55
    * @param env
    * @return_type 默认
    * @updateInfo
    */
  def baseStream(env:StreamExecutionEnvironment): Unit ={

    //连接kafka
    val baseConsumer = PropertiesUtils.loadLogAuditProperties("baseConsumer")
    val baseTopic = PropertiesUtils.loadLogAuditProperties("baseTopic")
    val kafkaConsumerBase = SourceFromKafkaUtil.fromTopic(baseConsumer,baseTopic)
    //source
    val transacBase: DataStream[String] = env.addSource(kafkaConsumerBase).setParallelism(PropertiesUtils.loadLogAuditProperties("sourceParallelism").toInt).name("Source Kafka base")
    //转换
    val csvLine = TransformationUtils.transBase(transacBase)
    //sink2hdfs
    val sink2hdfsBase = Sink2Hdfs.toHdfsPath(PropertiesUtils.loadLogAuditProperties("hdfsPathBase"))
    csvLine.addSink(sink2hdfsBase).setParallelism(PropertiesUtils.loadLogAuditProperties("sinkParallelism").toInt).name("base Sink To HDFS")
  }
  /**
    * @Description 处理supply的数据
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:56
    * @param env
    * @return_type 默认
    * @updateInfo
    */
  def supplyStream(env:StreamExecutionEnvironment): Unit ={
    //连接kafka
    val supplyTopic = PropertiesUtils.loadLogAuditProperties("supplyTopic")
    val supplyConsumer = PropertiesUtils.loadLogAuditProperties("supplyConsumer")
    val kafkaConsumerSupply = SourceFromKafkaUtil.fromTopic(supplyConsumer,supplyTopic)
    //source
    val transacSupply = env.addSource(kafkaConsumerSupply).setParallelism(PropertiesUtils.loadLogAuditProperties("sourceParallelism").toInt).name("Source Kafka supply")
    //转换
    val dataStreamSink = TransformationUtils.transSystem(transacSupply)
    //sink2hdfs
    val sink2hdfsSupply = Sink2Hdfs.toHdfsPath(PropertiesUtils.loadLogAuditProperties("hdfsPathSupply"))
    dataStreamSink.addSink(sink2hdfsSupply).setParallelism(PropertiesUtils.loadLogAuditProperties("sinkParallelism").toInt).name("supply Sink To HDFS")

  }
  /**
    * @Description 处理system的数据
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:56
    * @param env
    * @return_type 默认
    * @updateInfo
    */
  def systemStream(env:StreamExecutionEnvironment): Unit ={

    //连接kafka
    val systemTopic = PropertiesUtils.loadLogAuditProperties("systemTopic")
    val systemConsumer = PropertiesUtils.loadLogAuditProperties("systemConsumer")
    val kafkaConsumerSystem = SourceFromKafkaUtil.fromTopic(systemConsumer,systemTopic)
    //source
    val transacSystem = env.addSource(kafkaConsumerSystem).setParallelism(PropertiesUtils.loadLogAuditProperties("sourceParallelism").toInt).name("Source Kafka system")
    //转换
    val dataStreamSink = TransformationUtils.transSystem(transacSystem)
    //sink2hdfs
    val sink2hdfsSystem = Sink2Hdfs.toHdfsPath(PropertiesUtils.loadLogAuditProperties("hdfsPathSystem"))
    dataStreamSink.addSink(sink2hdfsSystem).setParallelism(PropertiesUtils.loadLogAuditProperties("sinkParallelism").toInt).name("system Sink To HDFS")

  }
  /**
    * @Description 处理impala日志
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:56
    * @param env
    * @return_type 默认
    * @updateInfo
    */
  def impalaStream(env:StreamExecutionEnvironment):Unit={
    //连接kafka
    val impalaTopic = PropertiesUtils.loadLogAuditProperties("impalaTopic")
    val impalaConsumer = PropertiesUtils.loadLogAuditProperties("impalaConsumer")
    val kafkaConsumerImpala = SourceFromKafkaUtil.fromTopic(impalaConsumer,impalaTopic)
    //source
    val transacImpala = env.addSource(kafkaConsumerImpala).setParallelism(PropertiesUtils.loadLogAuditProperties("sourceParallelism").toInt).name("Source Kafka impala")
    //转换
    val dataStreamSink = TransformationUtils.transImpala(transacImpala)
    //sink2hdfs
    val sink2hdfsImpala = Sink2Hdfs.toHdfsPath(PropertiesUtils.loadLogAuditProperties("hdfsPathImpala"))
    dataStreamSink.addSink(sink2hdfsImpala).setParallelism(PropertiesUtils.loadLogAuditProperties("sinkParallelism").toInt).name("impala Sink To HDFS")

  }
  /**
    * @Description 处理hiveserver2日志
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:57
    * @param env
    * @return_type 默认
    * @updateInfo
    */
  def hiveStream(env:StreamExecutionEnvironment):Unit={
    //连接kafka
    val hiveTopic = PropertiesUtils.loadLogAuditProperties("hiveTopic")
    val hiveConsumer = PropertiesUtils.loadLogAuditProperties("hiveConsumer")
    val kafkaConsumerHive = SourceFromKafkaUtil.fromTopic(hiveConsumer,hiveTopic)
    //source
    val transacHive = env.addSource(kafkaConsumerHive).setParallelism(PropertiesUtils.loadLogAuditProperties("sourceParallelism").toInt).name("Source Kafka hiveserver2")
    //转换
    val dataStreamSink = TransformationUtils.transHiveServer2(transacHive)
    //sink2hdfs
    val sink2hdfsHive = Sink2Hdfs.toHdfsPath(PropertiesUtils.loadLogAuditProperties("hdfsPathhiveserver2"))
    dataStreamSink.addSink(sink2hdfsHive).setParallelism(PropertiesUtils.loadLogAuditProperties("sinkParallelism").toInt).name("hiveserver2 Sink To HDFS")

  }
}
