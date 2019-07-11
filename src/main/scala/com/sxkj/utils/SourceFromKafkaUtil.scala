package com.sxkj.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
/**
  * @Author:zhengxiaofei
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description kafka作为source给flink端
  */
object SourceFromKafkaUtil {
  /**
    * @Description 连接kafka的topic，消费topic的数据
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:54
    * @param consumer,topic
    * @return_type FlinkKafkaConsumer010[String]
    * @updateInfo
    */
  def fromTopic(consumer:String,topic:String): FlinkKafkaConsumer010[String] ={
    //kafka consumer
    val kafkaProps = new Properties()
    //连接zookeeper
    kafkaProps.setProperty("zookeeper.connect", PropertiesUtils.loadLogAuditProperties("zookeeperHost"))
    //连接kafkaBroker
    kafkaProps.setProperty("bootstrap.servers", PropertiesUtils.loadLogAuditProperties("kafkaBroker"))

    //超时时间
    kafkaProps.setProperty("transaction.timeout.ms",3*24*60*60*1000+"")

    //配置三个kerberos的重要属性
    kafkaProps.setProperty("group.id", consumer)

    val kafkaKerberos = PropertiesUtils.loadLogAuditProperties("kafkaKerberos")
    val securityProtocol = PropertiesUtils.loadLogAuditProperties("securityProtocol")
    ConnectKafkaKerberosUtil.isSetKafkaKerberos(kafkaProps,kafkaKerberos,securityProtocol)


    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), kafkaProps)

    kafkaConsumer
  }
}
