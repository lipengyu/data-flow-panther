package com.wedata.stream.app

import java.util.Properties

import com.sxkj.utils.{ConnectKafkaKerberosUtil, PropertiesUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object SendMessageToTopic {
  def sendToTopic(kafkaTopic: String*): Unit = {
    //往topic中写数据
    //val topic = kafkaTopic
    // 解析参数

    //指定broker的ip和端口号
    //"cdh-s1.sxkj.online:9092,cdh-s2.sxkj.online:9092,cdh-s3.sxkj.online:9092,cdh-s4.sxkj.online:9092,cdh-s5.sxkj.online:9092,cdh-s6.sxkj.online:9092,cdh-s7.sxkj.online:9092"
    val brokers=PropertiesUtils.loadLogAuditProperties("kafkaBroker")
    //建配置文件

    val props=new Properties()
    props.put("bootstrap.servers",brokers)

    //判断是否需要配置kafka的kerberos，当传入的参数个数大于2个时说明需要连接kerberos
    val kafkaKerberos = PropertiesUtils.loadLogAuditProperties("kafkaKerberos")
    val securityProtocol = PropertiesUtils.loadLogAuditProperties("securityProtocol")
    ConnectKafkaKerberosUtil.isSetKafkaKerberos(props,kafkaKerberos,securityProtocol)

    //指定Kafka的编译器 放入
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //props.put("serializer.class","kafka.serializer.StringEncoder")
    //配置kafka的config
    //val kafkaconfig=new ProducerConfig(props)
    val producer= new KafkaProducer[String,String](props)

    while(true){
      for(topic <- kafkaTopic){
        producer.send(new ProducerRecord[String,String](topic,"keys"," "))
      }
      Thread.sleep(30*1000)
      //println("30min send a message")

    }


  }
}
