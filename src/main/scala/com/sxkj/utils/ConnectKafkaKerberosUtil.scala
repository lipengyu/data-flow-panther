package com.sxkj.utils

import java.util.Properties
/**
  * @Author:zhengxiaofei
  * @Date:2019 /5/21 15:42
  * @Version 1.0
  * @Description 连接配有kerberos的kafka
  */
object ConnectKafkaKerberosUtil {
  /**
    * @Description 设置了kerberos就传相关参数
    * @Author:zhengxiaofei
    * @Date: 2019/5/21 15:53
    * @param kafkaProps,kafkaKerberos,securityProtocol
    * @return_type 默认
    * @updateInfo
    */
  def isSetKafkaKerberos(kafkaProps:Properties,kafkaKerberos:String,securityProtocol:String): Unit = {
    if(kafkaKerberos != null && !"".equals(kafkaKerberos)){
      val jaas = PropertiesUtils.loadLogAuditProperties("jaas")
      val krbs = PropertiesUtils.loadLogAuditProperties("krbs")

      System.setProperty("java.security.krb5.conf", jaas)
      System.setProperty("java.security.auth.login.config", krbs)

      kafkaProps.setProperty("security.protocol",securityProtocol)
      kafkaProps.setProperty("sasl.kerberos.service.name", kafkaKerberos)
    }
  }
}
