package com.sxkj.sink

import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import java.util.ArrayList

import com.sxkj.utils.PropertiesUtils


/**
  * @Author: NingLang
  * @Date: 2019/6/4 20:05
  * @Version 1.0
  * @Description Flink流数据Sink到ElasticSearch
  */
class EsSinkFunction (es_indexName: String) {

    val es_host = PropertiesUtils.loadBuriedPointProperties("es_host")
    val es_port = PropertiesUtils.loadBuriedPointProperties("es_port").toInt

    val hostArr : Array[String]= es_host.split(",")

    val httpHosts = new ArrayList[HttpHost]

    for (host <- hostArr){
        httpHosts.add(new HttpHost(host,es_port, "http"))
    }

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
        httpHosts,
        new ElasticsearchSinkFunc(es_indexName)
    )
}
