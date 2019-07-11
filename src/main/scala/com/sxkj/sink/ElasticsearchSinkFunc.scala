package com.sxkj.sink

import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * @Author: NingLang
  * @Date: 2019/6/5 16:23
  * @Version 1.0
  * @Description 实现ElasticsearchSinkFunction接口避免报not serializable错误
  */
class ElasticsearchSinkFunc(index: String) extends ElasticsearchSinkFunction[String] {

  def createIndexRequest(element: String): IndexRequest = {
    val gson = new Gson()
    val json = gson.fromJson(element, classOf[util.HashMap[String,Object]])
    return Requests.indexRequest()
      .index(index)
      .`type`("type")
      .source(json)
  }

  override def process(element: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    requestIndexer.add(createIndexRequest(element))
  }
}

