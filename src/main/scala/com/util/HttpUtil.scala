package com.util

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/*
发送请求 GET
 */
object HttpUtil {
 def get(urlStr:String):String={
   //获取客户端
   val client = HttpClients.createDefault()
   val httpGet = new HttpGet(urlStr)
   // 发送请求
   val response = client.execute(httpGet)
   // 将结果格式化
   EntityUtils.toString(response.getEntity,"UTF-8")

 }
}
