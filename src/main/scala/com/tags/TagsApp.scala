package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

object TagsApp extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {

  var list=List[(String,Int)]()
    //转换为Row类型
    val row = args(0).asInstanceOf[Row]
    //转为广播变量的类型
    val appbroad=args(1).asInstanceOf[collection.Map[String, String]]

    //获取App名称
    val appid = row.getAs[String]("appid")
    var appname = row.getAs[String]("appname")
    //判断当前AppName,appid是否为空
    if (!appname.isEmpty) {
      list:+=("APP"+appname,1)
    }else if (!appid.isEmpty){
     list:+=("APP"+appbroad.getOrElse(appid, "其他"),1)
    }
    //返回
    list
  }
}
