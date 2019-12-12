package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

object TagsPC extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    //转换为Row类型
    val row = args(0).asInstanceOf[Row]

    //获取省市数据
    val provincename=row.getAs[String]("provincename")
    val cityname=row.getAs[String]("cityname")

    if (!provincename.isEmpty){
      list:+=("ZP"+provincename,1)
    }


    if(!cityname.isEmpty){
      list:+=("ZC"+cityname,1)
    }

    list
  }
}
