package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

object TagsQD extends Tags{
  override def makeTags(args:Any*):List[(String,Int)]= {

    var list=List[(String,Int)]()
    //转换为Row类型
    val row =args(0).asInstanceOf[Row]

    //获取渠道
    val adp=row.getAs[Int]("adplatformproviderid")

    if(adp.toString != null && !adp.equals(0)){
      list:+=("CN"+adp,1)
    }
  //返回值
    list
  }
}
