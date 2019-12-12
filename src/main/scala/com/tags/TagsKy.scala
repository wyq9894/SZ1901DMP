package com.tags

import com.util.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object TagsKy extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    //转换为Row类型
    val row: Row = args(0).asInstanceOf[Row]
    //转为广播变量的类型
    val wordbroadcast: Array[String] = args(1).asInstanceOf[Array[String]]

    //获取关键字
    val words=row.getAs[String]("keywords")

    if(! words.isEmpty) {
      //切成多个关键字
      val keywords: Array[String] = words.split("\\|")
       //x为字符
      keywords.filter(x => {
        !wordbroadcast.contains(x) && x.length >= 3 && x.length <=8
      }).foreach(t=>{
        list:+= ("K"+t, 1)
      })
    }

   /*
      //遍历时打印出地址,keywords为数组
      if (!wordbroadcast.contains(keywords) && keywords.length >= 3
        && keywords.length < 8) {
        keywords.foreach(t=>{
          list :+= ("K" + keywords, 1)
        })
          }
        }
    */

    list
  }
}
