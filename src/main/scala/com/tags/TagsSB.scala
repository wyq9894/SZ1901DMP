package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

object TagsSB extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    //转换为Row类型
    val row = args(0).asInstanceOf[Row]

    //获取设备操作系统

     val client=row.getAs[Int]("client")
  client match{
    case 1 =>list:+=(client + "Android D00010001",1)
    case 2 =>list:+=(client + " IOS D00010002",1)
    case 3 =>list:+=(client + " WinPhone D00010003",1)
      case _ =>list:+=("_ 其他 D00010004",1)
  }

    //设备联网方式
      val networkmannername=row.getAs[String]("networkmannername")
    networkmannername match{
      case "Wifi" =>list:+=(networkmannername + "D00020001",1)
      case "4G" =>list:+=(networkmannername + "D00020002",1)
      case "3G" =>list:+=(networkmannername + "D00020003",1)
      case "2G" =>list:+=(networkmannername + "D00020004",1)
      case _ =>list:+=("_ D00020005",1)
    }

    //设备运营商方式
    val ispname=row.getAs[String]("ispname")
    ispname match{
      case "移动" =>list:+=(ispname + "D00030001",1)
      case "联通" =>list:+=(ispname + "D00030002",1)
      case "电信" =>list:+=(ispname + "D00030003",1)
      case _ =>list:+=("_ D00030004",1)
    }

  list
  }
}

