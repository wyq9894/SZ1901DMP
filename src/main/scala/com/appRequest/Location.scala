package com.appRequest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
地域指标统计
spark sql
 */
object Location {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("location").master("local")
      .getOrCreate()

   //获取路径,
   //设置路径 D:\output\streaming o
   val Array(inputPath,outputPath)=args

    //获取数据
   val df=spark.read.parquet(inputPath)
    df.createTempView("log")
spark.sql(
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as ysrequest,
        |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as yxrequest,
        |sum(case when requestmode=1 and processnode>=3 then 1 else 0 end) as adrequest,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as cybid,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid !=0 then 1 else 0 end) as cybidsuccess,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as show,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as click,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) as dspWinPrice,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) as dspadpayment
        |from log
        |group by
        |provincename,cityname
      """.stripMargin
    ).show()

  }
}
