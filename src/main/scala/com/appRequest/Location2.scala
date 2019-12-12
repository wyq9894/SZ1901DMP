package com.appRequest

import com.util.ReqUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
spark core
 */
object Location2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("location").master("local")
      .getOrCreate()

    //获取路径
    val Array(inputPath,outputPath)=args

    //获取数据
    val df=spark.read.parquet(inputPath)
    df.rdd.map(row=>{
      // val client=row.getAs[Int]("client")
     val provincename=row.getAs[String]("provincename")
     val cityname=row.getAs[String]("cityname")
      val requestmode=row.getAs[Int]("requestmode")
     val processnode=row.getAs[Int]("processnode")
      val iseffective=row.getAs[Int]("iseffective")
      val isbilling=row.getAs[Int]("isbilling")
      val isbid=row.getAs[Int]("isbid")
      val iswin=row.getAs[Int]("iswin")
      val adorderid=row.getAs[Int]("adorderid")
      val winprice=row.getAs[Double]("winprice")
      val adpayment=row.getAs[Double]("adpayment")
    //业务处理方法
      val reqList = ReqUtils.reqAd(requestmode, processnode, iseffective,
        isbilling, isbid, iswin, adorderid, winprice, adpayment)
      ((provincename,cityname),reqList)
     // (client,reqList)
    }).reduceByKey((list1,list2)=>{
      // list1(1,2,3,4) list2(1,2,3,4) zip(List((1,1),(2,2),(3,3),(4,4)))
      list1.zip(list2)
        // List((1+1),(2+2),(3+3),(4+4))
        .map(t=>t._1+t._2)
      // List(2,4,6,8)
    }).map(t=>t._1+":"+t._2.mkString("<",",",">")).foreach(println)

/*
(江西省,抚州市):<3.0,2.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0>
(浙江省,金华市):<4.0,2.0,1.0,2.0,0.0,0.0,0.0,3.0,0.0>
 */

  }
}
