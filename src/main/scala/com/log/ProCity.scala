package com.log

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ProCity {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(" ProCity").master("local")
      .getOrCreate()


  //  if(args.length!=2){
  //    sys.exit()
  //  }

    //获取路径
 //   val Array(inputPath,outputPath)=args
    //读取数据
    val df=spark.read.parquet("D:\\output\\streaming")
    // 创建临时表
    df.createTempView("procity")
/*
地域分布
 */
    val result1=spark.sql("select " +
      "provincename,cityname," +
      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as request1," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as request2," +
      "sum(case when requestmode=1 and processnode>=3 then 1 else 0 end) as request3," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as rtb1," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid !=0 then 1 else 0 end) as rtb2," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as showcount," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as clickcount," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) as comsume," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) as cost " +
      "from procity " +
      "group by provincename,cityname")

   //   result1.show()

    /*
    终端设备
    运营
     */
    val result2=spark.sql("select " +
      "ispname," +
      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as request1," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as request2," +
      "sum(case when requestmode=1 and processnode>=3 then 1 else 0 end) as request3," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as rtb1," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid !=0 then 1 else 0 end) as rtb2," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as showcount," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as clickcount," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) as comsume," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) as cost " +
      "from procity " +
      "group by ispname")

   //  result2.show()


    /*
    终端设备
    网络类
     */

    val result3=spark.sql("select " +
      "networkmannername," +
      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as request1," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as request2," +
      "sum(case when requestmode=1 and processnode>=3 then 1 else 0 end) as request3," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as rtb1," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid !=0 then 1 else 0 end) as rtb2," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as showcount," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as clickcount," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) as comsume," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) as cost " +
      "from procity " +
      "group by networkmannername")

     // result3.show()


    /*
  终端设备
  设备类
    */

    val result4=spark.sql("select " +
      "devicetype," +
      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as request1," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as request2," +
      "sum(case when requestmode=1 and processnode>=3 then 1 else 0 end) as request3," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as rtb1," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid !=0 then 1 else 0 end) as rtb2," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as showcount," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as clickcount," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) as comsume," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) as cost " +
      "from procity " +
      "group by devicetype")

    // result4.show()



    /*
  终端设备
  操作系
    */


    val result5=spark.sql("select " +
      "client," +
      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as request1," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as request2," +
      "sum(case when requestmode=1 and processnode>=3 then 1 else 0 end) as request3," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as rtb1," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid !=0 then 1 else 0 end) as rtb2," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as showcount," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as clickcount," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end) as comsume," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) as cost " +
      "from procity " +
      "group by client")

    result5.show()


      spark.stop()
  }

}
