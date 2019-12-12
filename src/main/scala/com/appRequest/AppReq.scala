package com.appRequest

/*
媒体指标
1)利用广播变量广播小文件
2)使用redis存放字典文件
 */
import com.util.ReqUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AppReq {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("location").master("local")
      .getOrCreate()

    //获取路径,路径编辑 D:\output\streaming o D:\input\sparkdata\sparkstreaming\app_dict.txt
    val Array(inputPath,outputPath,add_dir)=args

    //获取数据
    val df=spark.read.parquet(inputPath)
    //读取字典文件
    val lines=spark.sparkContext.textFile(add_dir)
    //切分数据获取到appid,appname收集到Map里
    val appMap = lines.filter(_.split("\t", -1).length >= 5)
      .map(arr=>{
        arr.split("\t", -1)
      }).map(arr=>(arr(4), arr(1)))
     .collectAsMap()


   //广播文件
   val broadcast = spark.sparkContext.broadcast(appMap)
/*
 //将字典文件存到Redis中
    appMap.foreachPartition(rdd=>{
      // 开启redis连接
      val jedisCluster =new JedisClusters
      val jedis = jedisCluster.JedisC
   rdd.foreach(t=>{
     //存入redis
     jedis.set(t._1,t._2)
   })
      //还连接
      jedis.close()
    })
 */


    df.rdd.map(row => {
      val appid = row.getAs[String]("appid")
      var appname = row.getAs[String]("appname")
      //判断当前AppName是否为空
      if (appname.isEmpty) {
        appname = broadcast.value.getOrElse(appid, "其他")
      }

      /*
        //读取redis内的字段文件
        val jedisCluster = new JedisClusters
        val jedis = jedisCluster.JedisC
        val appid = row.getAs[String]("appid")
        var appname = row.getAs[String]("appname")
        // 从redis缓存读取,判断当前AppName是否为空
        if (appname.isEmpty) {
          appname = jedis.get(appid)
        }
        //还连接
        jedis.close()
       */

      //把需要的字段拿出来
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 业务处理方法
      val reqList = ReqUtils.reqAd(requestmode, processnode, iseffective,
        isbilling, isbid, iswin, adorderid, winprice, adpayment)
      (appname, reqList)
      //聚合同一个appaname的value
    }).reduceByKey((list1,list2)=>{
      // list1(1,2,3,4) list2(1,2,3,4) zip(List((1,1),(2,2),(3,3),(4,4)))
      list1.zip(list2)
        // List((1+1),(2+2),(3+3),(4+4))
        .map(t=>t._1+t._2)
      // // List(2,4,6,8)
    }).map(t=>
      t._1+" : "+t._2.mkString("<",",",">"))
      .foreach(println)
  }
}
