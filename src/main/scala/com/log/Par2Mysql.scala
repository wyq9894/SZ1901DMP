package com.log

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
将数据结果存入Mysql或HDFS
 */
object Par2Mysql {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("Par2Mysql").master("local")
      .getOrCreate()


    if(args.length!=2){
      sys.exit()
    }

    //获取路径
    val Array(inputPath,outputPath)=args
    //读取数据
    val df=spark.read.parquet(inputPath)
    // 创建临时表
    df.createTempView("log")
    // 执行SQL
    val result = spark.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    //存入mysql
        val load=ConfigFactory.load()
        val prop = new Properties()
        prop.setProperty("user",load.getString("jdbc.username"))
        prop.setProperty("password",load.getString("jdbc.password"))
       //数据库自己建表 procity
        result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"procity",prop)

    //要加上保存磁盘路径，否则不执行 D:\output\streaming D:\output\Result
    result.write.save(outputPath)


    //保存到磁盘，路径为省份/城市/数据
    /*
    路径编辑 D:\output\streaming D:\output\pro
     */
  //  result.write.partitionBy("provincename","cityname").json(outputPath)

  }
}
