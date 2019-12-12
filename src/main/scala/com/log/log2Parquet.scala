package com.log

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.util.{SchemaType, StrUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/*
将数据格式转换为Paequet格式
 */
object log2Parquet {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("parquet").master("local")
   //设置序列化方式
      .getOrCreate()
    //判断路径
   if (args.length !=2){
    println("目录不正确，退出")
    sys.exit()
  }
    import org.apache.spark.sql.functions._

   //获取数据
  val Array(inputPath,outputPath)=args
   val lines = spark.sparkContext.textFile(inputPath)
  //先切分再过滤，否则数组越界
  val rowRDD = lines.map(t=>t.split(",",-1)).filter(_.length>=85).map(arr=>{
      //转为DF
      Row(
        arr(0),
        StrUtils.toInt(arr(1)),
        StrUtils.toInt(arr(2)),
        StrUtils.toInt(arr(3)),
        StrUtils.toInt(arr(4)),
        arr(5),
        arr(6),
        StrUtils.toInt(arr(7)),
        StrUtils.toInt(arr(8)),
        StrUtils.toDouble(arr(9)),
        StrUtils.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StrUtils.toInt(arr(17)),
        arr(18),
        arr(19),
        StrUtils.toInt(arr(20)),
        StrUtils.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StrUtils.toInt(arr(26)),
        arr(27),
        StrUtils.toInt(arr(28)),
        arr(29),
        StrUtils.toInt(arr(30)),
        StrUtils.toInt(arr(31)),
        StrUtils.toInt(arr(32)),
        arr(33),
        StrUtils.toInt(arr(34)),
        StrUtils.toInt(arr(35)),
        StrUtils.toInt(arr(36)),
        arr(37),
        StrUtils.toInt(arr(38)),
        StrUtils.toInt(arr(39)),
        StrUtils.toDouble(arr(40)),
        StrUtils.toDouble(arr(41)),
        StrUtils.toInt(arr(42)),
        arr(43),
        StrUtils.toDouble(arr(44)),
        StrUtils.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StrUtils.toInt(arr(57)),
        StrUtils.toDouble(arr(58)),
        StrUtils.toInt(arr(59)),
        StrUtils.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StrUtils.toInt(arr(73)),
        StrUtils.toDouble(arr(74)),
        StrUtils.toDouble(arr(75)),
        StrUtils.toDouble(arr(76)),
        StrUtils.toDouble(arr(77)),
        StrUtils.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StrUtils.toInt(arr(84))
      )
    })
    // 构建DF
    val df = spark.createDataFrame(rowRDD,SchemaType.structType)
    // 保存数据结果,D:\output\streaming
 //   df.write.parquet(outputPath)



    // 创建临时表
   df.createTempView("province")
    // 执行SQL
    val result = spark.sql("select count(*) ct,provincename,cityname from province group by provincename,cityname")

    //配置mysql属性
    //加载配置文件
  /*
    val load=ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.username"))
    prop.setProperty("password",load.getString("jdbc.password"))
   //数据库自己建表 procity
    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"procity",prop)
*/
    //保存到磁盘，路径为
   result.write.partitionBy("provincename","cityname").json(outputPath)

    //减少分区
   // result.coalesce(10).write.json(outputPath)

    /*
   //DSL风格
   val df1 = df.groupBy("provincename","cityname").agg(count("cityname"))
   df1.show()
   */

  //存到HDFS
 //  df1.write.format("json").save("hdfs://hd001:9000"+"provincename/cityname/ct")

    // 保存数据结果
   //  df1.write.format("json").save(outputPath)



    /*
    //存到MYSQL
    val prop = new Properties()
    prop.setProperty("user","hive")
    prop.setProperty("password","123456")
    df1.write.jdbc("jdbc:mysql://hd001:3306/location","province1",prop)
   */



    // 关闭
    spark.stop()
  }
}

