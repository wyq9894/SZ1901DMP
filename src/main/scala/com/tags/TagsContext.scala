package com.tags

import com.typesafe.config.ConfigFactory
import com.util.TagsUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession


/*
上下标签，总标签，合并所有标签
 */
object TagsContext {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)

    //获取路径
    val Array(inputPath,outputPath,add_dir1,add_dir2,day)=args
    val spark = SparkSession.builder()
      .appName("TagsContext").master("local[*]")
      .getOrCreate()

    //Hbase配置
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.table.name")
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
    val hbConn = ConnectionFactory.createConnection(configuration)
   println(hbConn)
    val admin = hbConn.getAdmin
      //强转
    if(!admin.tableExists(TableName.valueOf(hbaseTableName))){
      println("当前表可用")
      // 创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // 将列簇加入表中
      tableDescriptor.addFamily(columnDescriptor)
      // 创建表
      admin.createTable(tableDescriptor)
      admin.close()
      hbConn.close()
    }

  //创建job
  val jobConf = new JobConf(configuration)
   //指定key类型，上边界，添加OutputFormat的子类TableOutputFormat
    //要转为TableOutputFormat
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 指定表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)


    // 获取数据
    val df = spark.read.parquet(inputPath)

    //读取appname字典文件
    val lines1=spark.sparkContext.textFile(add_dir1)

    //切分数据获取到appid,appname收集到Map里
    val appMap = lines1.filter(_.split("\t", -1).length >= 5)
      .map(arr=>{
        arr.split("\t", -1)
      }).map(arr=>(arr(4), arr(1)))
      .collectAsMap()


    //广播appname文件
    val broadcastApp: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(appMap)


    //读取停用词文件
  val kyMap: Array[String] = spark.sparkContext.textFile(add_dir2).collect

    // 广播停用词文件
  val broadcast2: Broadcast[Array[String]] = spark.sparkContext.broadcast(kyMap)

    // 打标签
  df
    // 过滤符合的用户数据
    .filter(TagsUtils.oneUseId)
      .rdd.map(row=>{
        //拿到UserID
     val userId=TagsUtils.getOneUserID(row)

        //todo 广告标签
        //返回的是list
        //路径D:\output\streaming o
      val adTags=TagsAD.makeTags(row)

       // todo App名称标签
    //路径D:\output\streaming o D:\input\sparkdata\sparkstreaming\app_dict.txt

      val appTags=TagsApp.makeTags(row,broadcastApp.value)

     //todo 渠道标签
      val qdTags=TagsQD.makeTags(row)

     //todo 设备标签
      val sbTags=TagsSB.makeTags(row)

     //todo 关键字标签
//路径 D:\output\streaming o D:\input\sparkdata\sparkstreaming\app_dict.txt D:\input\sparkdata\sparkstreaming\stopwords.txt
    val kyTags=TagsKy.makeTags(row,broadcast2.value)

    //todo 地域标签
    val procityTags=TagsPC.makeTags(row)

    //todo 商圈标签
    val businessList = BusinessTag.makeTags(row)

    val TagList=adTags ++ appTags ++ qdTags ++ sbTags ++ kyTags ++ procityTags ++ businessList
  (userId,TagList)
   // (userId,businessList)
  })
    .reduceByKey((list1,list2)=>{
    //两个list[(String,Int)]拼接再聚合
    list1++list2.groupBy(_._1)
     //初始值为0，聚合里面的value
      .mapValues(_.foldLeft[Int](0)(_+_._2))
      })
      .map{
     //自己设置字段名
    case (userId,userTags)=>{
      // 设置rowkey
      val put = new Put(Bytes.toBytes(userId))
      // 前面建了表和列簇，这里赋值，添加列的value
      put.addImmutable(Bytes.toBytes("tags"),
       //以天限制，day设置2019-12-11
     //路径  D:\output\streaming o D:\input\sparkdata\sparkstreaming\app_dict.txt D:\input\sparkdata\sparkstreaming\stopwords.txt 2019-12-11
        Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
      // 设置返回对象和put(rowkey，列簇，列的值)
      (new ImmutableBytesWritable(),put)
    }
        // 将数据存储入Hbase
  }.saveAsHadoopDataset(jobConf)

 /*
 查看hbase表数据
 scan 'sz1901'
 清空hbase表数据
 truncate 'sz1901'
  */

    /*
    .map(t=>
    t._1+" : "+t._2.mkString("<",",",">"))
    .foreach(println)
    */
  }
}
