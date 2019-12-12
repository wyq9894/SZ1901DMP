package com.tags
/*
将同一用户的不同ID对应的标签合并
 */


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
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}


/*
上下标签，总标签，合并所有标签
 */
object TagsContext2{
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)

    //获取路径
    val Array(inputPath,outputPath,add_dir1,add_dir2,day)=args

    val spark = SparkSession.builder()
      .appName("TagsContext").master("local")
      .getOrCreate()

    /*

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
  */


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

    val baseRDD: RDD[(List[String], Row)]=df
      // 过滤符合的用户数据
      .filter(TagsUtils.oneUseId)
      .rdd.map(row => {
      //拿到UserID,userId为集合
      val userId = TagsUtils.getAllUserId(row)
      (userId, row)
    })

    /*
    (List(IM8aeb752b6c, MC2sbw7w2b8m, ID7ceo1q5o5b,OP1s5d2d6l3d, AD8qaz8q2t2l),
    [0bb490c3000057eee4a8449500a54351,0,0,0,100002,未知,26C07B8C83DB4B6197CEB80D53B3F5DA,1,2,0.0,0.0,2016-10-0106:18:04,175.188.159.32,com.apptreehot.tyh,马上赚钱,AQ+CJwWBjO5xfa949sRDn1wuQoTC,HUAWEI+G730-T00,1,4.2.2,,540,960,116.495232899,24.978614572,河南省,郑州市,4,未知,3,Wifi,0,1,2,插屏,1,2,10,未知,1,1,6228.0,0.0,0,0,0.0,0.0,8aeb752b6c,2sbw7w2b8m,7ceo1q5o5b,1s5d2d6l3d,8qaz8q2t2l,,,,,,,0,555.0,240,290,,,,,,,,,,,AQ+CJwWBjO5xfa949sRDn1wuQoTC,,1,0.0,0.0,0.0,0.0,7886.0,,,mm_26632353_8068780_27326559,2016-10-0106:18:04,,0])

     */


    // 打标签

    //构建点，用flatMap压平，userId是集合,将其全部压为一个集合
  val VD = baseRDD.flatMap(tp=>{
       val row =tp._2
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

    //将用户ID保存至标签中，保留着原始ID
       val vds=tp._1.map((_,0))++TagList

       //将同一用户的不同id合并,vd为集合
     //如果过来的第一条数据的用户id等于uid,返回一条标签数据
       //保证每一个ID携带一个标签，其他ID不携带
    tp._1.map(uid=>{
    if(tp._1.head.equals(uid)){
      //uid转为long类型
      (uid.hashCode.toLong,vds)
    } else {
      (uid.hashCode.toLong,List.empty)
    }
     })
     })
     //  .take(20).foreach(println)

    /*
(971663965,List((IM1b5e2a7b75,0), (MC3amd2e5c4e,0), (ID7bwd7p2z6h,0), (OP8zbd2p4q5c,0), (AD3bod5e6t7e,0), (LC02,1), (LN插屏,1), (APP其他,1), (CN100002,1), (1Android D00010001,1), (WifiD00020001,1), (_ D00030004,1), (ZP湖南省,1), (ZC益阳市,1), (error!,1)))
(1621329389,List())
(1410977414,List())
(-911227282,List())
(39485283,List())
     */

   //构建边
   //保证一个点A不变，其他点B,C在变
  // A B C A->B A->C
  val ED = baseRDD.flatMap(tp=>{
      tp._1.map(uid=>{
        Edge(tp._1.head.hashCode.toLong,uid.hashCode.toLong,0)
      })
    })
     // .take(20).foreach(println)
/*
Edge(-1624268898,1126574050,0)
Edge(-1624268898,-859417086,0)
Edge(-1624268898,-518551352,0)
Edge(-1624268898,2125519644,0)
 */



    //构建图
  val graph=Graph(VD,ED)
    //根据点和线找出最小顶点,格式：vertices=(每个点,最小的公共顶点)
   val vertices = graph.connectedComponents().vertices
    //vertices与VD再join
     vertices.join(VD).map{
       //匹配，返回(vd,标签+userid),vd为顶点
      //字段名自己设置，符合join后的格式即可
       case (uid,(vd,tagsUserId))=>(vd,tagsUserId)
     }.reduceByKey{
       case (list1,list2)=>
         // 按照集合内的Tuple._1分组
       (list1++list2)
         // 按照集合内的Tuple._1分组
         .groupBy(_._1)
         // 聚合每个Tuple的Value值
        // .mapValues(_.foldLeft[Int](0)(_+_._2))
           .mapValues(_.map(_._2).sum)
         //转为List
           .toList
     }
    .take(10).foreach(println)


        /*
         .map{
       case (userId,userTags)=>{
         // 设置rowkey
         val put = new Put(Bytes.toBytes(userId))
         // 添加列的value
        //列簇
         put.addImmutable(Bytes.toBytes("tags"),
           Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
         // 设置返回对象和put(rowkey，列簇，列的值)
         (new ImmutableBytesWritable(),put)
       }
       // 将数据存储入Hbase
     }.saveAsHadoopDataset(jobConf)

*/
    spark.stop()
  }
}

/*
(-667807492,List((,1), (LN插屏,1), (ZC信阳市,1), (IM4ddc7s5l5c,0), (AD4oz53t2a2a,0), (LC02,1), (_ D00020005,1), (1Android D00010001,1), (OP4oh53b3o7l,0), (CN100002,1), (ZP河南省,1), (_ D00030004,1), (MC7tse2z3s5d,0), (ID6qez0l3s5m,0), (APP其他,1)))
(-1112807091,List((,1), (ZP河北省,1), (OP2eaq5w8c3b,0), (K游戏世界,1), (ID3a5a1e0l4a,0), (K网络游戏,1), (1Android D00010001,1), (APP爱奇艺,1), (LN视频前贴片,1), (WifiD00020001,1), (MC1bmq3d3b1s,0), (K我的世界,1), (ZC石家庄市,1), (AD6wcl0a8p5m76wcl0a8p5m46wcl0a8p5m76wcl0a8p5mf6wcl0a8p5m76wcl0a8p5md6wcl0a8p5m86wcl0a8p5mc6wcl0a8p5m76wcl0a8p5m96wcl0a8p5m16wcl0a8p5m96wcl0a8p5md6wcl0a8p5mc6wcl0a8p5ma6wcl0a8p5m76wcl0a8p5m,0), (CN100018,1), (IM2aow2c6a0a,0), (LC12,1), (_ D00030004,1)))

 */