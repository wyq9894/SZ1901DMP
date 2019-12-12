package com.Graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*

 */
object GraphxTest {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop\\hdfs\\hadoop-common-2.6.0-bin-master")
    Logger.getLogger("org").setLevel(Level.WARN)


    val spark = SparkSession.builder()
      .appName("GraphxTest").master("local[*]")
      .getOrCreate()
    // 构建点和边
    // 构建点
    val vdRDD = spark.sparkContext.makeRDD(Seq(
      (1L,("张三",23)),
      (2L,("阿奎罗",33)),
      (6L,("内马尔",27)),
      (9L,("马塞洛",35)),
      (133L,("库蒂尼奥",36)),
      (5L,("席尔瓦",35)),
      (7L,("法尔考",39)),
      (158L,("张智",39)),
      (16L,("苏亚雷斯",30)),
      (138L,("武磊",28)),
      (21L,("J罗",23)),
      (44L,("李四",30))
    ))

   //构建边，0为属性
   val edgeRDD  = spark.sparkContext.makeRDD(Seq(
     Edge(1L,133L,0),
     Edge(2L,133L,0),
     Edge(6L,133L,0),
     Edge(9L,133L,0),
     Edge(16L,138L,0),
     Edge(6L,138L,0),
     Edge(21L,138L,0),
     Edge(44L,138L,0),
     Edge(5L,158L,0),
     Edge(7L,158L,0)
   ))

   //构建图
   val graph = Graph(vdRDD,edgeRDD)
  // 取出所有连接的顶点,vertices为最小顶点
  val vertices= graph.connectedComponents().vertices
  //vd.foreach(println)
/*
    (44,1)
    (158,5)
    (138,1)
    (6,1)
    (2,1)
    (7,5)
    (21,1)
    (133,1)
    (1,1)
    (9,1)
    (5,5)
   */

    /*
    测试
    vd.join(vdRDD).foreach(println)

    (133,(1,(库蒂尼奥,36)))
    (1,(1,(张三,23)))
    (9,(1,(马塞洛,35)))
*/

   //整理数据,模式匹配
   //case (userId,(顶点,数据))=>(顶点,(数据))

   //以vdRDD的uid与vertices左边的点值join
   vertices.join(vdRDD).map{
    //字段名自己设置
     case (uid,(vd,(name,age)))=>(vd,List((name,age)))
   }.reduceByKey(_++_).foreach(println)

  }
}
