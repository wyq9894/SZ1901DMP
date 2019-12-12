package Day1212
/*
要求：(a,1) (a,3) (b,3) (b,5) (c,4)，求每个key对应value的平均值

 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AvgTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local")
    val sc = new SparkContext(conf)
    val map = Array(("a",1),("a",3),("b",3),("b",5),("c",4))

   val rdd=sc.parallelize(map)

    rdd.combineByKey(v=>(v,1),
     (acc:(Int,Int),newV)=>(acc._1+newV,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))
      =>(acc1._1+acc2._1,acc1._2+acc2._2)).foreach(println)
  }
}
/*
(a,(4,2))
(b,(8,2))
(c,(4,1))
 */
