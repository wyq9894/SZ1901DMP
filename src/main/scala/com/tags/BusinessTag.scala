package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisClusters, StrUtils, Tags}
import org.apache.spark.sql.Row

object BusinessTag extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度
   //可以过滤一下
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    // 通过经纬度获取商圈
    if(StrUtils.toDouble(lat)==0.0 && StrUtils.toDouble(long)==0.0){
      val business = getBusiness(long,lat)
      //将商圈信息切分
      val lines=business.split(",")
      lines.foreach(t=>{
        list:+=(t,1)
      })
    }

    list
  }

  /**
    * 获取商圈
    * @param long
    * @param lat
    * @return
    */
  def getBusiness(long: String,lat: String):String={
    try{
    //通过百度的逆地址解析，获取到商圈信息
     //6个字符，自己设置
     //lat.toDouble,long.toDouble类型不匹配，改为StrUtils.toDouble(lat)
    val geoHash=GeoHash.geoHashStringWithCharacterPrecision(StrUtils.toDouble(lat),StrUtils.toDouble(long),6)

    // 去查询数据库,可变
    var str=redis_queryBusiness(geoHash)

  //数据库没有，去高德查询
  if(str==null||str.length==0){
   //str为商圈信息
    str=AmapUtil.getBusinessFromAMap(StrUtils.toDouble(long),StrUtils.toDouble(lat))

    // 存储到redis，(geoHash,str)
    redis_insertBusiness(geoHash,str)
  }
    str
  }
  catch {
      case e:Exception =>"error!"
    }
  }


  /**
    * 查询数据库
    * @param geoHash
    * @return
    */
  def redis_queryBusiness(geoHash: String): String = {
    //调优，将其放入主类，传入到这个类中
    val jedisCluster =new JedisClusters
    val jedis = jedisCluster.JedisC
    val str = jedis.get(geoHash)
    jedis.close()
    //返回
     str
  }


  /**
    * 将数据存储redis
    * @param geoHash
    * @param str
    * @return
    */
  def redis_insertBusiness(geoHash: String, str: String) = {
    val jedisCluster =new JedisClusters
    val jedis = jedisCluster.JedisC
    jedis.set(geoHash,str)
    jedis.close()
  }
}
