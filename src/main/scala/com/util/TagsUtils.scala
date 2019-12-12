package com.util
/*
获取用户ID
 */
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsUtils {
//获取所有不为空的id
  val oneUseId=
    """
      |imei!='' or mac!='' or idfa!='' or openudid!='' or androidid!=''
    """.stripMargin

  // 获取唯一不为空用户ID
 def getOneUserID(row:Row):String={
   row match {
     case v if StringUtils.isNoneEmpty(row.getAs("imei"))=>"IM:"+v.getAs("imei")
     case v if StringUtils.isNoneEmpty(row.getAs("mac"))=>"IM:"+v.getAs("mac")
     case v if StringUtils.isNoneEmpty(row.getAs("idfa"))=>"IM:"+v.getAs("idfa")
     case v if StringUtils.isNoneEmpty(row.getAs("openudid"))=>"IM:"+v.getAs("openudid")
     case v if StringUtils.isNoneEmpty(row.getAs("androidid"))=>"IM:"+v.getAs("androidid")
     case _ =>"未知"
   }
 }

  //因业务需求，将同一用户不同userid标签合并，避免数据冗余
  // 获取所有用户
  def getAllUserId(row:Row)={
    var list =List[String]()
    if(!row.getAs[String]("imei").isEmpty) list:+="IM"+row.getAs[String]("imei")
    if(!row.getAs[String]("mac").isEmpty) list:+="MC"+row.getAs[String]("mac")
    if(!row.getAs[String]("idfa").isEmpty) list:+="ID"+row.getAs[String]("idfa")
    if(!row.getAs[String]("openudid").isEmpty) list:+="OP"+row.getAs[String]("openudid")
    if(!row.getAs[String]("androidid").isEmpty) list:+="AD"+row.getAs[String]("androidid")
    list
  }
}
