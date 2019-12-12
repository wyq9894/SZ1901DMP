package com.util
/*
类型转换工具类
 */
object StrUtils {
  //不用if条件
  def toInt(str:String):Int={
  try {
  str.toInt
  }catch{
  case _:Exception =>0
  }
  }

  def toDouble(str:String):Double={
    try {
      str.toInt
    }catch{
      case _:Exception =>0.0
    }
  }

}
