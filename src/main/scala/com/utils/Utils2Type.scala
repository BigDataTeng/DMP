package com.utils

/**
  * Description：xxxx<br/>
  * Copyright(c),2019,Beijing<br/>
  * This program is protected by copyright laws.<br/>
  * Date:2019年08月20日 17:38
  *
  * @author Mr.Liu 
  * @version : 1.0
  */
object Utils2Type {
  //String转换int
  def toInt(str: String): Int = {
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
  }

  //String转换double
  def toDouble(str: String): Double = {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0.0
    }
  }
}
