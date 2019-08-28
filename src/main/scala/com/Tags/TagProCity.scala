package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  *地域标签
  * （省标签格式：ZPxxx->1, 地市标签格式: ZCxxx->1）
  * xxx 为省或市名称
  */
object TagProCity extends Tag{
  def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(provincename)) {
      list:+=("ZP"+provincename,1)
    }
    if (StringUtils.isNotBlank(cityname)) {
      list:+=("ZC"+cityname,1)
    }
    list
  }
}
