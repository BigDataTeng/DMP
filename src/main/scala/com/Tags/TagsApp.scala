package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  *媒体标签
  */
object TagsApp extends Tag{
  //打标签的统一接口
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    val appIdAndName = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    //获取Appid和AppName
    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")
    //空值判断
    if (StringUtils.isNotBlank(appname)) {
      list:+=("App "+appname,1)
    }else if (StringUtils.isNotBlank(appid)){
      list:+=("App "+appIdAndName.value.getOrElse(appid,"无"),1)
    }
    list
  }
}
