package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  *媒体标签
  */
object TagsApp2 extends Tag{
  //打标签的统一接口
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[String]
    //获取Appid和AppName
    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")

    //空值判断
    if (StringUtils.isNotBlank(appname)) {
      list:+=("App "+appname,1)
    }else if (StringUtils.isNotBlank(appid)){
      list:+=("App "+jedis,1)
    }
    list
  }
}
