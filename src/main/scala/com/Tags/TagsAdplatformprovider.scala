package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  *渠道标签
  * 格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
  */
object TagsAdplatformprovider extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    //处理参数
    val row = args(0).asInstanceOf[Row]
    //获取渠道名称
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    if (StringUtils.isNotBlank(adplatformproviderid+"")){
      list:+=("CN"+adplatformproviderid,1)
    }
    list
  }
}
