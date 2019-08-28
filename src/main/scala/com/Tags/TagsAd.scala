package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsAd extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //获取广告类型
    val adType = row.getAs[Int]("adspacetype")
    //判断广告位类型(标签格式： LC03->1 或者 LC16->1)
    // 模式匹配xx 为数字，小于 10 补 0，把广告位类型名称，LN 插屏->1
    adType match {
      case v if v > 9 => list :+= ("LC" + v,1)
      case v if v <= 9 => list :+= ("LC0" + v,1)
    }
    //获取广告类型名称
    val adName = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adName)) {
      list :+= ("LN" + adName,1)
    }
    list
  }
}
