package com.Tags.TagsEquipment

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 设备运营商方式
  * 移 动 D00030001 联 通 D00030002 电 信 D00030003
  * _ D00030004
  */
object TagsOperator extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val ispname = row.getAs[String]("ispname")
    if (StringUtils.isNotBlank(ispname)) {
      if (ispname=="移动"){
        list:+=(ispname+" D00030001",1)
      }else if (ispname=="联通"){
        list:+=(ispname+" D00030002",1)
      }else if (ispname=="电信"){
        list:+=(ispname+" D00030003",1)
      }else{
        list:+=("_ D00030004",1)
      }
    }
    list
  }
}
