package com.Tags.TagsEquipment

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 设 备 联 网 方 式WIFI D00020001 4G D00020002
  * 3G D00020003
  * 2G D00020004
  * _   D00020005
  */
object TagsInternet extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val networkmannername = row.getAs[String]("networkmannername")
    if (StringUtils.isNotBlank(networkmannername)) {
      if (networkmannername=="Wifi"){
        list:+=(networkmannername+" D00020001",1)
      }else if(networkmannername=="4G"){
        list:+=(networkmannername+" D00020002",1)
      }else if(networkmannername=="3G"){
        list:+=(networkmannername+" D00020003",1)
      }else if(networkmannername=="2G"){
        list:+=(networkmannername+" D00020004",1)
      }else{
        list:+=("_",1)
      }
    }
    list
  }
}
