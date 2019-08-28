package com.Tags.TagsEquipment

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 设备操作系统标签    (操作系统 -> 1)
  * 设备操作系统
  * 1 Android D00010001
  * 2 IOS D00010002
  * 3 WinPhone D00010003
  * _ 其 他 D00010004
  */
object TagsOS extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val client = row.getAs[Int]("client")
    if (StringUtils.isNotBlank(client+"")){
      if (client==1){
        list:+=(client+" Android D00010001",1)
      }else if (client==2){
        list:+=(client+" IOS D00010002",1)
      }else if (client==3){
        list:+=(client+" WinPhone D00010003",1)
      }else{
        list:+=("_ 其 他 D00010004",1)
      }
    }
    list
  }
}
