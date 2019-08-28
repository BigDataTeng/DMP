package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  * 标签格式：Kxxx->1
  * xxx为关键字，关键字个数不能少于 3 个字符，且不能超过 8 个字符；
  * 关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
  */
object TagKeyWord extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]
    //获取关键字，打标签  正则切分为[|]
    val keywords = row.getAs[String]("keywords").split("\\|")
    //按照条件进行过滤
    keywords.filter(x => {
      x.length >= 3 && x.length <= 8 && !stopword.value.contains(x)
    })
      .foreach(word=>list:+=("K"+word,1))
    list
  }
}
