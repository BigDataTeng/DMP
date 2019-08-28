package kaoshi

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
  */
object TagType extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //获取广告类型
    val Type = row.getAs[String]("type")
    if (StringUtils.isNotBlank(Type)) {
      list:+=(Type,1)
    }
    list
  }
}
