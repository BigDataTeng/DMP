package kaoshi

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * json数据归纳格式（考虑获取到数据的成功因素 status=1成功 starts=0 失败）：
  * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
  * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  */
object kaoshi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //获取数据
    val data: RDD[String] = sc.textFile("dir/json.txt")
    //收集数据
    val datas= data.collect().toBuffer
    var list= List[List[String]]()
    //解析jason数据
    for(i <- 0 until datas.length){
      val str: String = datas(i).toString
      val jsonparse: JSONObject = JSON.parseObject(str)
      //获得状态并过滤
      val status = jsonparse.getIntValue("status")
      if(status == 0) return ""
      //获得逆地理编码列表
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""
      //获取pois信息列表
      val poisArray = regeocodeJson.getJSONArray("pois")
      if(poisArray == null || poisArray.isEmpty) return null

      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()
      // 循环输出
      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      val list1: List[String] = buffer.toList
      list:+=list1
    }

    val res: Map[String, Int] = list.flatMap(x=>x)
      .filter(x => x != "[]")
      //遍历
      .map(x => (x, 1))
      //f分组
      .groupBy(_._1)
      //聚合
      .mapValues(_.size)
    res.foreach(println)

  }
}