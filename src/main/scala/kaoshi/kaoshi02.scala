package com.weekTest

import com.alibaba.fastjson.{JSON, JSONObject}
import kaoshi.TagType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  */
object kaoshi02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //获取数据
    val data: RDD[String] = sc.textFile("dir/json.txt")
    //收集数据
    val datas= data.collect().toBuffer
    var list= List[String]()
    //解析jason数据
    for(i <- 0 until datas.length){
      val str= datas(i).toString
      val jsonparse = JSON.parseObject(str)
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
          buffer.append(json.getString("type"))
        }
      }
      list:+=buffer.mkString(";")
    }

    list.foreach(println)
//    val res = list.flatMap(x => x.split(";"))
//      .map(x => ("type：" + x, 1))
//      .groupBy(_._1)
//      .mapValues(_.size)
//      .toList.sortWith(_._2>_._2)
//    res.foreach(println)
  }
}
