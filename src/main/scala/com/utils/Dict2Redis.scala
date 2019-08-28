package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/**
  * 将字典集数据清洗后存入redis数据库
  * */
object Dict2Redis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //读取数据
    //获取字典集 ,进行数据的读取，处理分析数据
    val lines: RDD[String] = sc.textFile("F:\\Big-Data-22\\项目\\Spark用户画像分析\\app_dict.txt")
    val id_Name: RDD[(String, String)] = lines.map(_.split("\t", -1))
      .filter(_.length > 5).map(x => {
      (x(4), x(1)) //name,id
    })
    id_Name.foreachPartition(map => {
      map.foreach(x => {
        val jedis = JedisPool.getConnections()
        val k = x._1
        val v = x._2
        //写入
        jedis.set(k, v)
//        jedis.del(k)
        //关闭连接
        JedisPool.close(jedis)
      })
    })
    sc.stop()
  }
}
