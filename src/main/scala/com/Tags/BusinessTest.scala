package com.Tags

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 商圈存入到redis测试
  */
object BusinessTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.parquet("F:\\Big-Data-22\\项目一\\out-parquet")
    import spark.implicits._
    val res: Dataset[List[(String, Int)]] = df.map(row => {
      val business = TagBusiness.makeTags(row)
      business
    })
    res.rdd.foreach(println)
  }
}
