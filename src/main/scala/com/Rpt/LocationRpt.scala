package com.Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 3.2.1需求指标三 地域分布指标
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 获取数据
    val df = sQLContext.read.parquet(inputPath)
//    //注册临时表
//    df.registerTempTable("log")
    //使用sql的方式实现地域分布
//    val res = sQLContext.sql("select provincename,cityname," +
//      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end)as originalrequestNum," +
//      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end)as validrequestNum," +
//      "sum(case when requestmode=1 and processnode=3 then 1 else 0 end)as adNum," +
//      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end)as bidNum," +
//      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid<>0 then 1 else 0 end)as winNum," +
//      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end)as showNum," +
//      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end)as clickNum," +
//      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end)as winprice," +
//      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end)as adpayments " +
//      "from log group by  provincename,cityname")
    //将结果写入mysql
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
//    res.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"zone",prop)
    import spark.implicits._
    val res = df.map(row => {
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标
      val resList = RptUtils.request(requestmode, processnode)
      val clickList = RptUtils.click(requestmode, iseffective)
      val adList = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      ((pro, city), resList ++ clickList ++ adList)
    }).rdd.reduceByKey((x, y) => x.zip(y).map(x => x._1 + x._2))
      .map(x=>{
        (x._1._1,x._1._2,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8))
      })
//      .map(x => (x._1 + "," + x._2.mkString(",")))
    //将结果写入mysql
    res.toDF("省",
      "市",
      "原始请求",
      "有效请求",
      "广告请求",
      "参与竞价数",
      "竞价成功率",
      "展示量",
      "点击量",
      "广告成本",
      "广告消费"
    ).write.mode(SaveMode.Append)
      .jdbc(load.getString("jdbc.url"),"LocationRpt",prop)
    sc.stop()
    spark.stop()
  }
}