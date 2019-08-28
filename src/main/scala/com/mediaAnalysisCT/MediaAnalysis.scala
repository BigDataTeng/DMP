package com.mediaAnalysisCT


import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode, SparkSession}

/**
  * 3.2.3 需求指标 媒体分析
  */
object MediaAnalysis {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if (args.length != 2) {
      println("参数目录不正确,退出程序!")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // 读取本地文件
    val df = sQLContext.read.parquet(inputPath)
    //获取字典集 ,进行数据的读取，处理分析数据
    val lines: RDD[String] = sc.textFile("F:\\Big-Data-22\\项目\\资料\\Spark用户画像分析\\app_dict.txt")
    val id_Name = lines.map(_.split("\t"))
      .filter(_.length>5).map(x => {
      (x(4), x(1))//id,name
    }).collectAsMap()
    //封装广播变量
    val idAndName = sc.broadcast(id_Name)
    val res= df.map(row => {
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
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
      // 创建三个对应的方法处理指标
      val resList = RptUtils.request(requestmode, processnode)
      val clickList = RptUtils.click(requestmode, iseffective)
      val adList = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      var name=""
      if(appname=="其他"){
        name=idAndName.value.getOrElse(appid,"无名")
      }else{
        name=appname
      }
      (name, resList ++ clickList ++ adList)
    }).rdd.reduceByKey((x, y) => x.zip(y).map(x => x._1 + x._2))
      .map(x=>{
        (x._1,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8))
      })
    //将结果写入mysql
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    val result = res.toDF("媒体类别",
      "原始请求",
      "有效请求",
      "广告请求数",
      "参与竞价数",
      "竞价成功数",
      "展示数",
      "点击数",
      "广告成本",
      "广告消费")
    result.show()
    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"MediaAnalysis",prop)

    sc.stop()
    spark.stop()
    //    val idAndNameV = idAndName.value
    //    val res = otherRDD.join(idAndNameV,"appname")
    //    res.collect().foreach(println)
    //
    //    otherRDD.mapPartitions(x=>{
    //      for ((k,v)<-x if(idAndNameV.equals(k)))
    //        yield (k,idAndNameV.get(k).getOrElse("其他"),v._2)
    //    }).foreach(println)


    /* df.createOrReplaceTempView("log")
     // 指标统计
     val res = sQLContext.sql("select " +
       "case when appname='爱奇艺' then '爱奇艺' end as `媒体类别`," +
       "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end)as `总请求`," +
       "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end)as `广告请求`," +
       "sum(case when requestmode=1 and processnode=3 then 1 else 0 end)as `参与竞价数`," +
       "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end)as `竞价成功率`," +
       "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid<>0 then 1 else 0 end)as `展示量`," +
       "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end)as `点击量`," +
       "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end)as `点击率`," +
       "sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end)as `广告成本`," +
       "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end)as `广告消费` " +
       "from log group by  appname").show()*/
  }
}
