package com.ETL

import com.utils.{SchemaUtils, Utils2Type, toJdbc}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 格式转换
  */
object Txt2Parquet {
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
    // 进行数据的读取，处理分析数据
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val lines = sc.textFile(inputPath)
    // 按要求切割，并且保证数据的长度大于等于85个字段，
    // 如果切割的时候遇到相同切割条件重复的情况下，需要切割的话，那么后面需要加上对应匹配参数
    // 这样切割才会准确 比如 ,,,,,,, 会当成一个字符切割 需要加上对应的匹配参数
//    lines.map(_.split(","))
    val data: RDD[Array[String]] = lines.map(_.split(",", -1)).filter(_.size >= 85)


    val rowRDD: RDD[Row] = data.map(arr => {
      Row(
        arr(0),
        Utils2Type.toInt(arr(1)),
        Utils2Type.toInt(arr(2)),
        Utils2Type.toInt(arr(3)),
        Utils2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        Utils2Type.toInt(arr(7)),
        Utils2Type.toInt(arr(8)),
        Utils2Type.toDouble(arr(9)),
        Utils2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        Utils2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        Utils2Type.toInt(arr(20)),
        Utils2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        Utils2Type.toInt(arr(26)),
        arr(27),
        Utils2Type.toInt(arr(28)),
        arr(29),
        Utils2Type.toInt(arr(30)),
        Utils2Type.toInt(arr(31)),
        Utils2Type.toInt(arr(32)),
        arr(33),
        Utils2Type.toInt(arr(34)),
        Utils2Type.toInt(arr(35)),
        Utils2Type.toInt(arr(36)),
        arr(37),
        Utils2Type.toInt(arr(38)),
        Utils2Type.toInt(arr(39)),
        Utils2Type.toDouble(arr(40)),
        Utils2Type.toDouble(arr(41)),
        Utils2Type.toInt(arr(42)),
        arr(43),
        Utils2Type.toDouble(arr(44)),
        Utils2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        Utils2Type.toInt(arr(57)),
        Utils2Type.toDouble(arr(58)),
        Utils2Type.toInt(arr(59)),
        Utils2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        Utils2Type.toInt(arr(73)),
        Utils2Type.toDouble(arr(74)),
        Utils2Type.toDouble(arr(75)),
        Utils2Type.toDouble(arr(76)),
        Utils2Type.toDouble(arr(77)),
        Utils2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        Utils2Type.toInt(arr(84))
      )
    })

    // 构建DF
    val df: DataFrame = sQLContext.createDataFrame(rowRDD, SchemaUtils.structType)
    //保存数据
    df.write.parquet(outputPath)

//    df.createOrReplaceTempView("Ad")
//    val sql: DataFrame = spark.sql("select count(1) ct,provincename,cityname from Ad group by provincename,cityname order by ct desc")
//    sql.show()
//    sql.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)

//    将结果写入到mysql
//    val pro = toJdbc.getProperties()
//    js.write.mode(SaveMode.Append).jdbc(pro._1,"city",pro._2)

    //写入到分区hdfs
//    sql.toJSON.coalesce(1).write.partitionBy("provincename","cityname").save("hdfs://cdh:9000/ad")
    //通过反射生成df
    val rowRDD2: RDD[Ad] = data.map(arr =>
      Ad(
        arr(0),
        Utils2Type.toInt(arr(1)),
        Utils2Type.toInt(arr(2)),
        Utils2Type.toInt(arr(3)),
        Utils2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        Utils2Type.toInt(arr(7)),
        Utils2Type.toInt(arr(8)),
        Utils2Type.toDouble(arr(9)),
        Utils2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        Utils2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        Utils2Type.toInt(arr(20)),
        Utils2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        Utils2Type.toInt(arr(26)),
        arr(27),
        Utils2Type.toInt(arr(28)),
        arr(29),
        Utils2Type.toInt(arr(30)),
        Utils2Type.toInt(arr(31)),
        Utils2Type.toInt(arr(32)),
        arr(33),
        Utils2Type.toInt(arr(34)),
        Utils2Type.toInt(arr(35)),
        Utils2Type.toInt(arr(36)),
        arr(37),
        Utils2Type.toInt(arr(38)),
        Utils2Type.toInt(arr(39)),
        Utils2Type.toDouble(arr(40)),
        Utils2Type.toDouble(arr(41)),
        Utils2Type.toInt(arr(42)),
        arr(43),
        Utils2Type.toDouble(arr(44)),
        Utils2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        Utils2Type.toInt(arr(57)),
        Utils2Type.toDouble(arr(58)),
        Utils2Type.toInt(arr(59)),
        Utils2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        Utils2Type.toInt(arr(73)),
        Utils2Type.toDouble(arr(74)),
        Utils2Type.toDouble(arr(75)),
        Utils2Type.toDouble(arr(76)),
        Utils2Type.toDouble(arr(77)),
        Utils2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        Utils2Type.toInt(arr(84))
      ))
//    val tup= rowRDD2.map(x => {
//      ((x.provincename, x.cityname), 1)
//    }).reduceByKey(_+_).groupBy(_._1)
//
//    println(tup.take(5))


    val res2: DataFrame = rowRDD2.toDF()
//    res2.show()

    sc.stop()
    spark.stop()
  }
}

case class Ad(
               sessionid: String,
               advertisersid: Int,
               adorderid: Int,
               adcreativeid: Int,
               adplatformproviderid: Int,
               sdkversion: String,
               adplatformkey: String,
               putinmodeltype: Int,
               requestmode: Int,
               adprice: Double,
               adppprice: Double,
               requestdate: String,
               ip: String,
               appid: String,
               appname: String,
               uuid: String,
               device: String,
               client: Int,
               osversion: String,
               density: String,
               pw: Int,
               ph: Int,
               `/long`: String,
               lat: String,
               provincename: String,
               cityname: String,
               ispid: Int,
               ispname: String,
               networkmannerid: Int,
               networkmannername: String,
               iseffective: Int,
               isbilling: Int,
               adspacetype: Int,
               adspacetypename: String,
               devicetype: Int,
               processnode: Int,
               apptype: Int,
               district: String,
               paymode: Int,
               isbid: Int,
               bidprice: Double,
               winprice: Double,
               iswin: Int,
               cur: String,
               rate: Double,
               cnywinprice: Double,
               imei: String,
               mac: String,
               idfa: String,
               openudid: String,
               androidid: String,
               rtbprovince: String,
               rtbcity: String,
               rtbdistrict: String,
               rtbstreet: String,
               storeurl: String,
               realip: String,
               isqualityapp: Int,
               bidfloor: Double,
               aw: Int,
               ah: Int,
               imeimd5: String,
               macmd5: String,
               idfamd5: String,
               openudidmd5: String,
               androididmd5: String,
               imeisha1: String,
               macsha1: String,
               idfasha1: String,
               openudidsha1: String,
               androididsha1: String,
               uuidunknow: String,
               userid: String,
               iptype: Int,
               initbidprice: Double,
               adpayment: Double,
               agentrate: Double,
               lomarkrate: Double,
               adxrate: Double,
               title: String,
               keywords: String,
               tagid: String,
               callbackdate: String,
               channelid: String,
               mediatype: Int
             )