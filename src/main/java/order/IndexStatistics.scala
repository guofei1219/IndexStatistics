package order
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.DataUtil
object IndexStatistics{

  def main(args: Array[String]): Unit = {
    val confspark = new SparkConf().setAppName("IndexStatistics").setMaster("local[2]")
    val ssc = new StreamingContext(confspark, Seconds(2))
    ssc.checkpoint("C:\\work\\checkpoint")

    val topics = Set("canal")
    //本地虚拟机ZK地址
    val brokers = "rmhadoop01:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
     "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    // Create a direct stream
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val resut_lines = lines.flatMap(line => {
      val key = line._1
      val name = line._2.split("\t")(0)
      val click = line._2.split("\t")(1).toLong
      System.out.println(name +" : " + click)
      Some(name,click)

    })

    val tmp = resut_lines.updateStateByKey(updateRunningSum _)

    val res3 = tmp.foreachRDD(rdd =>{
      rdd.foreachPartition(rdd_partition =>{
        rdd_partition.foreach(data=>{
          if(!data.toString.isEmpty) {
            System.out.println("点击次数"+" : "+data._2)
            DataUtil.toMySQL(data._1.toString,data._2.toInt)
          }
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }


  def updateRunningSum(values: Seq[Long], state: Option[Long]) = {
    /*
      state:存放的历史数据
      values:当前批次汇总值
     */
    Some(state.getOrElse(0L)+values.sum)
  }

}
