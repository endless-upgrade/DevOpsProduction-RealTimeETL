package it.reply.data.pasquali.engine

import _root_.kafka.serializer._
import it.reply.data.pasquali.storage.Storage
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._

case class DirectStreamer(){


  val KUDU_MASTER = "cloudera-vm.c.endless-upgrade-187216.internal:7051"
  val KUDU_TABLE_BASE = "impala::datamart."


/*
  // TODO In the next update
  // For now will be do by reccomender.

  val CLOUDERA_KUDU_TABLE_USER_RATINGS = "impala::datamart.user_ratings"
  val CLOUDERA_KUDU_TABLE_MOVIE_RATED = "impala::datamart.movie_rated"
*/


  var conf : SparkConf = null
  var sc : SparkContext = null
  var ssc : StreamingContext = null
  var spark : SparkSession = null

  var storage : Storage = null

  var topics : Set[String] = Set()

  var kafkaParams : Map[String, String] = null


  def initStreaming(appName : String, master : String, fetchIntervalSec : Int) : DirectStreamer = {
    conf = new SparkConf().setMaster(master).setAppName(appName)
    ssc = new StreamingContext(conf, Seconds(fetchIntervalSec))
    ssc.checkpoint("checkpoint")
    this
  }

  def initKakfa(bootstrapServer : String, bootstrapPort : String,
                offset : String, groupID : String, singleTopic : String) : DirectStreamer = {

    initKakfa(bootstrapServer, bootstrapPort, offset, groupID, Set[String](singleTopic))
  }

  def initKakfa(bootstrapServer : String, bootstrapPort : String,
                offset : String, groupID : String, topics : Set[String]) : DirectStreamer = {

    this.topics = topics

    /*
    bootstrap.servers -> "localhost:9092"
    group.id -> "group1"
    */

    kafkaParams = Map[String, String](
      "bootstrap.servers" -> s"${bootstrapServer}:${bootstrapPort}",
      "auto.offset.reset" -> offset,
      "group.id" -> groupID)

    this
  }


  def createDirectStream() : InputDStream[(String, String)] ={

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    ssc.start()
    ssc.awaitTermination()

    messages
  }


/*
  def storeToHive(spark : SparkSession, table : String, dataFrame: DataFrame) = {

    //val mapped = rdd.map(raw => (IDGen.next, raw))
    //val df = spark.createDataFrame(mapped).toDF("id", "raw")
    dataFrame.registerTempTable("temptable")
    spark.sql("insert into table data_reply_db.datalake select * from temptable")

  }


  def initIDGenerator(spark : SparkSession, table : String) = {

    //spark.sql("insert into data_reply_db.datalake values (0, 'init')")
    val start = spark.sql(s"select * from ${table}").count()
    IDGen.init(start)


    try
    {
      val start = spark.sql(s"select * from ${table}").count()
      IDGen.init(start)
    }
    catch{

      case e : Exception => IDGen.init(0)

    }

  }

  object IDGen{

    var start : Long= 0;

    def init(s : Long) = {
      start = s
    }

    def next : Long = {

      val index = start
      start += 1
      index
    }

  }
*/
}
