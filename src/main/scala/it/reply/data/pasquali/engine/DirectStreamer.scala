package it.reply.data.pasquali.engine

import java.io.File

import _root_.kafka.serializer._
import com.typesafe.config.ConfigFactory
import it.reply.data.pasquali.Storage
import it.reply.data.pasquali.model.TransformedDFs
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

case class DirectStreamer(configFile : String){

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

  var KUDU_DATABASE = ""
  var HIVE_DATABASE = ""


  def initStreaming(appName : String, master : String, fetchIntervalSec : Int) : DirectStreamer = {
    conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = SparkContext.getOrCreate(conf)
    ssc = new StreamingContext(sc, Seconds(fetchIntervalSec))
    ssc.checkpoint("checkpoint")

    //val config = ConfigFactory.load("RealTimeETL")

    val config = ConfigFactory.parseFile(new File(configFile))

    KUDU_DATABASE = config.getString("rtetl.kudu.database")
    HIVE_DATABASE = config.getString("rtetl.hive.database")

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


  def createDirectStream(tableName : String, storage : Storage) : Unit ={

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    messages.foreachRDD(
      rdd =>
        {
          if(rdd.isEmpty){
            println("[ INFO ] Empty RDD")
          }
          else{
            val stringRDD = rdd.map(entry => entry._2)

            val dfs : TransformedDFs =
              ETL.transformRDD(stringRDD, spark, tableName)

            dfs.toHive.printSchema()
            dfs.toKudu.printSchema()

            println("\n[ INFO ] ====== Save To Hive Data Lake ======\n")
            storage.writeDFtoHive(dfs.toHive, "append", HIVE_DATABASE, tableName)
            println("\n[ INFO ] ====== Save To Kudu Data Mart ======\n")
            storage.insertKuduRows(dfs.toKudu, s"${KUDU_DATABASE}.${tableName}")

          }
        }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  def createDebugDirectStream(tableName : String) : Unit ={

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    messages.foreachRDD(
      rdd =>
      {
        if(rdd.isEmpty){
          println("[ INFO ] Empty RDD")
        }
        else{
          val stringRDD = rdd.map(entry => entry._2)

          val dfs : TransformedDFs =
            ETL.transformRDD(stringRDD, spark, tableName)

          dfs.toHive.printSchema()
          dfs.toKudu.printSchema()
          dfs.toHive.show()
          dfs.toKudu.show()
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
