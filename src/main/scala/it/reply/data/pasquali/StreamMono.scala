package it.reply.data.pasquali

import java.io.File

import com.typesafe.config.ConfigFactory
import it.reply.data.pasquali.engine.{DirectStreamer, ETL}
import it.reply.data.pasquali.model.TransformedDFs
import it.reply.data.pasquali.storage.Storage
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamMono {

  var toHive = true
  var onlyDebug = false

  var SPARK_APPNAME = ""
  var SPARK_MASTER = ""

  var KAFKA_BOOTSTRAP_ADDR = ""
  var KAFKA_BOOTSTRAP_PORT = ""
  var KAFKA_GROUP = ""

  var KUDU_ADDR = ""
  var KUDU_PORT = ""
  var KUDU_TABLE_BASE = ""

  var CONF_DIR = ""
  var CONFIG_FILE = "RealTimeETL.conf"

  var KUDU_DATABASE = ""
  var HIVE_DATABASE = ""

  // ********************************************************

  var conf : SparkConf = null
  var sc : SparkContext = null
  var ssc : StreamingContext = null
  var spark : SparkSession = null

  var storage : Storage = null

  var topics : Set[String] = Set()

  var kafkaParams : Map[String, String] = null

  def main(args: Array[String]): Unit = {

    /**
      **
      *USAGE: DirectStreamer  topicName smallest|largest [ [-h] | [--debug | --hive] ]
      * normally it push kakfa streams to hdfs folders and hive table
      * --debug  just print the stream
      * --hive  store only to hive datalake
      **
      *-h      show this usage
      */

    args.foreach(println)

    if (args.size < 2) {

      println(
        """  USAGE: DirectStreamer topicName smallest|largest [ [-h] | [--debug | --hive] ]\n
      normally it push kakfa streams to hdfs folders and hive table\n
      --debug  just print the stream\n
      --hive  store only to hive datalake\n
      \n
      -h      show this usage""")
      return
    }

    if (args.size > 2) {
      if (args(2) == "-h") {
        println(
          """  USAGE: DirectStreamer topicName smallest|largest [ [-h] | [--debug | --hive] ]\n
        normally it push kakfa streams to hdfs folders and hive table\n
        --debug  just print the stream\n
        --hive  store only to hive datalake\n
        \n
        -h      show this usage""")
        return
      }

      if (args(2) == "--hive") {
        toHive = true
        onlyDebug = false
      }

      if (args(2) == "--debug") {
        toHive = false
        onlyDebug = true
      }
    }

    //DEBUG MODE
    //onlyDebug = true
    //toHive = false


    //val configuration = ConfigFactory.load("BatchETL")
    CONF_DIR = scala.util.Properties.envOrElse("DEVOPS_CONF_DIR", "conf")

    println("\n")
    println(CONF_DIR)
    println("\n")

    val configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/${CONFIG_FILE}"))

    KAFKA_BOOTSTRAP_ADDR  = configuration.getString("rtetl.kafka.bootstrap.address")
    KAFKA_BOOTSTRAP_PORT = configuration.getString("rtetl.kafka.bootstrap.port")
    KAFKA_GROUP = configuration.getString("rtetl.kafka.group")

    KUDU_ADDR = configuration.getString("rtetl.kudu.address")
    KUDU_PORT = configuration.getString("rtetl.kudu.port")

    SPARK_APPNAME = configuration.getString("rtetl.spark.app_name")
    SPARK_MASTER = configuration.getString("rtetl.spark.master")

    KUDU_DATABASE = configuration.getString("rtetl.kudu.database")
    HIVE_DATABASE = configuration.getString("rtetl.hive.database")

    KUDU_TABLE_BASE = configuration.getString("rtetl.kudu.table_base")

    println("Configurations")
    println(s"APP_NAME = $SPARK_APPNAME")
    println(s"MASTER = $SPARK_MASTER")

    storage = Storage()
      .init(SPARK_MASTER, SPARK_MASTER, true)
      .initKudu(KUDU_ADDR, KUDU_PORT, KUDU_TABLE_BASE)

    initStreaming(SPARK_APPNAME, SPARK_MASTER, 10, KAFKA_BOOTSTRAP_ADDR, KAFKA_BOOTSTRAP_PORT, args(1), KAFKA_GROUP, args(0))

    val streamer: DirectStreamer = DirectStreamer(s"${CONF_DIR}/${CONFIG_FILE}")
      .initStreaming(SPARK_APPNAME, SPARK_MASTER, 10)
      .initKakfa(KAFKA_BOOTSTRAP_ADDR, KAFKA_BOOTSTRAP_PORT, args(1), KAFKA_GROUP, args(0))

    val spark = storage.spark
    val tableName = args(0).split("-")(2)


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
          val dfs : TransformedDFs = ETL.transformRDD(stringRDD, spark, tableName)

          dfs.toHive.printSchema()
          dfs.toKudu.printSchema()

          if(onlyDebug){
            dfs.toHive.show()
            dfs.toKudu.show()
          }
          else{
            println("\n[ INFO ] ====== Save To Hive Data Lake ======\n")
            storage.writeDFtoHive(dfs.toHive, "append", HIVE_DATABASE, tableName)
            println("\n[ INFO ] ====== Save To Kudu Data Mart ======\n")
            storage.upsertKuduRows(dfs.toKudu, s"${KUDU_DATABASE}.${tableName}")
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

  def initStreaming(appName : String,
                    master : String,
                    fetchIntervalSec : Int,
                    bootstrapServer : String,
                    bootstrapPort : String,
                    offset : String,
                    groupID : String,
                    singleTopic : String): Unit ={

    conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = SparkContext.getOrCreate(conf)
    ssc = new StreamingContext(sc, Seconds(fetchIntervalSec))
    ssc.checkpoint("checkpoint")


    this.topics = Set[String](singleTopic)

    /*
    bootstrap.servers -> "localhost:9092"
    group.id -> "group1"
    */

    kafkaParams = Map[String, String](
      "bootstrap.servers" -> s"${bootstrapServer}:${bootstrapPort}",
      "auto.offset.reset" -> offset,
      "group.id" -> groupID)
  }

}
