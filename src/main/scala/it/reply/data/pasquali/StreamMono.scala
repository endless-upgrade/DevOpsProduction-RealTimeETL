package it.reply.data.pasquali

import com.typesafe.config.ConfigFactory
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import it.reply.data.pasquali.engine.ETL
import it.reply.data.pasquali.model.TransformedDFs
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamMono {

  var toHive = true
  var onlyDebug = false

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
    //CONF_DIR = scala.util.Properties.envOrElse("DEVOPS_CONF_DIR", "conf")

    val CONF_DIR = "conf"

    println("\n")
    println(CONF_DIR)
    println("\n")

    //val configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/${CONFIG_FILE}"))
    val configuration = ConfigFactory.load()

    val KAFKA_BOOTSTRAP_ADDR  = configuration.getString("rtetl.kafka.bootstrap.address")
    val KAFKA_BOOTSTRAP_PORT = configuration.getString("rtetl.kafka.bootstrap.port")
    val KAFKA_GROUP = configuration.getString("rtetl.kafka.group")

    val KUDU_ADDR = configuration.getString("rtetl.kudu.address")
    val KUDU_PORT = configuration.getString("rtetl.kudu.port")

    val SPARK_APPNAME = configuration.getString("rtetl.spark.app_name")
    val SPARK_MASTER = configuration.getString("rtetl.spark.master")

    val KUDU_DATABASE = configuration.getString("rtetl.kudu.database")
    val HIVE_DATABASE = configuration.getString("rtetl.hive.database")

    val KUDU_TABLE_BASE = configuration.getString("rtetl.kudu.table_base")

    println("Configurations")
    println(s"APP_NAME = $SPARK_APPNAME")
    println(s"MASTER = $SPARK_MASTER")


    //*************************************************************************
    // Metrics

    val ENV = configuration.getString("rtetl.metrics.environment")
    val JOB_NAME = configuration.getString("rtetl.metrics.job_name")

    val GATEWAY_ADDR = configuration.getString("rtetl.metrics.gateway.address")
    val GATEWAY_PORT = configuration.getString("rtetl.metrics.gateway.port")

    //*************************************************************************

    storage = Storage()
      .init(SPARK_MASTER, SPARK_MASTER, true)
      .initKudu(KUDU_ADDR, KUDU_PORT, KUDU_TABLE_BASE)

    initStreaming(SPARK_APPNAME, SPARK_MASTER, 10, KAFKA_BOOTSTRAP_ADDR, KAFKA_BOOTSTRAP_PORT, args(1), KAFKA_GROUP, args(0))

    val spark = storage.spark
    val tableName = args(0).split("-")(2)

    //*******************************************************************************


    val LABEL_NUMBER_OF_NEW = s"${ENV}_${configuration.getString("rtetl.metrics.labels.number_of_new")}_$tableName"
    val LABEL_HIVE_NUMBER = s"${ENV}_${tableName}_${configuration.getString("rtetl.metrics.labels.hive_number")}"
    val LABEL_KUDU_NUMBER = s"${ENV}_${tableName}_${configuration.getString("rtetl.metrics.labels.kudu_number")}"
    val LABEL_PROCESS_DURATION = s"${ENV}_${tableName}_${configuration.getString("rtetl.metrics.labels.process_duration")}"

    val pushGateway : PushGateway = new PushGateway(s"$GATEWAY_ADDR:$GATEWAY_PORT")
    val registry = new CollectorRegistry

    val gaugeNewElementsNumber = Gauge.build().name(LABEL_NUMBER_OF_NEW)
      .help(s"Number of new $tableName extract from topic").register(registry)

    val gaugeHiveNumber = Gauge.build().name(LABEL_HIVE_NUMBER)
      .help(s"Number of $tableName in hive datalake").register(registry)

    val gaugeKuduNumber = Gauge.build().name(LABEL_KUDU_NUMBER)
      .help(s"Number of $tableName in kudu datamart").register(registry)

    val gaugeDuration = Gauge.build().name(LABEL_PROCESS_DURATION)
      .help(s"Duration of the single elaboration").register(registry)

    //*******************************************************************************

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    messages.foreachRDD(
      rdd =>
      {
        if(rdd.isEmpty){
          println("[ INFO ] Empty RDD")
        }
        else{

          val timer = gaugeDuration.startTimer()

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

          timer.setDuration()

          gaugeNewElementsNumber.set(rdd.count())
          gaugeHiveNumber.set(storage.readHiveTable(s"${HIVE_DATABASE}.${tableName}").count())
          gaugeKuduNumber.set(storage.readKuduTable(s"${KUDU_DATABASE}.${tableName}").count())

          pushGateway.push(registry, JOB_NAME)

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
