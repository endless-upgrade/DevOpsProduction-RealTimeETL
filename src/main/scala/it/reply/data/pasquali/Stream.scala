package it.reply.data.pasquali

import java.io.File

import com.typesafe.config.ConfigFactory
import it.reply.data.pasquali.engine.DirectStreamer
import it.reply.data.pasquali.storage.Storage

object Stream {

  var toHive = true
  var onlyDebug = false

  var SPARK_APPNAME = ""
  var SPARK_MASTER = ""

  var KAFKA_BOOTSTRAP_ADDR = ""
  var KAFKA_BOOTSTRAP_PORT = ""
  var KAFKA_GROUP = ""

  var KUDU_ADDR = ""
  var KUDU_PORT = ""

  var CONF_DIR = ""
  var CONFIG_FILE = "RealTimeETL.conf"

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

    println("Configurations")
    println(s"APP_NAME = $SPARK_APPNAME")
    println(s"MASTER = $SPARK_MASTER")


    val storage : Storage = Storage()
      .init(SPARK_MASTER, SPARK_MASTER, true)
      .initKudu(KUDU_ADDR, KUDU_PORT)

    val streamer: DirectStreamer = DirectStreamer(s"${CONF_DIR}/${CONFIG_FILE}")
      .initStreaming(SPARK_APPNAME, SPARK_MASTER, 10)
      .initKakfa(KAFKA_BOOTSTRAP_ADDR, KAFKA_BOOTSTRAP_PORT, args(1), KAFKA_GROUP, args(0))

    val spark = storage.spark
    val tableName = args(0).split("-")(2)

    if (onlyDebug) {
      streamer.createDebugDirectStream(tableName)
    }
    else {
      streamer.createDirectStream(tableName, storage)
    }

  }
}
