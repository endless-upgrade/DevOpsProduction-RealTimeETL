package it.reply.data.pasquali

import it.reply.data.pasquali.engine.{DirectStreamer, ETL}
import it.reply.data.pasquali.model.TransformedDFs
import it.reply.data.pasquali.storage.Storage

object Stream {

  var toHive = true
  var onlyDebug = false

  def main(args: Array[String]): Unit = {

    /**

    USAGE: DirectStreamer  topicName smallest|largest [ [-h] | [--debug | --hive] ]
    normally it push kakfa streams to hdfs folders and hive table
    --debug  just print the stream
    --hive  store only to hive datalake

    -h      show this usage
      */

    args.foreach(println)

    if(args.size < 2){

      println("""  USAGE: DirectStreamer topicName smallest|largest [ [-h] | [--debug | --hive] ]\n
      normally it push kakfa streams to hdfs folders and hive table\n
      --test  just print the stream\n
      --hive  store only to hive datalake\n
      \n
      -h      show this usage""")
      return
    }

    if(args.size > 2)
    {
      if(args(2) == "-h")
      {
        println("""  USAGE: DirectStreamer topicName [ [-h] | [--debug | --hive] ]\n
        normally it push kakfa streams to hdfs folders and hive table\n
        --test  just print the stream\n
        --hive  store only to hive datalake\n
        \n
        -h      show this usage""")
        return
      }

      if(args(2) == "--hive")
      {
        toHive = true
        onlyDebug = false
      }

      if(args(2) == "--debug")
      {
        toHive = false
        onlyDebug = true
      }
    }

    //DEBUG MODE
    //onlyDebug = true
    //toHive = false

    val storage : Storage = Storage()
      .init("Real Time ETL", "local", true)
      .initKudu("cloudera-vm.c.endless-upgrade-187216.internal", "7051")


    val streamer : DirectStreamer = DirectStreamer()
      .initStreaming("Real Time ETL", "local", 10)
      .initKakfa("localhost", "9092", args(1), "group1", args(0))


    val spark = storage.spark
    val tableName = args(0).split("-")(2)

    val messages = streamer.createDirectStream()

    messages.foreachRDD(
      rdd =>
      {
        if(rdd.isEmpty){
          println("[ INFO ] Empty RDD")
        }
        else{
          val stringRDD = rdd.map(entry => entry._2)

          var dfs : TransformedDFs =
            ETL.transformRDD(stringRDD, spark, tableName)

          dfs.toHive.printSchema()
          dfs.toKudu.printSchema()

          if(onlyDebug)
          {
            dfs.toHive.show()
            dfs.toKudu.show()
          }
          else
          {
            println("\n[ INFO ] ====== Save To Hive Data Lake ======\n")
            storage.writeDFtoHive(dfs.toHive, "append", "datalake", tableName)
            println("\n[ INFO ] ====== Save To Kudu Data Mart ======\n")
            storage.insertKuduRows(dfs.toKudu, s"datamart.${tableName}")
          }
        }
      }
    )
  }

}
