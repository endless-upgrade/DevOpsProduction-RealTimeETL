import it.reply.data.pasquali.engine.DirectStreamer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import sys.process._


class DirectStreamSpec extends FlatSpec with BeforeAndAfterAll{

  val testTopic = "test-topic"
  var ds : DirectStreamer = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    println("\n\n\nSTART CONFLUENT")

    var startConfluent = "sudo /opt/confluent-3.3.0/bin/confluent start schema-registry" !!

    println("-----------")
    println(startConfluent)
    println("-----------")

    if(startConfluent.contains("DONW"))
    {
      println("\n\n\nRETRY START CONFLUENT")
      startConfluent = "sudo /opt/confluent-3.3.0/bin/confluent start schema-registry" !!
    }

    if(startConfluent.contains("DONW"))
      fail("\n\n\nunable to start confluent")

    "echo {\\\"key\\\": \\\"value\\\"} | sudo /opt/confluent-3.3.0/bin/kafka-avro-console-producer --broker-list localhost:9092 --topic test-topic --property value.schema='{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"}]}'" !
  }

  override def afterAll(): Unit = {
    super.afterAll()

    if(ds != null)
      ds.ssc.stop()
  }

  "The Spark DirectStreamer" must
    "init spark conf and streming context" in {

    ds = DirectStreamer()
      .initStreaming("direct streaming test", "local", 10)

    assert(ds.sc != null)
    assert(ds.ssc != null)
  }

  it must "listen to a Kafka test-topic" in {

    val message = ds
      .initKakfa("localhost", "9092", "smallest", "group1", "test-topic")
      .createDebugDirectStream("", debugStream)


  }

  def debugStream(rdd : RDD[(String, String)],
                  spark : SparkSession,
                  tableName : String) : Unit = {

    if(rdd.isEmpty){
      println("[ INFO ] Empty RDD")
    }
    else{
      val stringRDD = rdd.map(entry => entry._2)
      assert(stringRDD.collect()(0) == "{\"key\": \"value\"}")
    }
  }



}
