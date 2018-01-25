import java.io.File

import com.typesafe.config.ConfigFactory
import io.prometheus.client.exporter.PushGateway
import io.prometheus.client.{CollectorRegistry, Gauge}
import it.reply.data.pasquali.engine.ETL
import it.reply.data.pasquali.utils.MetricPoster
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class ETLSpec extends FlatSpec with BeforeAndAfterAll{

  var CONF_DIR = ""
  var CONFIG_FILE = "RealTimeETL_staging.conf"

  var tagSample = """
      {"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"int32","optional":true,"field":"userid"},{"type":"int32","optional":true,"field":"movieid"},{"type":"string","optional":true,"field":"tag"},{"type":"string","optional":true,"field":"timestamp"}],"optional":false,"name":"tags"},"payload":{"id":5120,"userid":1741,"movieid":246,"tag":"setting:Chicago","timestamp":"1186434000"}}
    """.stripMargin

  var ratingSample = """
      {"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"int32","optional":true,"field":"userid"},{"type":"int32","optional":true,"field":"movieid"},{"type":"double","optional":true,"field":"rating"},{"type":"string","optional":true,"field":"timestamp"}],"optional":false,"name":"ratings"},"payload":{"id":39478,"userid":153,"movieid":508,"rating":4.5,"timestamp":"1101142930"}}
    """.stripMargin

  var sc : SparkContext = null
  var spark : SparkSession = null

  var LABEL_NUMBER_OF_NEW = ""
  var LABEL_HIVE_NUMBER = ""
  var LABEL_KUDU_NUMBER = ""
  var LABEL_PROCESS_DURATION =  ""

  var ENV = ""
  var JOB_NAME = ""
  var GATEWAY_ADDR = ""
  var GATEWAY_PORT = ""

  var gaugeRatingsDuration : Gauge = null
  var gaugeTagsDuration : Gauge = null
  val registry = new CollectorRegistry
  var pushGateway : PushGateway = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    //val configuration = ConfigFactory.load("RealTimeETL_staging")

    CONF_DIR = "conf"
    val configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/${CONFIG_FILE}"))

    val SPARK_APPNAME = configuration.getString("rtetl.spark.app_name")
    val SPARK_MASTER = configuration.getString("rtetl.spark.master")

    val conf = new SparkConf()
      .setAppName(SPARK_APPNAME)
      .setMaster(SPARK_MASTER)

    sc = SparkContext.getOrCreate(conf)

    spark = SparkSession.builder()
      .appName("Real Time ETL test")
      .master("local").getOrCreate()

    ENV = configuration.getString("rtetl.metrics.environment")
    JOB_NAME = configuration.getString("rtetl.metrics.job_name")
    GATEWAY_ADDR = configuration.getString("rtetl.metrics.gateway.address")
    GATEWAY_PORT = configuration.getString("rtetl.metrics.gateway.port")
    LABEL_PROCESS_DURATION = configuration.getString("rtetl.metrics.labels.process_duration")

    pushGateway = new PushGateway(s"$GATEWAY_ADDR:$GATEWAY_PORT")

    gaugeRatingsDuration = Gauge.build().name(s"ratings_${LABEL_PROCESS_DURATION}")
      .help("Duration of the transformation process for ratings").register(registry)

    gaugeTagsDuration = Gauge.build().name(s"tags_${LABEL_PROCESS_DURATION}")
      .help("Duration of the transformation process for tags").register(registry)

  }

  override def afterAll(): Unit = {
    super.afterAll()

    sc.stop()
    spark.stop()
  }

  "The real time etl process" should
    "start from a nested json and transform it in a plain dataframe" in {

    val sampleRDD = sc.parallelize(Seq(tagSample))
    val plainDFs = ETL.transformRDD(sampleRDD, spark, "tags")

    assert(plainDFs.toKudu != null)
    assert(plainDFs.toKudu.count() == 1)
    assert(plainDFs.toHive != null)
    assert(plainDFs.toHive.count() == 1)
  }

  it should "take a generic json Tag and generate the Tag table entry" in {

    val timer = gaugeTagsDuration.startTimer()

    val sampleRDD = sc.parallelize(Seq(tagSample))
    val tagsEntry = ETL.transformRDD(sampleRDD, spark, "tags")

    timer.setDuration()

    try{
      pushGateway.pushAdd(registry, s"${JOB_NAME}_${ENV}")
    }catch{
      case _ : Exception => println("Unable to reach Metric PushGateway")
    }

    //{"id":5120,"userid":1741,"movieid":246,"tag":"setting:Chicago","timestamp":"1186434000"}

    tagsEntry.toHive.show()

    val hive = tagsEntry.toHive.collect()(0)
    assert(hive(0) == 5120)
    assert(hive(1) == 1741)
    assert(hive(2) == 246)
    assert(hive(3) == "setting:Chicago")
    assert(hive(4) == "1186434000")

    val kudu = tagsEntry.toKudu.collect()(0)
    assert(kudu(0) == 1741)
    assert(kudu(1) == 246)
    assert(kudu(2) == "setting:Chicago")
    assert(kudu(3) == "1186434000")
  }

  it should "take a generic json Rating and generate the rating table entry" in {

    val timer = gaugeRatingsDuration.startTimer()

    val sampleRDD = sc.parallelize(Seq(ratingSample))
    val ratingsEntry = ETL.transformRDD(sampleRDD, spark, "ratings")

    timer.setDuration()
    try{
      pushGateway.pushAdd(registry, s"${JOB_NAME}_${ENV}")
    }catch{
      case _ : Exception => println("Unable to reach Metric PushGateway")
    }

    //{"id":39478,"userid":153,"movieid":508,"rating":4.5,"timestamp":"1101142930"}

    ratingsEntry.toHive.show()

    val hive = ratingsEntry.toHive.collect()(0)
    assert(hive(0) == 39478)
    assert(hive(1) == 153)
    assert(hive(2) == 508)
    assert(hive(3) == 4.5)
    assert(hive(4) == "1101142930")

    val kudu = ratingsEntry.toKudu.collect()(0)
    assert(kudu(0) == 153)
    assert(kudu(1) == 508)
    assert(kudu(2) == 4.5)
    assert(kudu(3) == "1101142930")
  }




}
