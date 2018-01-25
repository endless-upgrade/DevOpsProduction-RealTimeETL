package it.reply.data.pasquali.utils

import scala.collection.mutable
import scalaj.http.{Http, HttpOptions, HttpResponse}

object MetricPoster {

  var timerMap : mutable.HashMap[String, Long] = mutable.HashMap[String, Long]()
  var counterMap : mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()

  def postMultipleMetrics(metricTypes : Array[String],
                          gateway : String,
                          labels : Array[String],
                          helps : Array[String],
                          values : Array[Double],
                          job : String,
                          instance : String) : HttpResponse[String] = {


    var body = ""

    for ((label, i) <- labels.zipWithIndex){
      body += s"# TYPE $label ${metricTypes(i)}\n"
      body += s"# HELP $label ${helps(i)}.\n"
      body += s"$label ${values(i)}\n"
    }


    val index = s"metrics/job/$job/instance/$instance"

    Http(s"http://$gateway/$index")
      .put(body)
      .header("Content-Type", "data-binary")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000)).asString
  }


  def postMetric(metricType : String,
                 gateway : String,
                 label : String,
                 help : String,
                 value : Double,
                 job : String,
                 instance : String) : HttpResponse[String] = {


    var body = s"# TYPE $label $metricType\n"
    body += s"# HELP $label $help.\n"
    body += s"$label $value\n"

    val index = s"metrics/job/$job/instance/$instance"

    Http(s"http://$gateway/$index")
      .put(body)
      .header("Content-Type", "data-binary")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000)).asString
  }

  def startTimer(label : String) = {
    timerMap.put(label, System.currentTimeMillis())
  }

  def stopTimer(label : String) : Long = {
    val t = System.currentTimeMillis() - timerMap(label)
    timerMap.remove(label)
    t
  }

  def createCounter(label: String) = {
    counterMap.put(label, 0)
  }

  def incCounter(label: String) : Int = {
    counterMap.put(label, counterMap(label)+1)

    counterMap(label)
  }




}
