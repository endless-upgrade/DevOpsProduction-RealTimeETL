package it.reply.data.pasquali.engine

import it.reply.data.pasquali.model.TransformedDFs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ETL {

  def transformTags(jsonDF : DataFrame) : TransformedDFs = {

    val cols = Seq("id", "userid", "movieid", "tag", "timestamp")
    val colsKudu = Seq("userid", "movieid", "tag", "time")

    val toHive = jsonDF
      .withColumn("id", jsonDF("payload.id"))
      .withColumn("userid", jsonDF("payload.userid"))
      .withColumn("movieid", jsonDF("payload.movieid"))
      .withColumn("tag", jsonDF("payload.tag"))
      .withColumn("timestamp", jsonDF("payload.timestamp"))

    val toKudu = toHive.toDF("payload", "schema", "id", "userid", "movieid", "tag", "time")
      .select(colsKudu.head, colsKudu.tail: _*)

    TransformedDFs(toHive, toKudu)
  }

  def transformRatings(jsonDF : DataFrame) : TransformedDFs = {

    val cols = Seq("id", "userid", "movieid", "rating", "timestamp")
    val colsKudu = Seq("userid", "movieid", "rating", "time")

    val toHive = jsonDF
      .withColumn("id", jsonDF("payload.id"))
      .withColumn("userid", jsonDF("payload.userid"))
      .withColumn("movieid", jsonDF("payload.movieid"))
      .withColumn("rating", jsonDF("payload.rating"))
      .withColumn("timestamp", jsonDF("payload.timestamp"))

    val toKudu = toHive.toDF("payload", "schema", "id", "userid", "movieid", "rating", "time")
      .select(colsKudu.head, colsKudu.tail: _*)

    TransformedDFs(toHive, toKudu)
  }

  def transformGenomeScores(jsonDF : DataFrame) : TransformedDFs = {

    val cols = Seq("id", "movieid", "tagid", "relevance")
    val colsKudu = Seq("movieid", "tagid", "relevance")

    val toHive = jsonDF
      .withColumn("id", jsonDF("payload.id"))
      .withColumn("movieid", jsonDF("payload.movieid"))
      .withColumn("tagid", jsonDF("payload.tagid"))
      .withColumn("relevance", jsonDF("payload.relevance"))

    val toKudu = null // TODO

    TransformedDFs(toHive, toKudu)
  }

  def transformRDD(jsonStringRDD : RDD[String],
                   spark : SparkSession,
                   tableName : String) : TransformedDFs = {

    val jsonDF = spark.sqlContext.jsonRDD(jsonStringRDD)

    tableName match {
      case "tags" => ETL.transformTags(jsonDF)
      case "ratings" => ETL.transformRatings(jsonDF)
      case "genomescore" => ETL.transformGenomeScores(jsonDF)
    }

  }

}
