name := "RealTimeETL"

version := "0.1"

scalaVersion := "2.11.11"

//retrieveManaged := true

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-hive_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-yarn_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0",
  "org.apache.kudu" % "kudu-spark2_2.11" % "1.5.0",
  "com.yammer.metrics" % "metrics-core" % "2.2.0"
)

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.2" % "test"
        