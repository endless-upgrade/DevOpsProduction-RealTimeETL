#!/bin/bash

echo "FAT JAR builted"
echo "submit the task"

if [ "$#" -lt 2 ]; then
    echo "USAGE compileAndSubmit topicName smallest|largest [--debug | --hive]"
fi

echo $1
echo $2

if [[ -z $3 ]]; then
    echo $3
    spark-submit --class Stream --master local[*] --jars jars/kafka_2.11-0.8.2.1.jar,jars/kafka-clients-0.8.2.1.jar,jars/spark-streaming_2.11-2.2.0.jar,jars/spark-streaming-kafka-0-8_2.11-2.2.0.jar,jars/metrics-core-2.2.0.jar,jars/kudu-spark2_2.11-1.5.0.jar target/scala-2.11/realtimeetl_2.11-0.1.jar $1 $2 $3

else
    spark-submit --class Stream --master local[*] --jars jars/kafka_2.11-0.8.2.1.jar,jars/kafka-clients-0.8.2.1.jar,jars/spark-streaming_2.11-2.2.0.jar,jars/spark-streaming-kafka-0-8_2.11-2.2.0.jar,jars/metrics-core-2.2.0.jar,jars/kudu-spark2_2.11-1.5.0.jar target/scala-2.11/realtimeetl_2.11-0.1.jar $1 $2
fi
