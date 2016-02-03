#!/bin/bash

if [ "$#" -ne 0 ]; then
    echo "dude, use the webinterface to input your word" && exit 1
fi


$SPARK_HOME/bin/spark-submit --executor-memory 10000M --driver-memory 10000M --master spark://ip-172-31-2-199:7077 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 ./citycount.py 
