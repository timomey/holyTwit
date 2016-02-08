#!/usr/bin/env python
from __future__ import print_function
import sys
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
import time as timepackage
from operator import add
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan




if __name__ == "__main__":

    #cassandra keyspace name
    keyspacename = 'holytwit'
    tablename = 'topicgraph'
    citycounttablename = 'city_count'
    #spark  objects
    #conf = SparkConf().setAppName("holyTwit")
    #conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = SparkContext(appName="topicgraph")
    ssc = StreamingContext(sc, 1)


    #zookeeper quorum for to connect to kafka (local ips for faster access)
    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    #brokers = "52.34.117.127:9092,52.89.22.134:9092,52.35.24.163:9092,52.89.0.97:9092"
    #kafka topic to consume from:
    topic = "faketwitterstream"

    #alternative kafka stream:
    #directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    #StorageLevel.MEMORY_AND_DISK_SER

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-topicgraph", {topic: 8})
    #2nd stream for search querries
    kquerys = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-elastc", {"elasticquerries": 8})
    userqueries = kquerys.map(lambda x: x[1])
    lines = kvs.map(lambda x: x[1])


    ############################################
    # webside -> kafka stream -> ES query:
    ############################################
    def eachrddfct(rdd):
        rdd.foreachPartition(lambda record: query_to_es(record) )

    def sendPartition(iter):
        es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )
        try:
            for word in iter:
                #scroll = elasticsearch.helpers.scan(es, query='{"fields": "_id"}', index='twit', scroll='10s')
                #for res in scroll:
                #    if res['_id']
                es.create(index='twit', doc_type='.percolator', body={'query': {'match': {'message': word  }}})
        except TypeError:#this exception handles the ongoing empty stream. TypeError: NoneType
            pass

    inputwords = userqueries.flatMap(lambda l: str(l).split() )
    inputwords.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition ))

    ############################################


    ssc.start()
    ssc.awaitTermination()
