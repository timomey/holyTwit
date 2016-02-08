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
from elasticsearch.helpers import bulk




if __name__ == "__main__":

    #cassandra keyspace name
    keyspacename = 'holytwit'
    tablename = 'topicgraph'
    citycounttablename = 'city_count'
    #spark  objects
    conf = SparkConf().setAppName("holyTwit")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = SparkContext(conf)
    ssc = StreamingContext(sc, 1)

    #ELASTICSEARCH STUFF
    # index and document type constants
    INDEX_NAME = "documents"
    TYPE = "document"

    #elastic search connection:
    es = Elasticsearch(hosts=[{"host":["52.34.117.127","52.89.22.134","52.35.24.163","52.89.0.97"], "port":9200}], sniff_on_start=True)
    #create index for ES (ignore if it already exists       )
    es.indices.create(index='twit', ignore=400, body={
          "mappings": {
            "document": {
              "properties": {
                "message": {
                  "type": "string"
                }
              }
            }
          }
        }
    )

    #zookeeper quorum for to connect to kafka (local ips for faster access)
    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    #brokers = "52.34.117.127:9092,52.89.22.134:9092,52.35.24.163:9092,52.89.0.97:9092"
    #kafka topic to consume from:
    topic = "faketwitterstream"
    #alternative kafka stream:
    #directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-topicgraph", {topic: 8},StorageLevel.MEMORY_AND_DISK_SER)
    #2nd stream for search querries
    kquerys = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-topicgraph", {"elasticquerries": 8})
    userqueries = kquerys.map(lambda x: x[1])
    lines = kvs.map(lambda x: x[1])

    def sendPartition(iter):
        for inpu in iter:
            wordlist = inpu.split()
            count=0
            for word in wordlist:
                count+=1
                es.create(index='documents', doc_type='.percolator', body={'query': {'match': {'message': q}}}, id=count)

    userqueries.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

    #get all id's
    res = es.search(
        index="twit",
        body={"query": {"match_all": {}}, "size": 30000, "fields": ["_id"]})
    ids = [d['_id'] for d in res['hits']['hits']]
    #start the stream and keep it running - await for termination too.
    ssc.start()
    ssc.awaitTermination()
