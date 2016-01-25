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
from datetime import datetime
from operator import add

def define_the_search(word):
    def findword(row):
        if word in row.text:
            return (row.name, 1)
        else:
            return 0
    return findword

#session.execute("""
    #CREATE TABLE demostream (time text, city text PRIMARY KEY, country text);
    #""")



if __name__ == "__main__":
    wordofinterest = "#justinbieber"
    findword = define_the_search(wordofinterest)

    sc = SparkContext(appName="TwitterImpact")
    ssc = StreamingContext(sc, .1)

    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    topic = "tweets"
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 2})
    #kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": ['ec2-52-35-24-163.us-west-2.compute.amazonaws.com:9092','ec2-52-89-22-134.us-west-2.compute.amazonaws.com:9092','ec2-52-34-117-127.us-west-2.compute.amazonaws.com:9092','ec2-52-89-0-97.us-west-2.compute.amazonaws.com:9092']})
    lines = kvs.map(lambda x: x[1])


    #counts = lines.map(findword) \
    #    .filter(lambda a: a!=0,) \
    #    .reduceByKey(add)


    def write_into_cassandra(record):
        from cassandra.cluster import Cluster
        from cassandra import ConsistencyLevel

        # connect to cassandra
        cluster = Cluster(['ec2-52-35-24-163.us-west-2.compute.amazonaws.com'])
        session = cluster.connect("demo")

        prepared_write_query = session.prepare("INSERT INTO demostream (time, city, country) VALUES (?,?,?)")
        for i in record:
            print (i)
            json_str = json.loads(i)
            print (json_str)
            time = str(json_str["created_at"])
            city = 'test'#str(json_str["place"]["name"])
            country = str(json_str["text"])
            session.execute(prepared_write_query, (time, city, country))


    def process(rdd):
        rdd.foreachPartition(lambda record: write_into_cassandra(record))

    lines.foreachRDD(process )


    ssc.start()
    ssc.awaitTermination()
