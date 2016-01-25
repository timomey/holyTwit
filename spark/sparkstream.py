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

def clean_string(text):
    """input: string. output is a "clean" string:
            1: spaces at end and beginning are removed:
                                ' input ' -> 'input'
            2: replacements:    "\/" -> "/"
                                "\\" -> "\ "
                                "\'" -> "'"
                                '\"' -> '"'
                                "\n" -> " ")
                                "\t" -> " "
            3: multiple spaces are replaced by one.
                                "input      string" -> "input string"
            """
    text.strip()
    text.replace("\/", "/")
    text.replace("\\", "\ ")
    text.replace("\'", "'")
    text.replace('\"', '"')
    text.replace("\n", " ")
    text.replace("\t", " ")
    text = " ".join(text.split())
    text = text.lower()
    return text

def write_into_cassandra(record):
    from cassandra.cluster import Cluster
    from cassandra import ConsistencyLevel

    # connect to cassandra
    cluster = Cluster(['ec2-52-35-24-163.us-west-2.compute.amazonaws.com'])
    session = cluster.connect("demo")

    prepared_write_query = session.prepare("INSERT INTO demostream (time, city, country) VALUES (?,?,?)")
    for i in record:
        #print (i)
        json_str = json.loads(i)
        #print (json_str)

        time = 'ttt'#str(json_str["created_at"])
        city = 'test'#str(json_str["place"]["name"])
        country = str(json_str["text"])
        session.execute(prepared_write_query, (time, city, country))

def process(rdd):
    rdd.foreachPartition(lambda record: write_into_cassandra(record))




if __name__ == "__main__":
    wordofinterest = "#justinbieber"
    findword = define_the_search(wordofinterest)

    sc = SparkContext(appName="TwitterImpact")
    ssc = StreamingContext(sc, .1)

    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    topic = "tweets"
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 2})
    lines = kvs.map(lambda x: x[1])
    lines.foreachRDD(process )

    ssc.start()
    ssc.awaitTermination()
