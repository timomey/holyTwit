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

def define_the_search(word):
    def findword(row):
        if word in row.text:
            return (row.name, 1)
        else:
            return 0
    return findword

def clean_string(text):
    """input: string (u''). output is a "clean" string:
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
    #text = text.encode('ascii','ignore')
    text.strip()
    text.replace("\/", "/")
    text.replace("\\", "\ ")
    text.replace("\'", "'")
    text.replace('\"', '"')
    text.replace("\n", " ")
    text.replace("\t", " ")
    text = " ".join(text.split())
    text = text.lower()
    #if text == "":
    #    raise BlankError('empty tweet')
    return text

class BlankError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def write_into_cassandra(record):
    #from cassandra.cluster import Cluster
    #from cassandra import ConsistencyLevel
    keyspacename = 'twitterimpact'
    tablename = 'realtime'
    wordofinterest = '#justinbieber'
    # connect to cassandra
    cluster = Cluster(['ec2-52-35-24-163.us-west-2.compute.amazonaws.com','ec2-52-89-22-134.us-west-2.compute.amazonaws.com','ec2-52-34-117-127.us-west-2.compute.amazonaws.com','ec2-52-89-0-97.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    cassandra_create_table(keyspacename,tablename, session)
    prepared_write_query = session.prepare("INSERT INTO "+keyspacename+"."+tablename+" (wordofinterest, time, date, location, cowords_firstdegree, tweet) VALUES (?,?,?,?,?,?)")
    for i in record:
        json_str = json.loads(i)

        try:
            if wordofinterest in json_str['text']:
                wordofinterest
                time = clean_string(json_str["timestamp_ms"])
                date = timepackage.strftime('%Y-%m-%d %H:%M:%S',  timepackage.gmtime(int(time)/1000.))
                location = clean_string(json_str["place"]["name"])+', '+clean_string(json_str["place"]["country_code"])
                cowords_firstdegree = str(clean_string(json_str['text'].encode('ascii','ignore')).split())
                tweet = str(clean_string(json_str['text'].encode('ascii','ignore')))
                session.execute(prepared_write_query, (wordofinterest, time, date, location, cowords_firstdegree))
        except (KeyError, BlankError):
            #could implement counter here
            continue
        except TypeError:
            #this basically occurs when place.name does not exist.
            continue


def process(rdd):
    rdd.foreachPartition(lambda record: write_into_cassandra(record))

#cassandra stuff:
def cassandra_create_keyspace(keyspacename,session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")

def cassandra_create_table(keyspacename, tablename, session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")
    session.execute("CREATE TABLE IF NOT EXISTS "+keyspacename+"."+tablename+" (wordofinterest text, time text, date text, location text, cowords_firstdegree text,tweet text, PRIMARY KEY ((wordofinterest, location, date), time)) WITH CLUSTERING ORDER BY (time DESC);")




if __name__ == "__main__":

    #findword = define_the_search(wordofinterest)

    sc = SparkContext(appName="TwitterImpact")
    ssc = StreamingContext(sc, .1)

    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    topic = "tweets"
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 2})
    lines = kvs.map(lambda x: x[1])
    lines.foreachRDD(process )

    ssc.start()
    ssc.awaitTermination()
