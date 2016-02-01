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

def clean_string(text):
    """input: string (u''). output is a "clean" string:
           1: spaces at end and beginning are removed:
                                ' input ' -> 'input'
            2: replacements:    "\/" -> "/"
                                "\\" -> "\ "
                                "\'" -> "'"
                                '\"' -> '"'
                                "\n" -> " "
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
    return text


#cassandra stuff:
def cassandra_create_keyspace(keyspacename,session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")

def cassandra_create_table(keyspacename, tablename, session):
    cassandra_create_keyspace(keyspacename, session)
    session.execute("CREATE TABLE IF NOT EXISTS "+keyspacename+"."+tablename+" (wordofinterest text, time text, date text, location text, cowords_firstdegree text,tweet text, PRIMARY KEY ((wordofinterest, location, date), time)) WITH CLUSTERING ORDER BY (time DESC);")


def cassandra_create_citycount_table(keyspacename, tablename, session):
    #it not exists create the keyspace
    cassandra_create_keyspace(keyspacename, session)
    # if not exists create table with following schema
    session.execute("CREATE TABLE IF NOT EXISTS "+keyspacename+"."+tablename+" \
                        (wordofinterest text, place text, count counter, \
                        PRIMARY KEY (wordofinterest,place) ); ")

def update_to_cassandra(record):
    #There is a problem with counter variable count. ; maybe counter can not be ordered by?!?
    tablename = wordofinterest
    cluster = Cluster([
        'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
        'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
        'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
        'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    cassandra_create_citycount_table(keyspacename,tablename, session)

    prepared_write_query = session.prepare("UPDATE "+keyspacename+"."+tablename+" SET count = count + ? WHERE place=? AND wordofinterest=?")
    for element in record:
        key = str(element[0][0])+", "+ str(element[0][1])
        count = element[1]
        session.execute(prepared_write_query, (count, key, wordofinterest) )




def citycount_to_cassandra(rdd):
    #each RDD consists of a bunch of partitions which themselves are local on a single machine (each)
    #so for each partition, do what you wanna do ->
    rdd.foreachPartition(lambda record: update_to_cassandra(record))





if __name__ == "__main__":

    wordofinterest = str(sys.argv[1])

    #cassandra keyspace name
    keyspacename = 'holytwit'
    tablename = 'citycount'

    #spark streaming objects
    sc = SparkContext(appName="TwitterImpact")
    ssc = StreamingContext(sc, 1)

    #zookeeper quorum for to connect to kafka (local ips for faster access)
    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    #kafka topic to consume from:
    topic = "twitterdump_timo"
    #topic and number of partitions (check with kafka)
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 4})
    lines = kvs.map(lambda x: x[1])

    #1. filter: is the word in the tweet. 2.filter does it have a place name 3. filter does it have country country_code
    #4. map it to ((place.name, place.country_code),1).
    #5. reducebykey add a+b -> sum for each place.
    #def countcity(lines):
    output = lines.filter(lambda l: wordofinterest in json.loads(l)["text"])\
        .filter(lambda l: len(json.loads(l)["place"]["name"]) > 0 )\
        .filter(lambda l: len(json.loads(l)["place"]["country_code"]) > 0)\
        .filter(lambda l: len(json.loads(l)["timestamp_ms"]) >0  )\
        .map(lambda l: ( (json.loads(l)["place"]["name"], json.loads(l)["place"]["country_code"] ), 1))\
        .reduceByKey(lambda a,b: a+b)

    #before doing the stuff, create the table if necessary (schema defined here too)
    #output is a DStream object containing a bunch of RDDs. for each rdd go ->
    output.foreachRDD(citycount_to_cassandra)

    #start the stream and keep it running - await for termination too.
    ssc.start()
    ssc.awaitTermination()
