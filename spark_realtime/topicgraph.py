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


def update_to_cassandra(record):
    cluster = Cluster([
        'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
        'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
        'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
        'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    #cassandra_create_topicgraph_table(keyspacename,tablename, session)

    #prepared_write_query = session.prepare("UPDATE "+keyspacename+"."+tablename+" SET count = count + ? WHERE connection=? AND word=?")
    prepared_write_query = session.prepare("INSERT INTO "+keyspacename+"."+tablename+" (word, connection, count, time) VALUES (?,?,?,?) USING TTL 120;")
    for element in record:
        word = str(element[0][0])
        connection = str(element[0][1])
        count = element[1]
        session.execute(prepared_write_query, (word,connection,count, int(timepackage.time())*1000 ))




def topicgraph_to_cassandra(rdd):
    #each RDD consists of a bunch of partitions which themselves are local on a single machine (each)
    #so for each partition, do what you wanna do ->
    rdd.foreachPartition(lambda record: update_to_cassandra(record))





if __name__ == "__main__":

    #word = str(sys.argv[1])

    #cassandra keyspace name
    keyspacename = 'holytwit'
    tablename = 'topicgraph'

    #spark streaming objects
    sc = SparkContext(appName="topicgraph")
    ssc = StreamingContext(sc, 1)


    #zookeeper quorum for to connect to kafka (local ips for faster access)
    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    #kafka topic to consume from:
    topic = "faketwitterstream"
    #topic and number of partitions (check with kafka)
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-topicgraph", {topic: 4})
    lines = kvs.map(lambda x: x[1])


    cluster = Cluster([
        'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
        'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
        'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
        'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    #get wordlist from cassandra
    read_stmt = "select word,numberofwords from "+keyspacename+".listofwords ;"

    def lambda_map_word_connections(splitted_text):
        response = session.execute(read_stmt)
        wordlist = [str(row.word) for row in response]
        #broadcasted_wordlist = sc.broadcast(wordlist)
        #broadcasted_wordlist.unpersist(blocking=False)

        return_list_of_tuples=list()
        for word_input in wordlist:
            if word_input in splitted_text:
                for word_tweet in splitted_text:
                    if word_tweet != word_input:
                        return_list_of_tuples.append( ( (word_input, str(word_tweet.encode('ascii','ignore')) ) , 1))
        return  return_list_of_tuples

    #1. filter: is the word in the tweet. 2.filter does it have a place name 3. filter does it have country country_code
    #4. map it to ((place.name, place.country_code),1).
    #5. reducebykey add a+b -> sum for each place.
    #def countcity(lines):
    output = lines.filter(lambda l: len(json.loads(l)['text'])>0 )\
        .filter(lambda l: json.loads(l)["timestamp_ms"] >0  )\
        .map(lambda l: set(json.loads(l)["text"].split() )) \
        .flatMap(lambda l: lambda_map_word_connections(l)) \
        .reduceByKey(lambda a,b: a+b)
        #this could be an attempt to sort; but makes sense maybe only in batch?!?
        #.map(lambda l: (l[1],l[0]))\
        #.transform(sortByKey)

    #output.pprint()
    #before doing the stuff, create the table if necessary (schema defined here too)
    #output is a DStream object containing a bunch of RDDs. for each rdd go ->
    output.foreachRDD(topicgraph_to_cassandra)


    #start the stream and keep it running - await for termination too.
    ssc.start()
    ssc.awaitTermination()
