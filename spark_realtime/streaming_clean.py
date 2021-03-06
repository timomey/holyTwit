#!/usr/bin/env python
#This is the spark streaming job that needs "clean" input to work smoothly.
#streaming.py takes any input and takes care of the dirty stuff.
#to have more efficiency the tweets can be cleaned before ingestion to kafka and then consumed with this file.


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
import itertools
from stop_words import get_stop_words




def write_to_cassandra(record):
    """
    this function saves the words that were found to be connections to cassandra.
    It takes care of data that is already in there. The number of occurences are sorted within cassandra using materialized views (cassandra 3.0 and higher)

    input: record should have the format ((word, connection), count)
    """
    cluster = Cluster([
        'ec2-52-36-123-77.us-west-2.compute.amazonaws.com',
        'ec2-52-36-185-47.us-west-2.compute.amazonaws.com',
        'ec2-52-26-37-207.us-west-2.compute.amazonaws.com',
        'ec2-52-33-125-6.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    write_query = session.prepare("INSERT INTO holytwit.htgraph\
                                    (word, degree1, count)\
                                    values (?,?,?) USING TTL 1200")
    write_query.consistency_level = ConsistencyLevel.QUORUM
    read_query = session.prepare("SELECT *\
                                    FROM holytwit.htgraph\
                                    WHERE word=? AND degree1=?")
    #consistency level: make sure at least 2 nodes say the same thing:
    read_query.consistency_level = ConsistencyLevel.QUORUM
    #for each word - connection - count tuple , write to cassandra
    for ((word,degree1),count) in record:
        rows = session.execute(read_query, (word, degree1))
        #in case there was already an entry, add the old value:
        if rows:
            newcount = rows[0].count + count
            session.execute(write_query,(word, degree1, newcount) )
        else:
            session.execute(write_query,(word, degree1,count) )
def write_city_to_cassandra(record):
    """
    this function saves the place where a tweet was tweeted to cassandra.

    input: record should have the format: ((word, place),count)
    """
    #connect to cassandra cluster and start a session
    cluster = Cluster([
        'ec2-52-36-123-77.us-west-2.compute.amazonaws.com',
        'ec2-52-36-185-47.us-west-2.compute.amazonaws.com',
        'ec2-52-26-37-207.us-west-2.compute.amazonaws.com',
        'ec2-52-33-125-6.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    #prepare the cassandra query to write the data to cassandra
    write_query = session.prepare("INSERT INTO holytwit.city_count\
                                    (word, place, count)\
                                    values (?,?,?) USING TTL 1200")
    #make sure that the data is written consistently (this makes sure at least 2 nodes have the new data)
    write_query.consistency_level = ConsistencyLevel.QUORUM
    #prepare read query
    read_query = session.prepare("SELECT *\
                                    FROM holytwit.city_count\
                                    WHERE word=? AND place=?")
    #make sure the value that is read is consistent (waits for 2 nodes to respond and checks for consistency)
    read_query.consistency_level = ConsistencyLevel.QUORUM

    #for each word - place - count tuple , write to cassandra
    for ((word,place),count) in record:
        rows = session.execute(read_query, (word, place))
        #in case there was already an entry, add the old value:
        if rows:
            newcount = rows[0].count + count
            session.execute(write_query,(word, place, newcount) )
        else:
            session.execute(write_query,(word, place,count) )

def topicgraph_to_cassandra(rdd):
    #each RDD consists of a bunch of partitions which themselves are local on a single machine (each)
    #so for each partition, do what you wanna do ->
    rdd.foreachPartition(lambda record: write_to_cassandra(record))

def city_to_cassandra(rdd):
    #each RDD consists of a bunch of partitions which themselves are local on a single machine (each)
    #so for each partition, do what you wanna do ->
    rdd.foreachPartition(lambda record: write_city_to_cassandra(record))

if __name__ == "__main__":
    stopwords = []
    with open('stop_words.txt', 'r') as f:
        for line in f:
            if line != '\n':
                stopwords.append(line.strip())
    #stop_words = get_stop_words('en')
    #cassandra keyspace name
    keyspacename = 'holytwit'
    tablename = 'topicgraph'
    citycounttablename = 'city_count'
    #spark  objects
    conf = SparkConf()
    conf.setAppName("holyTwit")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.streaming.receiver.maxRate", 100)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)
    broadcast_stopwords = sc.broadcast(stopwords)
    #StorageLevel.MEMORY_AND_DISK_SER
    ############################################
    # consume from kafka streams
    ############################################
    #zookeeper quorum for to connect to kafka (local ips for faster access)
    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    # Stream 1 ->faketwitterstream
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-topicgraph", {"faketwitterstream": 8})
    topic ="faketwitterstream"
    brokers = "52.34.117.127:9092,52.89.22.134:9092,52.35.24.163:9092,52.89.0.97:9092"
    #kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    #Stream 2 -> queries
    kquerys = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-elastc", {"elasticquerries": 8})
    KafkaUtils.createDirectStream
    userqueries = kquerys.map(lambda x: x[1])

    ############################################
    # webside -> kafka stream -> ES query:
    ############################################
    def eachrddfct(rdd):
        rdd.foreachPartition(lambda record: query_to_es(record) )

    def sendPartition(iter):
        #es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )
        es = Elasticsearch(hosts=[{"host":"ip-172-31-2-202", "port":9200},{"host":"ip-172-31-2-201", "port":9200},{"host":"ip-172-31-2-200", "port":9200},{"host":"ip-172-31-2-203", "port":9200}] )
        try:
            for word in iter:
                es.create(index='twit', doc_type='.percolator', body={'query': {'match': {'message': word  }}})
        except TypeError:#this exception handles the ongoing empty stream. TypeError: NoneType
            pass

    inputwords = userqueries.flatMap(lambda l: str(l).split() )
    inputwords.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition ))

    ############################################
    # twitterstream -> hashtag graph
    ############################################
    #ES connection on the driver level
    #es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )


    def NoneTypefilter(text_ht_place_tuple_list):
        if text_ht_place_tuple_list:
            return True
        else:
            return False

    def ES_check(tweet):
        #es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )
        es = Elasticsearch(hosts=[{"host":"ip-172-31-2-202", "port":9200},{"host":"ip-172-31-2-201", "port":9200},{"host":"ip-172-31-2-200", "port":9200},{"host":"ip-172-31-2-203", "port":9200}] )
        text = json.loads(tweet)["text"].lower()

        esresult = es.percolate(index='twit',doc_type='.percolate', body={'doc':{'message': text }})
        if esresult['matches']:
            #for all matches
            matched_words_perc = map(lambda l: es.get(index='twit',doc_type='.percolator',id=l['_id']),esresult['matches'])
            matched_words = map(lambda l: l['_source']['query']['match']['message'] , matched_words_perc)

            place = json.loads(tweet)["place"].lower()
            hashtags = json.loads(tweet)["hashtags"]
            return (matched_words, text, place, hashtags)
        else:
            #return ()
            pass

    def word_and_hashtag(mw_t_p_tuple):
        if not mw_t_p_tuple:
            return (('nm','nm'),1)
        else:
            matched_words = mw_t_p_tuple[0]
            text = mw_t_p_tuple[1]
            hashtags = mw_t_p_tuple[3]
            if not hashtags:
                pass
            else:
                list_of_tuple=[]
                for i in itertools.product(matched_words, hashtags):
                    list_of_tuple.append((i,1))
                return list_of_tuple


    def word_and_words(mw_t_p_tuple):
        if not mw_t_p_tuple:
            return (('nm','nm'),1)
        else:
            matched_words = mw_t_p_tuple[0]
            text = mw_t_p_tuple[1]
            stop_words = get_stop_words('en')
            connections = list(set(text.split()))
            connections = [word for word in connections if word not in broadcast_stopwords.value]

            if not connections:
                pass
            else:
                list_of_tuple=[]
                for i in itertools.product(matched_words, connections):
                    list_of_tuple.append((i,1))
                return list_of_tuple



    hashtagsoutput = lines.map(lambda l: ES_check(l) )\
        .filter(lambda l: NoneTypefilter(l))\
        .map(lambda l: word_and_words(l) )\
        .flatMap(lambda l: l)\
        .reduceByKey(lambda a,b: a+b)
    #hashtagsoutput.pprint()
    hashtagsoutput.foreachRDD(topicgraph_to_cassandra)

    ############################################
    # twitterstream -> city count
    ############################################

    def word_and_city(matchwords_text_place_tuple):
        if not matchwords_text_place_tuple:
            return [matchwords_text_place_tuple]
        else:
            matched_words = matchwords_text_place_tuple[0]
            place = matchwords_text_place_tuple[2]
            return map(lambda x: ((x, place),1), matched_words)

    cityoutput = lines.map(lambda l: ES_check(l))\
        .filter(lambda l: NoneTypefilter(l))\
        .map(lambda l: word_and_city(l))\
        .flatMap(lambda l: l)\
        .reduceByKey(lambda a,b: a+b)

    cityoutput.foreachRDD(city_to_cassandra)


    ssc.start()
    ssc.awaitTermination()
