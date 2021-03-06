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



def write_to_cassandra(record):
    cluster = Cluster(['ec2-52-33-153-115.us-west-2.compute.amazonaws.com','ec2-52-36-102-156.us-west-2.compute.amazonaws.com'])
    #cluster = Cluster([
    #    'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
    #    'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
    #    'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
    #    'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    write_query = session.prepare("INSERT INTO holytwit.htgraph\
                                    (word, degree1, count)\
                                    values (?,?,?)")
    write_query.consistency_level = ConsistencyLevel.QUORUM
    read_query = session.prepare("SELECT *\
                                    FROM holytwit.htgraph\
                                    WHERE word=? AND degree1=?")
    read_query.consistency_level = ConsistencyLevel.QUORUM
    #delete_query = session.prepare("DELETE *\
    #                                FROM holytwit.htgraph\
    #                                WHERE word=? AND degree1=? AND date=?")
    #delete_query.consistency_level = ConsistencyLevel.QUORUM
    for ((word,degree1),count) in record:
        #time string for right now in minutes:
        currenttime = datetime.datetime.now()
        date = currenttime.strftime('%Y-%m-%d %H')
        rows = session.execute(read_query, (word, degree1))
        #session.execute(delete_query, (word, degree1,date))
        if rows:
            newcount = rows[0].count + count
            session.execute(write_query,(word, degree1, newcount) )
        else:
            session.execute(write_query,(word, degree1,count) )
def write_city_to_cassandra(record):
    #cluster = Cluster([
    #    'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
    #    'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
    #    'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
    #    'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    cluster = Cluster(['ec2-52-33-153-115.us-west-2.compute.amazonaws.com','ec2-52-36-102-156.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    write_query = session.prepare("INSERT INTO holytwit.city_count\
                                    (word, place, count)\
                                    values (?,?,?)")
    write_query.consistency_level = ConsistencyLevel.QUORUM
    read_query = session.prepare("SELECT *\
                                    FROM holytwit.city_count\
                                    WHERE word=? AND place=?")
    read_query.consistency_level = ConsistencyLevel.QUORUM

    #delete_query = session.prepare("DELETE *\
    #                                FROM holytwit.city_count\
    #                                WHERE word=? AND place=? AND date=?")
    #delete_query.consistency_level = ConsistencyLevel.QUORUM
    for ((word,place),count) in record:
        #time string for right now in minutes:
        currenttime = datetime.datetime.now()
        date = currenttime.strftime('%Y-%m-%d %H')
        rows = session.execute(read_query, (word, place))
        #session.execute(delete_query, (word,place,date))
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

    #cassandra keyspace name
    keyspacename = 'holytwit'
    tablename = 'topicgraph'
    citycounttablename = 'city_count'
    #spark  objects
    conf = SparkConf()
    conf.setAppName("holyTwit")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.streaming.receiver.maxRate", 500)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 2)

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
        es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )
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

    def text_hashtags_tuple(tweet):
        es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )
        return_list_of_tuples=[]
        #see if the text is there:
        try:
            text = json.loads(tweet)["text"]
        except TypeError:
            return [(('notext','na'),1)]
        #check if this text contains any of the keywords.
        esresult = es.percolate(index='twit',doc_type='.percolate', body={'doc':{'message': text }})
        if esresult['matches']:
            matched_words = []
            #for all matches
            for match in esresult['matches']:
                #get the matched keyword and store in list
                perc_match = es.get(index='twit',doc_type='.percolator',id=match['_id'])
                matched_words.append(perc_match['_source']['query']['match']['message'])
            #get hashtags and place
            try:
                hashtags = [hash.split()[0] for hash in text.split('#')[1:]]
            except IndexError:
                #pass
                list_of_tuple = map(lambda x: (x, 'nohashtags'), matched_words)
                #return (matched_words, 'nohashtags')
                return list_of_tuple
            else:
                list_of_lists_of_tuples = map(lambda x: [(x,ht) for ht in hashtags] ,matched_words)
                list_of_tuple = [(item,1) for sublist in list_of_lists_of_tuples for item in sublist]
                return list_of_tuple

        else:
            #maybe faster to not return
            pass
            #return [(('nomatch','na','na'),1)]

    def NoneTypefilter(text_ht_place_tuple_list):
        if text_ht_place_tuple_list:
            return True
        else:
            return False

    def ES_check(tweet):
        es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )
        try:
            text = json.loads(tweet)["text"].lower()
            place = str(json.loads(tweet)["place"]["name"].encode('ascii','ignore') +", "+ json.loads(tweet)["place"]["country_code"].encode('ascii','ignore')).lower()
        except TypeError:
            return (('na','na'),1)

        esresult = es.percolate(index='twit',doc_type='.percolate', body={'doc':{'message': text }})
        if esresult['matches']:
            matched_words = []
            #for all matches
            for match in esresult['matches']:
                #get the matched keyword and store in list
                perc_match = es.get(index='twit',doc_type='.percolator',id=match['_id'])
                matched_words.append(perc_match['_source']['query']['match']['message'])

            return (matched_words, text, place)
        else:
            #return (('nm','nm'),1)
            pass

    def word_and_hashtag(mw_t_p_tuple):
        if mw_t_p_tuple[1] == 1:
            return [mw_t_p_tuple]
        else:
            matched_words = mw_t_p_tuple[0]
            text = mw_t_p_tuple[1]
            try:
                hashtags = [hash.split()[0] for hash in text.split('#')[1:]]
            except IndexError:
                #pass
                list_of_tuple = map(lambda x: (x, 'nohashtags'), matched_words)
                #return (matched_words, 'nohashtags')
                return list_of_tuple
            else:
                list_of_lists_of_tuples = map(lambda x: [(x,ht) for ht in hashtags] ,matched_words)
                list_of_tuple = [(item,1) for sublist in list_of_lists_of_tuples for item in sublist]
                return list_of_tuple




    hashtagsoutput = lines.map(lambda l: ES_check(l) )\
        .filter(lambda l: NoneTypefilter(l))\
        .map(lambda l: word_and_hashtag(l))\
        .flatMap(lambda l: l)\
        .reduceByKey(lambda a,b: a+b)
    #hashtagsoutput.pprint()
    hashtagsoutput.foreachRDD(topicgraph_to_cassandra)

    ############################################
    # twitterstream -> city count
    ############################################

    def word_and_city(matchwords_text_place_tuple):
        if matchwords_text_place_tuple[1] == 1:
            return [matchwords_text_place_tuple]
        else:
            matched_words = matchwords_text_place_tuple[0]
            place = matchwords_text_place_tuple[2]
            return map(lambda x: ((x, place),1),matched_words)

    cityoutput = lines.map(lambda l: ES_check(l))\
        .filter(lambda l: NoneTypefilter(l))\
        .map(lambda l: word_and_city(l))\
        .flatMap(lambda l: l)\
        .reduceByKey(lambda a,b: a+b)

    cityoutput.foreachRDD(city_to_cassandra)


    ssc.start()
    ssc.awaitTermination()
