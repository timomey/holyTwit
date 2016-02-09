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

def update_to_cassandra(record):
    cluster = Cluster([
        'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
        'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
        'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
        'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()

    #prepared_write_query = session.prepare("UPDATE "+keyspacename+"."+tablename+" SET count = count + ? WHERE connection=? AND wordofinterest=?")
    prepared_write_query = session.prepare("INSERT INTO "+keyspacename+"."+tablename+" (word, connection, count, time) VALUES (?,?,?,?) USING TTL 120;")
    for element in record:
        word = str(element[0][0])
        connection = str(element[0][1])
        count = element[1]
        session.execute(prepared_write_query, (word,connection,count, int(timepackage.time())*1000 ))

def write_to_cassandra(record):
    cluster = Cluster([
        'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
        'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
        'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
        'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    write_query = session.prepare("INSERT INTO holytwit.degree1\
                                    (word,degree1,place,date,min,se60,se55,se50,se45,se40,se35,se30,se25,se20,se15,se10,se5)\
                                    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    write_query.consistency_level = ConsistencyLevel.QUORUM
    read_query = session.prepare("SELECT *\
                                    FROM holytwit.degree1\
                                    WHERE word=? AND degree1=? AND place=? AND date=?")
    read_query.consistency_level = ConsistencyLevel.QUORUM
    for ((word,degree1, place),count) in record:
        #time string for right now in minutes:
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        rows = session.execute(read_query, (word, degree1,place,date))
        if rows:
            now = datetime.datetime.now()
            countarray = [rows[0].se5, rows[0].se10, rows[0].se15, rows[0].se20, rows[0].se25, rows[0].se30, rows[0].se35, rows[0].se40, rows[0].se45, rows[0].se50, rows[0].se55, rows[0].se60, rows[0].min]
            testlist = range(5,61,5)
            for i in range(len(testlist)):
                if testlist[i] > now.second:
                    countarray[i]=countarray[i]+count
                    countarray[-1] += count
                    countarray[-1] -= countarray[(i+1)%(len(countarray)-1)]
                    countarray[(i+1)%(len(countarray)-1)] = 0
                    break
            session.execute(write_query,(word, degree1, place, date) + tuple(countarray[::-1]) )
        else:
            now = datetime.datetime.now()
            countarray= [0,] * 13
            testlist = range(5,61,5)
            for i in range(len(testlist)):
                if testlist[i] > now.second:
                    countarray[i] = count
                    countarray[-1]= count
                    break
            session.execute(write_query,(word, degree1, place, date) + tuple(countarray[::-1]))


def update_to_cassandracity2(record):
    #There is a problem with counter variable count. ; maybe counter can not be ordered by?!?
    cluster = Cluster([
        'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
        'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
        'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
        'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    #cassandra_create_citycount_table(keyspacename,citycounttablename, session)

    prepared_write_query = session.prepare("INSERT INTO "+keyspacename+"."+citycounttablename+" (wordofinterest,place,count) VALUES (?,?,?) USING TTL 120")
    prepared_write_query.consistency_level = ConsistencyLevel.QUORUM
    prepared_read_query = session.prepare("SELECT count FROM "+keyspacename+"."+citycounttablename+" WHERE wordofinterest=? AND place =?")
    prepared_read_query.consistency_level = ConsistencyLevel.QUORUM
    for element in record:
        word = element[0][0]
        place = str(element[0][1].encode('ascii','ignore'))+", "+ str(element[0][2].encode('ascii','ignore'))
        count = element[1]
        row = session.execute(prepared_read_query,(word,place))
        if row:
            count += row[0].count
            session.execute(prepared_write_query, (word, place, count))
        else:
            session.execute(prepared_write_query, (word, place, count))


def citycount_to_cassandra(rdd):
    #each RDD consists of a bunch of partitions which themselves are local on a single machine (each)
    #so for each partition, do what you wanna do ->
    rdd.foreachPartition(lambda record: update_to_cassandracity2(record))

def topicgraph_to_cassandra(rdd):
    #each RDD consists of a bunch of partitions which themselves are local on a single machine (each)
    #so for each partition, do what you wanna do ->
    rdd.foreachPartition(lambda record: write_to_cassandra(record))

def perco_parse(result):
    # take the first match
    if result.get('matches') is not None and len(result['matches'])>0:
        return([int(r['_id']) for r in result['matches']][0])

if __name__ == "__main__":

    #cassandra keyspace name
    keyspacename = 'holytwit'
    tablename = 'topicgraph'
    citycounttablename = 'city_count'

    #spark streaming objects
    sc = SparkContext(appName="topicgraph")
    ssc = StreamingContext(sc, 1)

    #zookeeper quorum for to connect to kafka (local ips for faster access)
    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    #brokers = "52.34.117.127:9092,52.89.22.134:9092,52.35.24.163:9092,52.89.0.97:9092"
    #kafka topic to consume from:
    topic = "faketwitterstream"
    #topic and number of partitions (check with kafka)
    #directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-topicgraph", {topic: 8})
    #2nd stream for search querries
    userqueries = kquerys.map(lambda x: x[1])
    lines = kvs.map(lambda x: x[1])

    #for cassandra:
    cluster = Cluster([
        'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
        'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
        'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
        'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    #get wordlist from cassandra
    read_stmt = "select word from "+keyspacename+".listofwords ;"
    response = session.execute(read_stmt)
    wordlist = [str(row.word) for row in response]
    #broadcasted_wordlist = sc.broadcast(wordlist)

    def lambda_map_word_connections(tuuple):
        return_list_of_tuples=[]
        splitted_text = tuuple[0][0]
        if splitted_text != 'error':
            for word_input in wordlist:
                if word_input in splitted_text:
                    for word_tweet in splitted_text:
                        if word_tweet != word_input:
                            return_list_of_tuples.append( ( (str(word_input), str(word_tweet.encode('ascii','ignore')), tuuple[0][1] ) , 1) )
                            return  return_list_of_tuples
                        else:
                            return [((word_input,'intweet','noplace'),1)]
                else:
                    return [((word_input,'notintweet','noplace'),1) ]
        else:
            return [tuuple]

    def textsplit_placetuple(tweet):
        try:
            splittextset = list(set(json.loads(tweet)["text"].split()))
            #hashtags:
            #hashtags = [hash.split()[0] for hash in json.loads(tweets)["text"].split('#')[1:]]
            place = str(json.loads(tweet)["place"]["name"].encode('ascii','ignore')+","+json.loads(tweet)["place"]["country_code"].encode('ascii','ignore'))
            return ((splittextset,place),1)
        except TypeError:
            return (('error','error','noplace'),1)

    #output = lines.map(lambda l: textsplit_placetuple(l) )\
    #    .map(lambda l: lambda_map_word_connections(l))\
    #    .flatMap(lambda l: l)\
    #    .reduceByKey(lambda a,b: a+b)
        #.map(lambda l: (l[1],l[0]))\
        #.transform(sortByKey)
    #output.pprint()
    #output.foreachRDD(topicgraph_to_cassandra)



    hashtagsoutput = lines.map(lambda l: text_hashtags_place_tuple(l) )\
        .flatMap(lambda l: l)\
        .reduceByKey(lambda a,b: a+b)
    hashtagsoutput.pprint()
    #hashtagsoutput.foreachRDD(topicgraph_to_cassandra)

    def lambda_map_word_city(tweet):
        return_list_of_tuples=[]
        for word in wordlist:
            try:
                if word in json.loads(tweet)["text"]:
                    return_list_of_tuples.append( ( (word, json.loads(tweet)["place"]["name"], json.loads(tweet)["place"]["country_code"] ) , 1))
            except:
                pass
        return  return_list_of_tuples


    #output2 = lines.flatMap(lambda l: lambda_map_word_city(l) )\
    #    .reduceByKey(lambda a,b: a+b)
    #output2.foreachRDD(citycount_to_cassandra)


    #start the stream and keep it running - await for termination too.
    ssc.start()
    ssc.awaitTermination()
