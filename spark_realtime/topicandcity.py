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
            countarray = (rows[0].se5, rows[0].se10, rows[0].se15, rows[0].se20, rows[0].se25, rows[0].se30, rows[0].se35, rows[0].se40, rows[0].se45, rows[0].se50, rows[0].se55, rows[0].se60, rows[0].min)
            testlist = range(5,61,5)
            for i in range(len(testlist)):
                if testlist[i] > now.second:
                    countarray[i]=countarray[i]+count
                    countarray[-1] += count
                    countarray[-1] -= [(i+1)%(len(countarray)-1)]
                    countarray[(i+1)%(len(countarray)-1)] = 0
                    break
            session.execute(write_query,(word, degree1, place, date) + countarray[::-1] )
        else:
            now = datetime.datetime.now()
            countarray= (0,) * 13
            testlist = range(5,61,5)
            for i in range(len(testlist)):
                if testlist[i] > now.second:
                    countarray[i] = count
                    countarray[-1]= count
                    break
            session.execute(write_query,(word, degree1, place, date) + countarray[::-1])



def update_to_cassandracity(record):
    #There is a problem with counter variable count. ; maybe counter can not be ordered by?!?
    cluster = Cluster([
        'ec2-52-89-218-166.us-west-2.compute.amazonaws.com',
        'ec2-52-88-157-153.us-west-2.compute.amazonaws.com',
        'ec2-52-35-98-229.us-west-2.compute.amazonaws.com',
        'ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    #cassandra_create_citycount_table(keyspacename,citycounttablename, session)

    prepared_write_query = session.prepare("UPDATE "+keyspacename+"."+citycounttablename+" SET count = count + ? WHERE place=? AND wordofinterest=? ")
    for element in record:
        word = element[0][0]
        place = str(element[0][1].encode('ascii','ignore'))+", "+ str(element[0][2].encode('ascii','ignore'))
        count = element[1]
        session.execute(prepared_write_query, (count, place, word))

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

if __name__ == "__main__":
    #wordofinterest = str(sys.argv[1])

    #cassandra keyspace name
    keyspacename = 'holytwit'
    tablename = 'topicgraph'
    citycounttablename = 'city_count'

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
    read_stmt = "select word from "+keyspacename+".listofwords ;"
    response = session.execute(read_stmt)
    wordlist = [str(row.word) for row in response]
    #broadcasted_wordlist = sc.broadcast(wordlist)

    def lambda_map_word_connections(tuuple):
        return_list_of_tuples=list()
        splitted_text = tuuple[0]
        for word_input in wordlist:
            if word_input in splitted_text:
                for word_tweet in splitted_text:
                    if word_tweet != word_input:
                        return_list_of_tuples.append( ( (word_input, str(word_tweet.encode('ascii','ignore')), tuuple[1] ) , 1) )
        return  return_list_of_tuples

    #1. filter: is the word in the tweet. 2.filter does it have a place name 3. filter does it have country country_code
    #4. map it to ((place.name, place.country_code),1).
    #5. reducebykey add a+b -> sum for each place.
    #def countcity(lines):
    #output = lines.filter(lambda l: len(json.loads(l)['text'])>0 )\
        #.filter(lambda l: json.loads(l)["timestamp_ms"] >0  )\
        #.filter(lambda l: len(json.loads(l)["place"]["country_code"]) > 0)\
        #.filter(lambda l: len(json.loads(l)["place"]["name"])>0 )\
        #.map(lambda l: (set(json.loads(l)["text"].split()), json.loads(l)["place"]["name"]+","+json.loads(l)["place"]["country_code"] ) ) \
        #.flatMap(lambda l: lambda_map_word_connections(l)) \
        #.reduceByKey(lambda a,b: a+b)
    def textsplit_placetuple(tweet):
        try:
            splittextset = set(json.loads(l)["text"].split())
            place = json.loads(l)["place"]["name"]+","+json.loads(l)["place"]["country_code"]
            return ((splittextset,place),1)
        except:
            return ('error',0)

    output = lines.map(lambda l: textsplit_placetuple ) \
        .flatMap(lambda l: lambda_map_word_connections(l)) \
        .reduceByKey(lambda a,b: a+b)
        #.map(lambda l: (l[1],l[0]))\
        #.transform(sortByKey)
    #output.pprint()
    #before doing the stuff, create the table if necessary (schema defined here too)
    #output is a DStream object containing a bunch of RDDs. for each rdd go ->
    output.foreachRDD(topicgraph_to_cassandra)


    def lambda_map_word_city(tweet):
        return_list_of_tuples=list()
        for word in wordlist:
            try:
                if word in json.loads(tweet)["text"]:
                    return_list_of_tuples.append( ( (word, json.loads(tweet)["place"]["name"], json.loads(tweet)["place"]["country_code"] ) , 1))
            except:

        return  return_list_of_tuples


    #lines.MEMORY_AND_DISK()
    #1. filter: is the word in the tweet. 2.filter does it have a place name 3. filter does it have country country_code#4. map it to ((place.name, place.country_code),1).#5. reducebykey add a+b -> sum for each place.#def countcity(lines):
    #output = lines.filter(lambda l: wordofinterest in json.loads(l)["text"])\
    #output = lines.map(lambda l: json.loads(l)["place"]["name"] )
    #output = lines.filter(lambda l: len(json.loads(l)['text'])>0 )\
        #.filter(lambda l: json.loads(l)["timestamp_ms"] >0  )\
        #.filter(lambda l: len(json.loads(l)["place"]["country_code"]) > 0)\
        #.filter(lambda l: len(json.loads(l)["place"]["name"])>0 )\
        #.flatMap(lambda l: lambda_map_word_city(l) )\
        #.reduceByKey(lambda a,b: a+b)
    output2 = lines.flatMap(lambda l: lambda_map_word_city(l) )\
        .reduceByKey(lambda a,b: a+b)
    output2.foreachRDD(citycount_to_cassandra)


    #start the stream and keep it running - await for termination too.
    ssc.start()
    ssc.awaitTermination()
