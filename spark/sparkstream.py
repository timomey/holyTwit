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
    #if text == "":
    #    raise BlankError('empty tweet')
    return text


#cassandra stuff:
def cassandra_create_keyspace(keyspacename,session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")

def cassandra_create_table(keyspacename, tablename, session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")
    session.execute("CREATE TABLE IF NOT EXISTS "+keyspacename+"."+tablename+" (wordofinterest text, time text, date text, location text, cowords_firstdegree text,tweet text, PRIMARY KEY ((wordofinterest, location, date), time)) WITH CLUSTERING ORDER BY (time DESC);")

def cassandra_create_citycount_table(keyspacename, tablename, session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")
    session.execute("CREATE TABLE IF NOT EXISTS "+keyspacename+"."+tablename+" (place text, count counter, PRIMARY KEY (place)); ")


def write_into_cassandra(record):
    #from cassandra.cluster import Cluster
    #from cassandra import ConsistencyLevel
    keyspacename = 'twitterimpact'
    #tablename = 'fakerealtime'
    tablename = wordofinterest
    # connect to cassandra
    #THIS ONE IS timo-kafka-cluster: cluster = Cluster(['ec2-52-35-24-163.us-west-2.compute.amazonaws.com','ec2-52-89-22-134.us-west-2.compute.amazonaws.com','ec2-52-34-117-127.us-west-2.compute.amazonaws.com','ec2-52-89-0-97.us-west-2.compute.amazonaws.com'])
    cluster = Cluster(['ec2-52-89-218-166.us-west-2.compute.amazonaws.com','ec2-52-88-157-153.us-west-2.compute.amazonaws.com','ec2-52-35-98-229.us-west-2.compute.amazonaws.com','ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])

    session = cluster.connect()
    cassandra_create_table(keyspacename,tablename, session)
    prepared_write_query = session.prepare("INSERT INTO "+keyspacename+"."+tablename+" (wordofinterest, time, date, location, cowords_firstdegree, tweet) VALUES (?,?,?,?,?,?)")
    for i in record:
        json_str = json.loads(i)
        #error_dict = {"keyerror": 0, "typeerror":0}

        try:
            if wordofinterest in json_str['text']:
                wordofinterest
                time = clean_string(str(json_str["timestamp_ms"]))
                date = timepackage.strftime('%Y-%m-%d %H:%M:%S',  timepackage.gmtime(int(time)/1000.))
                location = str(clean_string(json_str["place"]["name"])+', '+clean_string(json_str["place"]["country_code"]))
                cowords_firstdegree = str(clean_string(json_str['text'].encode('ascii','ignore')).split())
                tweet = str(clean_string(json_str['text'].encode('ascii','ignore')))
                session.execute(prepared_write_query, (wordofinterest, time, date, location, cowords_firstdegree, tweet))
        except (KeyError):
            #could implement counter here
            #error_dict["keyerror"] += 1
            #print json_str
            continue
        except TypeError:
            #this basically occurs when place.name does not exist.
            #error_dict["typeerror"] += 1
            #print json_str
            continue


def process(rdd):
    rdd.foreachPartition(lambda record: write_into_cassandra(record))

def citycount_to_cassandra(rdd):
    def update_to_cassandra(record):
        for element in record:
            print element[0][0]
            print element[0][1]
            print element[1]
            key = str(element[0][0]) + str(element[0][1])
            count = element[1]
            session.execute(prepared_write_query, (count, key) )
    keyspacename = 'twitterimpact'
    tablename = wordofinterest
    cluster = Cluster(['ec2-52-89-218-166.us-west-2.compute.amazonaws.com','ec2-52-88-157-153.us-west-2.compute.amazonaws.com','ec2-52-35-98-229.us-west-2.compute.amazonaws.com','ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    cassandra_create_citycount_table(keyspacename,tablename, session)
    prepared_write_query = session.prepare("UPDATE "+keyspacename+"."+tablename+" SET count = count + ? WHERE place=?")
    #prepared_write_query = session.prepare("INSERT INTO "+keyspacename+"."+tablename+" (place, count) VALUES (?,?)")
    rdd.foreachPartition(lambda ele: update_to_cassandra(ele))





if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description='Word of interest for TwitterImpact.')
    #parser.add.argument('word')
    #findword = define_the_search(wordofinterest)

    #main(sys.argv)


    wordofinterest = str(sys.argv[1])

    #wordofinterest = 'trump'

    sc = SparkContext(appName="TwitterImpact")
    ssc = StreamingContext(sc, 5)

    zkQuorum = "52.34.117.127:2181,52.89.22.134:2181,52.35.24.163:2181,52.89.0.97:2181"
    topic = "twitterdump_timo"
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 4})
    lines = kvs.map(lambda x: x[1])

    #1. filter: is the word in the tweet. 2.filter does it have a place name 3. filter does it have country country_code
    #4. map it to ((place.name, place.country_code),1).
    #5. reducebykey add a+b -> sum for each place.
    #def countcity(lines):
    output = lines.filter(lambda l: wordofinterest in json.loads(l)["text"])\
        .filter(lambda l: len(json.loads(l)["place"]["name"]) > 0 )\
        .filter(lambda l: len(json.loads(l)["place"]["country_code"]) > 0)\
        .map(lambda l: ( (json.loads(l)["place"]["name"], json.loads(l)["place"]["country_code"] ), 1))\
        .reduceByKey(lambda a,b: a+b)

    #output.pprint()

    output.foreachRDD(citycount_to_cassandra)


    ssc.start()
    ssc.awaitTermination()
