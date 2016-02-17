from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
import kafka
from cassandra.cluster import Cluster
import json

def kafka_producer(input_str):
    #KAFKA PRODUCER STUFF
    topic = 'elasticquerries'
    cluster = kafka.KafkaClient("ip-172-31-2-200:9092,ip-172-31-2-201:9092,ip-172-31-2-202:9092,ip-172-31-2-203:9092")
    prod = kafka.SimpleProducer(cluster, async = False, batch_send_every_n = 1)
    prod.send_messages(topic, input_str.encode('utf-8'))

def add_current_top10_toES():
    es = Elasticsearch(hosts=[{"host":"ip-172-31-2-202", "port":9200},{"host":"ip-172-31-2-201", "port":9200},{"host":"ip-172-31-2-200", "port":9200},{"host":"ip-172-31-2-203", "port":9200}] )
    pers = es.search(index='twit',doc_type='.percolator')
    listof_words_in_es = map(lambda x: str(x['_source']['query']['match']['message']), pers['hits']['hits'])
    for words in listof_words_in_es:
        hashtagsmt = "SELECT count,degree1 FROM holytwit.highestconnection WHERE word='"+str(words)+"' LIMIT 10;"
        response_degree = session.execute(hashtagsmt)
        response_hashtags_list = []
        for val in response_degree:
            response_hashtags_list.append(val)

        top10_connections = [x.degree1 for x in response_hashtags_list if x.degree1 not in listof_words_in_es]
        for w in top10_connections:
            kafka_producer(w)


if __name__=="__main__":
    add_current_top10_toES()
