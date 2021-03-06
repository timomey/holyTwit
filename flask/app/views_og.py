from app import app
from flask import jsonify
from cassandra.cluster import Cluster
from flask import render_template, Flask, flash, request
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
from subprocess import call
import time as timepackage
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
import kafka
import json
import os

# App config.
#DEBUG = True
#app = Flask(__name__)
#app.config.from_object(__name__)
#app.config['SECRET_KEY'] = '7d441f27d441f27567d441f2b6176a'


def write_input_to_cass(inp):
    #cluster = Cluster(['ec2-52-33-153-115.us-west-2.compute.amazonaws.com','ec2-52-36-102-156.us-west-2.compute.amazonaws.com'])
    cluster = Cluster([
        'ec2-52-36-123-77.us-west-2.compute.amazonaws.com',
        'ec2-52-36-185-47.us-west-2.compute.amazonaws.com',
        'ec2-52-26-37-207.us-west-2.compute.amazonaws.com',
        'ec2-52-33-125-6.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    inputlist = inp.split()
    prepared_write_query = session.prepare("INSERT INTO holytwit.listofwords (word, numberofwords, time) VALUES (?,?,?) USING TTL 600;")
    for inputword in inputlist:
        word = inputword
        numberofwords = 1
        time = int(timepackage.time()*1000)
        session.execute(prepared_write_query,(word,numberofwords,time))

def kafka_producer(input_str):
    #KAFKA PRODUCER STUFF
    topic = 'elasticquerries'
    cluster = kafka.KafkaClient("ip-172-31-2-200:9092,ip-172-31-2-201:9092,ip-172-31-2-202:9092,ip-172-31-2-203:9092")
    prod = kafka.SimpleProducer(cluster, async = False, batch_send_every_n = 1)
    prod.send_messages(topic, input_str.encode('utf-8'))


def add_current_top10_toES():
    es = Elasticsearch(hosts=[{"host":"ip-172-31-2-202", "port":9200},{"host":"ip-172-31-2-201", "port":9200},{"host":"ip-172-31-2-200", "port":9200},{"host":"ip-172-31-2-203", "port":9200}] )
    #pers = es.search(index='twit',doc_type='.percolator')
    #listof_words_in_es = map(lambda x: str(x['_source']['query']['match']['message']), pers['hits']['hits'])
    #es.create(index='twitdeg2', doc_type='.percolator', body={'query': {'match': {'message': word   }} } )

    cluster = Cluster([
        'ec2-52-36-123-77.us-west-2.compute.amazonaws.com',
        'ec2-52-36-185-47.us-west-2.compute.amazonaws.com',
        'ec2-52-26-37-207.us-west-2.compute.amazonaws.com',
        'ec2-52-33-125-6.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    #to create 2. deg connections
    listof_ogwords = []
    with open('words.txt','r') as words:
        for line in words:
            listof_ogwords.append(line.strip())
    listof_deg2words = []
    os.system('rm deg2.txt')
    for words in listof_ogwords:
        hashtagsmt = "SELECT count,degree1 FROM holytwit.highestconnection WHERE word='"+str(words)+"' LIMIT 10;"
        response_degree = session.execute(hashtagsmt)
        response_hashtags_list = []
        for val in response_degree:
            response_hashtags_list.append(val)

        top10_connections = [x.degree1 for x in response_hashtags_list if x.degree1 not in listof_ogwords]
        top10_connections = [word for word in top10_connections if word not in listof_deg2words]
        with open('deg2.txt','a') as deg2:
            for w in top10_connections:
                kafka_producer(w)
                deg2.write(w)
                deg2.write('\n')



class ReusableForm(Form):
    input = TextField('Input:', validators=[validators.required()])


@app.route('/')
def home():
    return render_template("home.html")

@app.route('/index')
def index():
    return render_template("home.html")

@app.route('/slides')
def slides():
    return render_template("slides.html")


@app.route('/_addsecdeg')
def addsecde():
    add_current_top10_toES()
    render_template('success.html')

@app.route('/_triggerwordres', methods=['GET', 'POST'])
def triggertableres():
    es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )
    #ELASTICSEARCH STUFF
    es.indices.delete(index='twit', ignore=400)
    #create index for ES (ignore if it already exists       )
    es.indices.create(index='twit', ignore=400, body={
          "mappings": {
            "document": {
              "properties": {
                "message": {
                  "type": "string"
                }
              }
            }
          }
        }
    )

    os.system('rm words.txt')
    flash(' >>>>>>>>> all words have been reset! Have fun with some new ones, try it again!')
    return render_template("success.html")

@app.route('/api/place/<word>')
def place_word_api(word):
    cluster = Cluster([
        'ec2-52-36-123-77.us-west-2.compute.amazonaws.com',
        'ec2-52-36-185-47.us-west-2.compute.amazonaws.com',
        'ec2-52-26-37-207.us-west-2.compute.amazonaws.com',
        'ec2-52-33-125-6.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    stmt = "SELECT count,place FROM holytwit.citycount WHERE word='"+str(word)+"' LIMIT 10;"
    response = session.execute(stmt)
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [ {'name': str(x.place), 'y': x.count, 'drilldown': 'null' } for x in response_list]
    return jsonify(data=jsonresponse)

@app.route('/api/hashtags/<word>')
def hashtag_word_api(word):
    cluster = Cluster([
        'ec2-52-36-123-77.us-west-2.compute.amazonaws.com',
        'ec2-52-36-185-47.us-west-2.compute.amazonaws.com',
        'ec2-52-26-37-207.us-west-2.compute.amazonaws.com',
        'ec2-52-33-125-6.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    hashtagsmt = "SELECT count,degree1 FROM holytwit.highestconnection WHERE word='"+str(word)+"' LIMIT 10;"
    response_degree = session.execute(hashtagsmt)
    response_hashtags_list = []
    for val in response_degree:
        response_hashtags_list.append(val)
    response_hashtags_list = [ {'name': str(x.degree1), 'y': x.count, 'drilldown': 'null'} for x in response_hashtags_list ]
    return jsonify(data=response_hashtags_list)


@app.route('/input', methods=['GET', 'POST'])
def citycount():
    es = Elasticsearch(hosts=[{"host":"ip-172-31-2-202", "port":9200},{"host":"ip-172-31-2-201", "port":9200},{"host":"ip-172-31-2-200", "port":9200},{"host":"ip-172-31-2-203", "port":9200}] )
    form = ReusableForm(request.form)
    print form.errors

    #pers = es.search(index='twit',doc_type='.percolator')
    #listof_words_in_es = map(lambda x: str(x['_source']['query']['match']['message']), pers['hits']['hits'])
    listof_words_in_es = []
    try:
        with open('words.txt','r') as words:
            for line in words:
                listof_words_in_es.append(line.strip())
    except:
        pass
    if request.method == 'POST':
        input=request.form['input']
        print ' > looking for ' + input +' in the incoming twitterstream'

        for w in input.split():
            with open('words.txt','a') as wordlist:
                wordlist.write(w)
                wordlist.write('\n')

        kafka_producer(input)

        if form.validate():
            flash(' >>>>>>>>> looking for ' + input +' in the incoming twitterstream')

        else:
            flash('Error: All the form fields are required. ')
    return render_template("input.html", form=form, currently_tracked_words = listof_words_in_es)


@app.route('/output/')
def get_stream():
    #get the words from ES
    #es = Elasticsearch(hosts=[{"host":"ip-172-31-2-202", "port":9200},{"host":"ip-172-31-2-201", "port":9200},{"host":"ip-172-31-2-200", "port":9200},{"host":"ip-172-31-2-203", "port":9200}] )
    #pers = es.search(index='twit',doc_type='.percolator')
    #listof_words_in_es = map(lambda x: str(x['_source']['query']['match']['message']), pers['hits']['hits'])
    listof_words_in_es = []
    with open('words.txt','r') as words:
        for line in words:
            listof_words_in_es.append(line.strip())
    #Cassandra connection:
    cluster = Cluster([
        'ec2-52-36-123-77.us-west-2.compute.amazonaws.com',
        'ec2-52-36-185-47.us-west-2.compute.amazonaws.com',
        'ec2-52-26-37-207.us-west-2.compute.amazonaws.com',
        'ec2-52-33-125-6.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    hashtagdata = {}
    placesdata = {}
    deg2_visuals_dict = {}
    for words in listof_words_in_es:
        get_place_stmt = "SELECT count,place FROM holytwit.citycount WHERE word='"+str(words)+"' LIMIT 10;"
        hashtagsmt = "SELECT count,degree1 FROM holytwit.highestconnection WHERE word='"+str(words)+"' LIMIT 10;"
        response = session.execute(get_place_stmt)
        response_degree = session.execute(hashtagsmt)
        response_hashtags_list = []
        for val in response_degree:
            response_hashtags_list.append(val)
        response_list = []
        for val in response:
            response_list.append(val)

        response_places = [ {'name': str(x.place), 'y': x.count, 'drilldown': 'null' } for x in response_list]
        placesdata[words+'place'] = response_places
        response_hashtags_list_data = [ {'name': str(x.degree1), 'y': x.count, 'drilldown': str(x.degree1)} for x in response_hashtags_list ]
        hashtagdata[words+'connection'] = response_hashtags_list_data

        top10_connections = [x.degree1 for x in response_hashtags_list ]
        deg2_visuals = []
        for deg1 in top10_connections:
            try:
                deg2 = "SELECT count,degree1 FROM holytwit.highestconnection WHERE word='"+str(deg1)+"' LIMIT 10;"
                response_deg2 = session.execute(deg2)
                response_deg2_list =[]
                for val in response_deg2:
                    response_deg2_list.append(val)
                drilldown_data = [[str(x.degree1), x.count] for x in response_deg2_list]
                deg2_visuals.append({'name': str(deg1), 'id': str(deg1), 'data': drilldown_data})
            except:
                pass
        #put all deg2_visuals into one dictionary
        deg2_visuals_dict[words+'deg2'] = deg2_visuals
    return render_template("output.html", data_places = placesdata, data_hashtags = hashtagdata, list_of_words = listof_words_in_es, data_deg2 = deg2_visuals_dict)
