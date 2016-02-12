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

# App config.
#DEBUG = True
#app = Flask(__name__)
#app.config.from_object(__name__)
#app.config['SECRET_KEY'] = '7d441f27d441f27567d441f2b6176a'

class userhelper:
    def __init__(self):
        self.wordlist = []
        self.seconddegree = []

    def add_words(self, stringofwords):
        self.wordlist = self.wordlist + stringofwords.lower().split()

    def reset(self):
        self.wordlist = []

    def add_second_degree(self, listofwords):
        self.seconddegree = self.seconddegree + listofwords

def write_input_to_cass(inp):
    cluster = Cluster(['ec2-52-33-153-115.us-west-2.compute.amazonaws.com','ec2-52-36-102-156.us-west-2.compute.amazonaws.com'])
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

@app.route('/input', methods=['GET', 'POST'])
def citycount():
    form = ReusableForm(request.form)
    print form.errors
    if 'user' in vars():
        user.add_words(input)
    else:
        user = userhelper()
    if request.method == 'POST':
        input=request.form['input']
        print ' > looking for ' + input +' in the incoming twitterstream'
        #cassandra_create_listofwords_table()

        user.add_words(input)
        kafka_producer(input)


        if form.validate():
            # Save the comment here.
            flash(' >>>>>>>>> looking for ' + input +' in the incoming twitterstream')

        else:
            flash('Error: All the form fields are required. ')
    return render_template("input.html", form=form, currently_tracked_words = user.wordlist)

@app.route('/_triggerwordres', methods=['GET', 'POST'])
def triggertableres():
    form = ReusableForm(request.form)
    print form.errors
    if request.method == 'POST':
        input=request.form['input']
        print ' > looking for ' + input +' in the incoming twitterstream'
        #cassandra_create_listofwords_table()
        kafka_producer(input)


        if form.validate():
            # Save the comment here.
            flash(' >>>>>>>>> looking for ' + input +' in the incoming twitterstream')

        else:
            flash('Error: All the form fields are required. ')

    if user:
        user.reset()

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
    flash(' >>>>>>>>> all words have been reset! Have fun with some new ones, try it again!')
    return render_template("citycountinput.html", form=form)

@app.route('/api/place/<word>')
def place_word_api(word):
    cluster = Cluster(['ec2-52-33-153-115.us-west-2.compute.amazonaws.com','ec2-52-36-102-156.us-west-2.compute.amazonaws.com'])
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
    cluster = Cluster(['ec2-52-33-153-115.us-west-2.compute.amazonaws.com','ec2-52-36-102-156.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    hashtagsmt = "SELECT count,degree1 FROM holytwit.highestconnection WHERE word='"+str(word)+"' LIMIT 10;"
    response_degree = session.execute(hashtagsmt)
    response_hashtags_list = []
    for val in response_degree:
        response_hashtags_list.append(val)
    response_hashtags_list = [ {'name': str(x.degree1), 'y': x.count, 'drilldown': 'null'} for x in response_hashtags_list ]
    return jsonify(data=response_hashtags_list)



@app.route('/output/')
def get_stream():
    #should get the words here automatically from elasticsearch
    cluster = Cluster(['ec2-52-33-153-115.us-west-2.compute.amazonaws.com','ec2-52-36-102-156.us-west-2.compute.amazonaws.com'])
    session = cluster.connect()
    maxnumpanels = 10
    hashtagdata = {}
    placesdata = {}
    for words in user.wordlist:
        stmt = "SELECT count,place FROM holytwit.citycount WHERE word='"+str(words)+"' LIMIT 10;"
        hashtagsmt = "SELECT count,degree1 FROM holytwit.highestconnection WHERE word='"+str(words)+"' LIMIT 10;"
        response = session.execute(stmt)
        response_degree = session.execute(hashtagsmt)
        response_hashtags_list = []
        for val in response_degree:
            response_hashtags_list.append(val)
        response_list = []
        for val in response:
            response_list.append(val)

        response_places = [ {'name': str(x.place), 'y': x.count, 'drilldown': 'null' } for x in response_list]
        placesdata[words] = response_places
        response_hashtags_list = [ {'name': str(x.degree1), 'y': x.count, 'drilldown': 'null'} for x in response_hashtags_list ]
        hashtagdata[words] = response_hashtags_list
        top10_connections = [x.degree1 for x in response_hashtags_list]
        #send the top10 connections back to ELASTICSEARCH
        user.add_second_degree(top10_connections)

    return render_template("output.html", data_places = response_places, data_hashtags = response_hashtags_list)

    #jsonresponse = [{"place": x.place, "count": x.count} for x in response_list]
    #return jsonify(wordofinterest=jsonresponse)
