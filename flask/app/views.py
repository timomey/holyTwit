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

def write_input_to_cass(inp):
    cluster = Cluster(['ec2-52-89-218-166.us-west-2.compute.amazonaws.com','ec2-52-88-157-153.us-west-2.compute.amazonaws.com','ec2-52-35-98-229.us-west-2.compute.amazonaws.com','ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
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

@app.route('/citycount', methods=['GET', 'POST'])
def citycount():
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
    return render_template("citycountinput.html", form=form)

@app.route('/triggertableres', methods=['GET', 'POST'])
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
    flash(' >>>>>>>>> all tables reset! have fun with new ones! ')
    return render_template("citycountinput.html")




@app.route('/citycount/<wordofinterest>')
def get_stream(wordofinterest):
    maxnumpanels = 10
    stmt = "SELECT * FROM holytwit.city_count WHERE wordofinterest='"+str(wordofinterest)+"';"

    response = session.execute(stmt)
    response_list = []
    for val in response:
        response_list.append(val)
    #responsetuple = [('sf',4),('nyc',3),('la',7),('lv',1),('sss',8),('pa',94),('b',24),('rrsf',14),('tai',22),('berlin',411),('toront',42123),('peter',987)]

    responsetuple = [ (x.place, x.count) for x in response_list]
    responsetuple.sort(key= lambda x: x[1], reverse=True)
    if len(responsetuple)>maxnumpanels:
        citycountlist = responsetuple[:maxnumpanels]
    else:
        citycountlist = responsetuple[:len(responsetuple)]

    return render_template("citycount.html", citycounttuplelist = citycountlist)

    #jsonresponse = [{"place": x.place, "count": x.count} for x in response_list]
    #return jsonify(wordofinterest=jsonresponse)
