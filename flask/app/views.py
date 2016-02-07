from app import app
from flask import jsonify
from cassandra.cluster import Cluster
from flask import render_template, Flask, flash, request
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
from subprocess import call
import time as timepackage


# App config.
#DEBUG = True
#app = Flask(__name__)
#app.config.from_object(__name__)
#app.config['SECRET_KEY'] = '7d441f27d441f27567d441f2b6176a'


def cassandra_create_keyspace(keyspacename,session):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")


def cassandra_create_listofwords_table():
    #list of words meintained in cassandra so that there can be multiple
    cluster = Cluster(['ec2-52-89-218-166.us-west-2.compute.amazonaws.com','ec2-52-88-157-153.us-west-2.compute.amazonaws.com','ec2-52-35-98-229.us-west-2.compute.amazonaws.com','ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect('twitterimpact')
    cassandra_create_keyspace('holytwit', session)
    session.execute("CREATE TABLE IF NOT EXISTS holytwit.listofwords \
                    (word text, numberofwords int, time timestamp, \
                    PRIMARY KEY (word,time ));")

def write_input_to_cass(inp):
    cluster = Cluster(['ec2-52-89-218-166.us-west-2.compute.amazonaws.com','ec2-52-88-157-153.us-west-2.compute.amazonaws.com','ec2-52-35-98-229.us-west-2.compute.amazonaws.com','ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
    session = cluster.connect('twitterimpact')
    inputlist = inp.split()
    prepared_write_query = session.prepare("INSERT INTO holytwit.listofwords (word, numberofwords, time) VALUES (?,?,?) USING TTL 60;")
    for inputword in inputlist:
        word = inputword
        numberofwords = 1
        time = int(timepackage.time()*1000)
        session.execute(prepared_write_query,(word,numberofwords,time))


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
        write_input_to_cass(input)


        if form.validate():
            # Save the comment here.
            flash(' >>>>>>>>> looking for ' + input +' in the incoming twitterstream')

        else:
            flash('Error: All the form fields are required. ')
    return render_template("citycountinput.html", form=form)


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
