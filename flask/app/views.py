from app import app
from flask import jsonify
from cassandra.cluster import Cluster
from flask import render_template
from subprocess import call
import time



cluster = Cluster(['ec2-52-89-218-166.us-west-2.compute.amazonaws.com','ec2-52-88-157-153.us-west-2.compute.amazonaws.com','ec2-52-35-98-229.us-west-2.compute.amazonaws.com','ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
session = cluster.connect('twitterimpact')

@app.route('/')
def home():
    return render_template("home.html")

@app.route('/index')
def index():
    return render_template("home.html")

@app.route('/slides')
def slides():
    return render_template("slides.html")

@app.route('/citycount')
def citycount():
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
