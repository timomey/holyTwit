from app import app
from flask import jsonify
from cassandra.cluster import Cluster

cluster = Cluster(['ec2-52-89-218-166.us-west-2.compute.amazonaws.com','ec2-52-88-157-153.us-west-2.compute.amazonaws.com','ec2-52-35-98-229.us-west-2.compute.amazonaws.com','ec2-52-34-216-192.us-west-2.compute.amazonaws.com'])
#cluster = Cluster(['ec2-52-35-24-163.us-west-2.compute.amazonaws.com'])
session = cluster.connect('twitterimpact')

@app.route('/')
@app.route('/index')
def index():
   return "Hello, World!"

#@app.route('/api/demo1/<topic>')
#def get_topic(topic):
#	stmt = "SELECT * FROM demo WHERE topic='#"+topic+"'"
#	response = session.execute(stmt)
#	response_list = []
#	for val in response:
#		response_list.append(val)
#	jsonresponse = [{"Topic": x.topic, "connections": x.connections, "country": x.country, "time": x.time,} for x in response_list]
#	return jsonify(emails=jsonresponse)

@app.route('/api/streamtweetcount')
def get_stream():
        stmt = "SELECT count(*) FROM fakerealtime"
        response = session.execute(stmt)
        response_list = []
        for val in response:
                response_list.append(val)
        #jsonresponse = [{"city": x.city, "tweet": x.country, "time": x.time,} for x in response_list]
        jsonresponse = [{"count": x.count} for x in response_list]
        return jsonify(tweet=jsonresponse)
