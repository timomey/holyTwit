#!/usr/bin/env python

# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
from pprint import pprint
import json
import os
import kafka


# loads Twitter credentials from .twitter file that is in the same directory as this script
file_dir = os.path.dirname(os.path.realpath(__file__))
with open(file_dir + '/.twitter') as twitter_file:
    twitter_cred = json.load(twitter_file)

# authentication from the credentials file above
access_token = twitter_cred["access_token"]
access_token_secret = twitter_cred["access_token_secret"]
consumer_key = twitter_cred["consumer_key"]
consumer_secret = twitter_cred["consumer_secret"]

class StdOutListener(StreamListener):
    """ A listener handles tweets that are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, filename):
        self.filename = filename

    # this is the event handler for new data
    def on_data(self, data):
        if not os.path.isfile(self.filename):    # check if file doesn't exist
            f = file(self.filename, 'w')
            f.close()
        with open(self.filename, 'ab') as f:
            print "writing to {}".format(self.filename)
            f.write(data)
        #f.closed #results into True if the file is closed.

        # this is the event handler for errors
        def on_error(self, status):
            print('ERRORSTATUS')
            print(status)

class KafkaListener(StreamListener):
    """ A listener handles tweets that are the received from the stream.
    This is a basic listener that saves tweeots to kafka.
    """
    def __init__(self,producer):
        #tweetlist can be used to store some more tweets and push them out at once instead of individually
        #self.tweetlist = list()
        self.producer = producer

    #event handler for new data
    def on_data(self, data):
        topic = "tweets2"
        try:
            json_dict = json.loads(data.strip())
            json_dict['created_at']

            tweet = data.strip()
            prod = self.producer
            prod.send_messages(topic, tweet.encode('utf-8'))#json.dumps(json_dict))

        except:
            print 'exception'
            print json.loads(data.strip())




if __name__ == '__main__':
    #kafka cluster and producer
    cluster = kafka.KafkaClient("ip-172-31-2-200:9092,ip-172-31-2-201:9092,ip-172-31-2-202:9092,ip-172-31-2-203:9092")
    prod = kafka.SimpleProducer(cluster, async = True, batch_send_every_n=20)

    #listener = StdOutListener(file_dir + "/tweets.txt")
    listener = KafkaListener(prod)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print "Use CTRL + C to exit at any time.\n"
    stream = Stream(auth, listener)
    stream.filter(locations=[-180,-90,180,90]) # this is the entire world, any tweet with geo-location enabled
