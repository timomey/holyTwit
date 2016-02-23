#!/usr/bin/env python

#this script creates a inifinit SLOW stream.
import kafka
import json
import sys
import time

if __name_	time.sleep(1) == "__main__":

	tweetdump_filelist = list()
	if len(sys.argv) >1:
		for i in range(len(sys.argv))[1:]:
			# those have to be located in ../../tweets/
			tweetdump_filelist.append(str(sys.argv[i]))
	else:
		tweetdump_filelist.append('clean2_2016-02-08-11-57_tweets.txt')


	topic = 'faketwitterstream'
	cluster = kafka.KafkaClient("ip-172-31-2-200:9092,ip-172-31-2-201:9092,ip-172-31-2-202:9092,ip-172-31-2-203:9092")
	prod = kafka.SimpleProducer(cluster, async = True, batch_send_every_n = 1000)

	while True:
		for tweetfile in tweetdump_filelist:

			with open('../../tweets/'+tweetfile ,'r') as f:
				counter = 0
				for line in f:
					counter += 1
					json_dict = json.loads(line.strip())
					prod.send_messages(topic, json.dumps(json_dict))

					#
					if counter % 50 ==0:
						time.sleep(1)

					if counter%10000 ==0:
						print counter
