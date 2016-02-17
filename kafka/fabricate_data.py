#!/usr/bin/env python

#this file creates a "fabricated" file of tweets from a "clean" (as in clean_dump.py) file of tweets.
#(for nicer presentations of the app -> reliable and nice counts, graphs and second connections )
#input list of words. the script will only save the tweets that contain at least one of the words.
# save to ../../tweets/word1_word2_ ..


import json
import sys

if __name__ == "__main__":

	if len(sys.argv)<2:
		print 'need some words to fabricate data'
	words = []
	for i in range(1,len(sys.argv)):
		words.append(sys.argv[i])

	tweetfile = 'clean2_2016-02-08-11-57_tweets.txt'

	with open('../../tweets/'+tweetfile ,'r') as f:
		counter = 0
		for line in f:
			counter += 1
			#try:
			json_dict = json.loads(line.strip())

			if sum(map(lambda x: x in json_dict["text"], words)) > 0:
				with open('../../tweets/'+ '_'.join(words), 'a') as fabricated:
					fabricated.write(line)


			if counter%10000 ==0:
				print counter
