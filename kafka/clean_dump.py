#!/usr/bin/env python

import kafka
import json
import sys
import os

if __name__ == "__main__":
    folder = sys.argv[1]
    path = '../../tweets/'+folder
    counter = 0
    for path, subdirs, files in os.walk(path):
        for name in files:
            tweetfile = os.path.join(path, name)
            #tweetfile = '2016-02-08-11-57_tweets.txt'
            with open(str(tweetfile) ,'r') as f:
                with open('../../tweets/clean_'+folder.txt,'a') as writ:
                    for line in f:
                        counter += 1
                        json_dict = json.loads(line.strip())
                        try:
                            text = str(json_dict['text'].encode('ascii','ignore'))
                            place = str(json_dict['place']['name'].encode('ascii','ignore') +", "+ json_dict['place']['country_code'].encode('ascii','ignore'))
                            time = json_dict['timestamp_ms']
                            hashtags = [hash.split()[0] for hash in text.split('#')[1:]]
                        except:
                            pass
                        else:
                            json_output = {'text': text,'hashtags': hashtags ,'place': place, 'time': time}
                            writ.write(json.dumps(json_output))
                            writ.write('\n')

                        if counter%10000 ==0:
                            print counter
