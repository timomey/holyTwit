#!/usr/bin/env python

import kafka
import json
import sys

if __name__ == "__main__":
    tweetfile = '2016-02-08-11-57_tweets.txt'
    with open('../../tweets/'+tweetfile ,'r') as f:
        counter = 0
        with open('../../tweets/clean_'+tweetfile,'a') as writ:
            for line in f:
                counter += 1

                json_dict = json.loads(line.strip())
                try:
                    if len(json_dict['text'])>2 and json_dict["place"]["name"] and json_dict["place"]["country_code"] and tweet['timestamp_ms']:
                        text = str(json_dict['text'].encode('ascii','ignore'))
                        place = str(json_dict['place']['name'].encode('ascii','ignore') + json_dict['place']['country_code'].encode('ascii','ignore'))
                        time = tweet['timestamp_ms']
                        try:
                            hashtags = [hash.split()[0] for hash in text.split('#')[1:]]
                        except IndexError:
                            pass
                        json_output = {'text': text,'hashtags': hashtags ,'place': place, 'time': time}
                        writ.write(json.dumps(json_output))
                        writ.write('\n')
                except:
                    pass

                if counter%10000 ==0:
                    print counter
