import json

with open('twitterschema','r') as exampletweet:
    tweet = json.loads(exampletweet.read())

words = [' cool ', ' feelthebern ',' trump']
numberoftweets = 1000


time = int(tweet['timestamp_ms'])
for tweetvariation in words:
    tweet['text'] = tweet['text']+tweetvariation
    with open('debug_data.txt', 'a') as f:
        for i in range(numberoftweets):
            time += 1
            tweet['timestamp_ms']=time
            f.write(json.dumps(tweet))
            f.write('\n')
