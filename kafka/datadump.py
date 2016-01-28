import kafka
import json





topic = 'twitterdump_timo'
cluster = kafka.KafkaClient("ip-172-31-2-200:9092,ip-172-31-2-201:9092,ip-172-31-2-202:9092,ip-172-31-2-203:9092")
prod = kafka.SimpleProducer(cluster, async = True, batch_send_every_n = 5)





with open('../../tweets/tweetsfirstbatch.txt','r') as f:
        counter = 0
        for line in f:
            counter += 1
            try:
                json_dict = json.loads(line.strip())
                json_dict['created_at']
                #print json.dumps(json_dict)

                tweet = line.strip()
                prod.send_messages(topic, json.dumps(json_dict))#tweet.encode('utf-8'))

            except:
                continue


            if counter%10000 ==0:
                print counter
