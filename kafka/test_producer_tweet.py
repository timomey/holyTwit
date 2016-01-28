import kafka

topic = "twitterdump_timo"
cluster = kafka.KafkaClient("ip-172-31-2-200:9092,ip-172-31-2-201:9092,ip-172-31-2-202:9092,ip-172-31-2-203:9092")
prod = kafka.SimpleProducer(cluster, async = True, batch_send_every_n = 500)

exampletweet = '{"created_at": "Tue Jan 26 00:02:05 +0000 2016", "text": "trump is an idiot", "timestamp_ms": "1453766525705", "place": {"id": "5e02a0f0d91c76d2", "url": "https://api.twitter.com/1.1/geo/id/5e02a0f0d91c76d2.json", "place_type": "city", "name": "\u0130stanbul", "full_name": "\u0130stanbul, T\u00fcrkiye", "country_code": "TR" }'
while 1:
    prod.send_messages(topic, exampletweet)
