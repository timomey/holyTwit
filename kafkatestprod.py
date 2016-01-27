import kafka

topic = "my-topic"
cluster = kafka.KafkaClient("ip-172-31-2-200:9092,ip-172-31-2-201:9092,ip-172-31-2-202:9092,ip-172-31-2-203:9092")
prod = kafka.SimpleProducer(cluster, async = True, batch_send_every_n = 500) 

while 1:
    prod.send_messages(topic, 'this is a string how does this go')
