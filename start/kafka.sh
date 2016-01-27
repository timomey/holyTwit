#!/bin/bash

sudo /usr/local/zookeeper/bin/zkServer.sh start

sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &

$KAFKA_MANAGER_HOME/bin/kafka-manager -Dhttp.port=9001
