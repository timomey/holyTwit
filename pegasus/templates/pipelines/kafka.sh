#!/bin/bash

CLUSTER_NAME=timo-kafka-cluster
REGION=us-west-2

./ec2spinup templates/instances/kafka.json

./ec2fetch $REGION $CLUSTER_NAME

./ec2install $CLUSTER_NAME environment
./ec2install $CLUSTER_NAME ssh
./ec2install $CLUSTER_NAME aws
./ec2install $CLUSTER_NAME zookeeper
./ec2install $CLUSTER_NAME kafka
#./ec2install $CLUSTER_NAME hadoop

#ONLY AFTER ADDING SSH CONNECTIONS TO CONFIG (WRITE A SCRIPT)
scp -r ~/Google\ Drive/InsightDateEngineering/twitter_stream_listener/insight_twitter_listener/ kafka1:~
