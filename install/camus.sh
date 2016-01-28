#!/bin/bash

sudo apt-get install maven

cd /usr/local/
sudo git clone https://github.com/linkedin/camus.git
sudo chown -R ubuntu camus
cd ./camus/
mvn clean package
cd /usr/local/camus/camus-example/src/main/resources/


vim camus.properties
