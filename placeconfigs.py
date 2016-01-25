#!/usr/bin/env python

from shutil import copyfile

copyfile('./configs/camus/camus.properties', '/usr/local/camus/camus-example/src/main/resources/')
copyfile('./configs/cassandra/address.yaml','/usr/local/datastax-agent/conf/')
