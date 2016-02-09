#!/usr/bin/env python
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan


es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )
#ELASTICSEARCH STUFF
es.indices.delete(index='twit', ignore=400)
print 'index "twit" deleted from elasticsearch cluster.'
#create index for ES (ignore if it already exists       )
es.indices.create(index='twit', ignore=400, body={
      "mappings": {
        "document": {
          "properties": {
            "message": {
              "type": "string"
            }
          }
        }
      }
    }
)
print 'new and empty index "twit" created on elastic search cluster.'

#other stuff:
#es.create(index='twit', doc_type='.percolator', body={'query': {'match': {'message': word  }}})
