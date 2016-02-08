from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan


es = Elasticsearch(hosts=[{"host":"52.34.117.127", "port":9200},{"host":"52.89.22.134", "port":9200},{"host":"52.35.24.163", "port":9200},{"host":"52.89.0.97", "port":9200}] )


#ELASTICSEARCH STUFF
es.indices.delete(index='twit', ignore=400)
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


scroll = elasticsearch.helpers.scan(es, query='{"fields": "_id"}', index='twit', scroll='10s')
for res in scroll:
    print res['_id']

es.create(index='twit', doc_type='.percolator', body={'query': {'match': {'message': word  }}}, id=idcounter)
