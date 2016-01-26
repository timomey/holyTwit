from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel


cluster = Cluster(['ec2-52-35-24-163.us-west-2.compute.amazonaws.com','ec2-52-89-22-134.us-west-2.compute.amazonaws.com','ec2-52-34-117-127.us-west-2.compute.amazonaws.com','ec2-52-89-0-97.us-west-2.compute.amazonaws.com'])
session = cluster.connect()

def cassandra_create_keyspace(keyspacename):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")

def cassandra_create_table(keyspacename, tablename):
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspacename+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};")
    session.execute("CREATE TABLE IF NOT EXISTS "+keyspacename.tablename+" (wordofinterest text, time text, date text, location text, cowords_firstdegree text, PRIMARY KEY ((wordofinterest, location, date), time))) WITH CLUSTERING ORDER BY (time DESC);")
