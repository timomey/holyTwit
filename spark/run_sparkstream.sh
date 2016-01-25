$SPARK_HOME/bin/spark-submit --executor-memory 10000M --driver-memory 10000M --master spark://ip-172-31-2-200:7077 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 ./sparkstream.py 
