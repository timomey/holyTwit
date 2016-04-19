#HolyTwit - real time reactions to current events using twitter

An Insight Data Engineering Project

The implemented version of this project can be seen at www.holytwit.club

(update: Since the core Insight program is over now, the AWS clusters are down now and thus www.holytwit.club can not be updated in real time anymore. But a static version still exists. Check it out! )

##What does it do?

On the input page (www.holytwit.club/input) the user can add any word (number of words) he or she is interested in.
Then Spark Streaming together with Elasticsearch Percolators start looking for these words in the real time twitter stream (or a significantly faster version of it) and find all tweets that contain any one of the words.
For every input word, the program aggregates and counts all other words that are tweeted together with the input and saves the connection to the first word. These are first degree connections to the input words.

##The Pipeline

Data from the real public twitter stream is sent to Kafka with a simple producer and once in a while saved to S3. This data was collected for a few weeks and accumulated to more than 500GB of twitter stream data. 

Ingestion: Kafka
