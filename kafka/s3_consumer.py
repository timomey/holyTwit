import os
import boto3

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

bucket_name = 	"timo-raw-twitter"
folder_name = "twitter/"
file_name = "tweets.json"
#NOTHING WORKS YET

client = boto3.client('s3')

s3 = boto3.resource('s3')

conn = boto.connect_s3(aws_access_key, aws_secret_access_key)

bucket = conn.get_bucket(bucket_name)
key = bucket.get_key(folder_name + file_name)

data = key.get_contents_as_string()
