from connection import connect_to_mongo
from schemes import *

connect_to_mongo()
print(Tweets.objects())

for tweet in Tweets.objects():
    print(tweet.to_json())
