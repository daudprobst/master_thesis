from connection import connect_to_mongo
from schemes import *

connect_to_mongo()
print(f'{len(Tweets.objects())} tweets were fetched')
print(f'{len(Users.objects())} users were fetched')

for tweet in Tweets.objects():
    print(tweet.to_json())

for user in Users.objects():
    print(user.to_json())
