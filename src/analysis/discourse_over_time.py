from lib.db.queries.tweet_queries import get_tweets_for_hashtags
from lib.db.connection import connect_to_mongo


if __name__ == "__main__":
    connect_to_mongo()
    firestorm_tweets = get_tweets_for_hashtags(['#studierenwieBaerbock'])
    print(type(firestorm_tweets))