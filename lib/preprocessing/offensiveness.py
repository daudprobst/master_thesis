from lib.twitter_data.tweets_in_phases import TweetsInPhases
from lib.db.connection import connect_to_mongo
import pandas as pd
from lib.db.queried import QUERIES as queried_firestorms



if __name__ == "__main__":

    connect_to_mongo()

    # select firestorm
    firestorm_meta = queried_firestorms['pinkygloves']
    ####

    print(firestorm_meta['query'],  firestorm_meta['true_start_date'], firestorm_meta['true_end_date'])

    firestorm_tweets = TweetsInPhases.from_hashtag_in_query(firestorm_meta['query']).select_time_range(
        firestorm_meta['true_start_date'], firestorm_meta['true_end_date']
    )


    print(f'Firestorm has {len(firestorm_tweets)} phases and {len(firestorm_tweets.tweets)} tweets')