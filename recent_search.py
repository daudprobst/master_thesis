from requests_base import make_request, create_params
from time import sleep
from typing import Tuple, Optional, List, Dict
from datetime import datetime, timedelta, timezone
from db.schemes import *
from db.connection import connect_to_mongo
from json import dumps
import os
import csv

from pprint import pprint

RECENT_SEARCH_URL = 'https://api.twitter.com/2/tweets/search/recent'



def recent_search(params: dict, headers: dict = None,
                  tweet_fetch_limit: int = 1000) -> Dict:
    """
    :param params: the parameters for the Twitter search (use create_params() to build this)
    :param headers: Headers for connecting to Twitter API; default values are set in requests_base
    :param tweet_fetch_limit: if more than this amount of tweets has been fetched, the search is stopped
    :return:
    """

    fetch_report = {
        "tweet_fetch_limit": tweet_fetch_limit,
        "started_fetching": datetime.now().isoformat(),
        "finished_fetching": '',
        "fetched_total": 0,
        "next_token": '',
        "params": params
    }
    fetched_total = 0
    next_token = None

    # Paginating through the result if there is more than the 100 limit can provide
    while next_token or fetched_total <= 0:
        if next_token:  # always the case except for the first request
            params['next_token'] = next_token

        response = make_request(RECENT_SEARCH_URL, params, headers)

        print(f'Fetched some new tweets! {response.meta()}')
        if(response.meta()['result_count'] == 0 and fetched_total == 0):
            print("No results were found for this query!")
            break

        fetched_total += response.meta()['result_count']

        response.write_to_db()
        response.write_to_csv('data/egge.csv')

        next_token = response.next_token()

        if fetched_total >= tweet_fetch_limit:
            print(f'Fetched at least as much tweets as the fetch limit.'
                  f'Aborting process here but returning last next_token')
            break

        if next_token:
            # Wait for 6s so there are not too many requests in a short time (API limit from twitter)
            sleep(6)

    fetch_report['finished_fetching'] = datetime.now().isoformat()
    fetch_report['fetched_total'] = fetched_total
    fetch_report['next_token'] = next_token
    return fetch_report


if __name__ == "__main__":
    a_while_ago = datetime.now(timezone.utc) - timedelta(hours=5)
    # TODO -> Would be nice to automatically read the last timestamp for a specific hashtag and then
    #  continue fetching from there (maybe put each hashtag in a separate file?)

    req_field = [
                    'attachments', 'author_id', 'conversation_id', 'created_at', 'entities', 'geo', 'id',
                    'in_reply_to_user_id', 'lang', 'public_metrics', 'possibly_sensitive',
                    'referenced_tweets', 'source', 'text'
                ]
    req_user_fields = ['id', 'username', 'withheld', 'location', 'verified', 'public_metrics']
    req_media_fields = ['media_key', 'type', 'url', 'public_metrics']

    req_params = create_params(query='#impfzwang',
                               fields=req_field,
                               user_fields=req_user_fields,
                               media_fields=req_media_fields,
                               start_time=a_while_ago,
                               max_results=50) #TODO remove max results!

    connect_to_mongo()
    # TODO For Now we empty the DB before
    Tweets.objects().delete()



    fetch_report = recent_search(req_params, tweet_fetch_limit=10) #TODO remove fetch limit!
    print(fetch_report)

    headers_exist = os.path.exists('data/fetch_log.csv')
    with open('data/fetch_log.csv', 'a', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        if not headers_exist:
            spamwriter.writerow(fetch_report.keys())
        spamwriter.writerow(fetch_report.values())
