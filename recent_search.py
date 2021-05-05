from requests_base import make_request, create_params
from response_handler import buffer_missing_fields, write_to_csv, flatten, TwitterSearchResponse
from time import sleep
from typing import Tuple, Optional, List, Dict
from datetime import datetime, timedelta, timezone
from db.schemes import *
from db.connection import connect_to_mongo
from json import dumps

from pprint import pprint

RECENT_SEARCH_URL = 'https://api.twitter.com/2/tweets/search/recent'



def recent_search(params: dict, headers: dict = None,
                  tweet_fetch_limit: int = 1000) -> Tuple[List[Dict], Optional[str]]:
    """
    :param params:
    :param headers:
    :param tweet_fetch_limit:
    :return:
    """

    total_results = []
    next_token = None

    # Paginating through the result if there is more than the 100 limit can provide
    while next_token or not total_results:
        if next_token:  # always the case except for the first request
            params['next_token'] = next_token

        response = make_request(RECENT_SEARCH_URL, params, headers)

        respObj = TwitterSearchResponse(response)
        print(respObj.attach_media_to_tweets())
        if response['meta']['result_count'] == 0:
            print("Empty response")
            return total_results, None

        connect_to_mongo()
        # TODO TMP EMPTY DB
        #Tweets.objects().delete()
        #for entry in response['data']:
        #    entry['test_id'] = entry['id']
        #    entry.pop('id')
        #
        #    print(dumps(entry))
        #    Tweets.from_json(dumps(entry)).save(force_insert=True)

        # matched_response_data = matched_response_data(response, )
        response['data'] = buffer_missing_fields(response['data'], params['tweet.fields'].split(','))

        total_results.extend(response['data'])

        if len(total_results) >= tweet_fetch_limit:
            print(f'Fetched at least as much tweets as the fetch limit.'
                  f'Aborting process here but returning last next_token')
            return total_results, response['meta']

        if 'next_token' in response['meta']:
            next_token = response['meta']['next_token']
            print(f'Fetched {len(total_results)}. More to Come!')
            # to respect the 180 tweets per 15min request cap, we aim for one request every 6s
            sleep(6)
        else:
            next_token = None

    # ALl tweets for search query are fetched
    return total_results, None


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
    req_media_fields = ['type', 'url', 'public_metrics'] #'media_key',

    req_params = create_params(query='#impfzwang',
                               fields=req_field,
                               user_fields=req_user_fields,
                               media_fields=req_media_fields,
                               start_time=a_while_ago,
                               max_results=50) #TODO remove max results!

    results, res_next_token = recent_search(req_params, tweet_fetch_limit=10) #TODO remove fetch limit!


    if res_next_token:
        # TODO log meta information for responses (when conducted for which params, next_token passed?)
        print(f'Next token for response was {res_next_token}. Should include a meta logging here for the responses!')

    #pprint(results)
    write_to_csv(results, 'data/egge.csv')
