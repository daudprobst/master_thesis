from lib.twt_api.requests_base import make_request
from lib.db.schemes import *

from time import sleep
import os
import csv
from typing import Dict

RECENT_SEARCH_URL = 'https://api.twitter.com/2/tweets/search/recent'


def recent_search(params: dict, headers: dict = None,
                  tweet_fetch_limit: int = 1000) -> Dict:
    """Executes the recent_search Twitter request and writes the results to the mongo_db (must be connected!) and a .csv

    :param params: the parameters for the Twitter search (use create_params() to build this)
    :param headers: Headers for connecting to Twitter API; default values are set in requests_base
    :param tweet_fetch_limit: if more than this amount of tweets has been fetched, the search is stopped
    :return: returns a fetch report that describes the search
    """

    fetch_report = {
        "tweet_fetch_limit": tweet_fetch_limit,
        "started_fetching": datetime.now().isoformat(),
        "finished_fetching": '',
        "fetched_total": 0,
        "next_token": '',
        "params": params,
        "interrupts": ''
    }
    fetched_total = 0
    next_token = None

    # Paginating through the result if there is more than the 100 limit can provide
    while next_token or fetched_total <= 0:
        if next_token:  # always the case except for the first request
            params['next_token'] = next_token

        try:
            response = make_request(RECENT_SEARCH_URL, params, headers)
        except Exception as e:
            print(f"TwitterAPI Access failed: {e}")
            fetch_report['finished_fetching'] = datetime.now().isoformat()
            fetch_report['fetched_total'] = fetched_total
            fetch_report['next_token'] = next_token
            fetch_report['interrupts'] = e
            return fetch_report

        print(f'Fetched new batch of tweets! {response.meta}')
        if response.meta['result_count'] == 0 and fetched_total == 0:
            print("No results were found for this query!")
            break

        fetched_total += response.meta['result_count']

        response.write_to_db()
        response.write_to_csv()

        next_token = response.next_token

        if fetched_total >= tweet_fetch_limit:
            print(f'Fetched at least as much tweets as the fetch limit.'
                  f'Aborting process here but returning last next_token')
            break

        if next_token:
            print(f"Fetched {fetched_total} tweets so. Continuing fetching after delay...")
            # Wait for 6s so there are not too many requests in a short time (API limit from twitter)
            sleep(6)

    print("Finished fetching")
    fetch_report['finished_fetching'] = datetime.now().isoformat()
    fetch_report['fetched_total'] = fetched_total
    fetch_report['next_token'] = next_token
    return fetch_report


def fetch_report_to_csv(fetch_report: dict, filename: str) -> None:
    """Appends the fetch report as row to the csv specified with filename
    :param fetch_report: fetch_report to append to file
    :param filename: file to append fetch_report to
    """

    headers_exist = os.path.exists('data/fetch_log.csv')
    with open(filename, 'a', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        if not headers_exist:
            spamwriter.writerow(fetch_report.keys())
        spamwriter.writerow(fetch_report.values())