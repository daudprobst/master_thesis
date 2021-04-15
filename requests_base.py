import requests
import os
from datetime import datetime
import json


def auth(bearer_token: str = 'TWT_BEARER_TOKEN') -> str:
    """Returns the bearer token specified in OS: To set your environment variables in your
    terminal run the following line: export 'TWT_BEARER_TOKEN'='<your_bearer_token>'

    :param: optional custom name for environment variable that contains the bearer token
    :return: bearer token as string
    """

    try:
        return os.environ.get(bearer_token)
    except Exception as e:
        raise Exception(f'Failed to load authentification {bearer_token} from environment variables: {e}')


def create_headers(bearer_token: str = None) -> dict:
    """Returns the headers needed for making a Twitter API Request (incl. authentification)

    :param bearer_token: bearer token used for authentification in Twitter API
    :return: Headers needed for making a Twitter API request

    """
    if not bearer_token:
        bearer_token = auth()
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_params(query: str, max_results: int = None, fields: list = None,
                  start_time: datetime = None, end_time: datetime = None, next_token: str = None):
    """Create a params dictionary that can be passed to make_request. If none is specified for any
    of the arguments, the Twitter API default will be used.
    :param query: query to be searched
    :param max_results: max_amount of tweets to be returned; Twitter API allows values between 1 and 100.
    :param fields: all fields to be returned; default Twitter API returns 'text' and 'id'
    :param start_time: time from which tweets are fetched; default Twitter API start_time is 7 days ago; this can't be
    longer than 7 days ago for the standard access since the standard access only allows fetching the past seven days.
    :param end_time: time until which tweets are fetched; 7 days limit from start_time applies as well.
    :param next_token: next_token is only used for pagination, i.e. for retrieving results when there are more then
    100 results; a next_token will be included in the response which can be used for the next request in that case
    :returns: parameters for making the API request as a dictionary
    """

    params = {'query': query}

    if max_results:
        params['max_results'] = max_results
    if fields:
        # TODO some parsing needs to happen here!
        params['fields'] = fields
    if start_time:
        params['start_time'] = start_time
    if end_time:
        params['end_time'] = end_time
    if next_token:
        params['next_token'] = next_token

    return params


def make_request(base_url: str, params: dict, headers: dict = None, request_type: str = 'GET') -> json:
    """Executes the request specified through the arguments and returns the response as JSON

    :param base_url: base_url of endpoint, e.g. https://api.twitter.com/2/tweets/search/recent
    :param params: parameter for request, can be created with create_params
    :param headers: headers of request, can be created with create_headers
    :param request_type: 'GET' or 'POST'
    :raises Exception if a status code other than 200 is returned
    :return: Result of request as returned by the API

    """
    if not headers:
        headers = create_headers()
    response = requests.request(request_type, base_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()
