import requests
import os
from datetime import datetime, timedelta, time
from typing import Sequence, Tuple
from TwitterSearchResponse import TwitterSearchResponse


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


def create_params(query: str, max_results: int = 100, fields: Sequence[str] = None,
                  media_fields: Sequence[str] = None, user_fields: Sequence[str] = None,
                  start_time: datetime = None, end_time: datetime = None,
                  next_token: str = None) -> dict:
    """Create a params dictionary that can be passed to make_request. If none is specified for any
    of the arguments, the Twitter API default will be used.
    :param query: query to be searched
    :param max_results: max_amount of tweets to be returned; Twitter API allows values between 1 and 100.
    :param fields: all fields to be returned; defaults to ['id', 'text']
    :param media_fields: all media_fields to be returned
    :param user_fields: all user_fields to be returned
    :param start_time: time from which tweets are fetched (inclusive); default Twitter API start_time is 7 days ago;
    standard access only allows fetching back max. 7 days
    :param end_time: time until which tweets are fetched (exclusive)
    :param next_token: next_token is only used for pagination, i.e. for retrieving results when there are more then
    100 results; a next_token will be included in the response which can be used for the next request in that case
    :returns: parameters for making the API request as a dictionary
    """

    params = {'query': query}

    if not fields:
        fields = ['id, text']

    expansions = []
    if media_fields:
        if 'attachments' not in fields:
            fields.append('attachments')  # we need the attachments.media id for matching
        expansions.append('attachments.media_keys')
        params['media.fields'] = ','.join(media_fields)

    if user_fields:
        if 'author_id' not in fields:
            fields.append('author_id')  # we need author id for matching
        expansions.append('author_id')
        params['user.fields'] = ','.join(user_fields)

    if expansions:
        params['expansions'] = ','.join(expansions)

    params['tweet.fields'] = ','.join(fields)  # TWT API expects comma separated list
    params['max_results'] = max_results

    if start_time:
        params['start_time'] = start_time.isoformat()
    if end_time:
        params['end_time'] = end_time.isoformat()
    if next_token:
        params['next_token'] = next_token

    return params


def make_request(base_url: str, params: dict, headers: dict = None, request_type: str = 'GET') -> TwitterSearchResponse:
    """Executes the request specified through the arguments and returns the response as JSON

    :param base_url: base_url of endpoint, e.g. https://api.twitter.com/2/tweets/search/recent
    :param params: parameter for request, can be created with create_params
    :param headers: headers of request, can be created with create_headers
    :param request_type: 'GET' or 'POST'
    :raise Exception: if a status code other than 200 is returned
    :return: Result of request as returned by the API

    """
    if not headers:
        headers = create_headers()
    response = requests.request(request_type, base_url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    return TwitterSearchResponse(response.json(), params)


def day_wrapping_datetimes(day: datetime) -> Tuple[datetime, datetime]:
    """
    Returns the datetimes for the first second of the day and the first second of the next day
    (we need first second of next day not last second of this day because enddate is exclusive for twitter api)
    :param day: day for which we want the first and last second
    :return: Tuple containing datetimes for first and last second of day
    """
    return (
        datetime.combine(day.date(), time.min),
        datetime.combine((day + timedelta(days=1)).date(), time.min)
    )
