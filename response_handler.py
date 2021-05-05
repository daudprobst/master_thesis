import pandas as pd
import csv
import os
from typing import Sequence, Dict, List, Union
import collections


class TwitterSearchResponse:

    def __init__(self, response_dict: Dict) -> None:
        self._data = response_dict

    def meta(self) -> Union[Dict, None]:
        try:
            return self._data['meta']
        except KeyError:
            return None

    def media(self) -> Union[List[Dict], None]:
        try:
            return self._data['includes']['media']
        except KeyError:
            return None

    def tweets(self) -> Union[List[Dict], None]:
        try:
            return self._data['data']
        except KeyError:
            return None

    def attach_media_to_tweets(self) -> Union[List[Dict], None]:
        if not self.media():
            return self.tweets()

        output_tweets = self.tweets()
        for media in self.media():
            print(f'Media with id {media["media_key"]}')
            for tweet in self.tweets():
                if 'attachments' in tweet and 'media_keys' in tweet['attachments']:
                    if media['media_key'] in tweet['attachments']['media_keys']:
                        print(f'Found match for media key {media["media_key"]} in tweet \n{tweet}')
                        tweet['media'] = media

        # TODO push this to the tweet in a  smart way and return all tweet objs where the tweets with matching media
        # contain the media obj
        return self.tweets()


def buffer_missing_fields(response_data: Sequence[Dict], fields: Sequence[str]) -> Sequence[Dict]:
    for response in response_data:
        for field in fields:
            if field not in response:
                response[field] = ''
    return response_data


def match_includes(response_data: Dict, media_fields: List[str], user_fields: List[str]) -> List[Dict]:
    """Takes a full response object and combines data fields with the matching
    includes for users and media. Returns a list of the combined data fields"""

    if 'includes' not in response_data:
        print("Warning: No includes where found. Returning data only.")
        return response_data['data']

    if 'media' in response_data:
        pass
        #probably loop through all media includes they are rare anyways

    #user includes should be solveable with a itertools zip as they always have the same length

    return response_data['data']

def write_to_csv(response_data: Sequence[Dict], filename: str) -> None:
    """
    Appends the specified response dictionary to a .csv specified
    :param response_data: List of *flat* dictionaries, where each dictionary represent one data point (i.e. one Tweet)
    :param filename: full filepath specified for the .csv

    """
    # ordered dict work in Python 3.7+
    response_data_sorted = [dict(sorted(row.items())) for row in response_data]
    if not os.path.exists(filename):
        print(f'File with filepath {filename} was not found. Reinitializing with empty file and headers.')
        with open(filename, 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile)
            spamwriter.writerow(response_data_sorted[0])

    with open(filename, 'a', newline='') as csvfile:
        for row in response_data_sorted:
            spamwriter = csv.writer(csvfile)
            spamwriter.writerow(row.values())


def flatten_response(response: Sequence[Dict]) -> Sequence[Dict]:
    """
    :param response: List of flattened responses
    :return: A flattened dict of the response that contains only the Tweets (meta information is cut)
    """
    output = [flatten(line) for line in response]
    return output


def flatten(dictionary: Dict, parent_key=False, separator: str='_') -> Dict:
    """
    Turn a nested dictionary into a flattened dictionary
    Originally authored by ScriptSmith
    (https://github.com/ScriptSmith/socialreaper/blob/master/socialreaper/tools.py#L8)
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    """

    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            items.extend(flatten(value, new_key, separator).items())
        elif isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten({str(k): v}, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)