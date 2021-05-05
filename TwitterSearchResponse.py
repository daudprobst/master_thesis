import csv
import os
from typing import Sequence, Dict, List, Union
import collections
import db.schemes as mongo_db
from json import dumps


class TwitterSearchResponse:

    def __init__(self, response_dict: Dict, search_params: Dict) -> None:
        self._data = response_dict
        self._search_params = search_params

    def meta(self) -> Union[Dict, None]:
        try:
            return self._data['meta']
        except KeyError:
            return None

    def next_token(self) -> Union[str, None]:
        try:
            return self.meta()['next_token']
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

    def write_to_db(self) -> None:

        for entry in self.attach_media_to_tweets():
            entry['tweet_id'] = entry['id']
            entry.pop('id')
            mongo_db.Tweets.from_json(dumps(entry)).save() # TODO force_insert=True

        #TODO writer users to DB!!


    def write_to_csv(self, filename: str) -> None:
        """
        Writes Tweet data (without user includes) to a specified csv file
        :param filename: full filepath specified for the .csv
        """
        # ordered dict work in Python 3.7+
        response_data_sorted = [dict(sorted(row.items())) for row in self.tweets()]
        if not os.path.exists(filename):
            print(f'File with filepath {filename} was not found. Reinitializing with empty file and headers.')
            with open(filename, 'w', newline='') as csvfile:
                spamwriter = csv.writer(csvfile)
                spamwriter.writerow(response_data_sorted[0])

        with open(filename, 'a', newline='') as csvfile:
            for row in response_data_sorted:
                spamwriter = csv.writer(csvfile)
                spamwriter.writerow(row.values())


    def buffer_missing_fields(self) -> Sequence[Dict]:
        """Buffers requested fields for which TwitterAPI returned no answer (e.g. often the case for 'withheld'
          for with an empty string. This happens in place for the self.tweets() of the instance"""
        for tweet in self.tweets():
            for field in self._search_params['tweet.fields'].split(','):
                if field not in tweet:
                    tweet[field] = ''
        return self.tweets()


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