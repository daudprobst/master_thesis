import pandas as pd
import csv
import os
from typing import Sequence, Dict
import collections

def buffer_missing_fields(response_data: Sequence[Dict], fields: Sequence[str]) -> Sequence[Dict]:
    for response in response_data:
        for field in fields:
            if field not in response:
                response[field] = ''
    return response_data

def match_includes(response_data: Dict) -> Sequence[Dict]:
    pass


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



def flatten(dictionary, parent_key=False, separator='_'):
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