import pandas as pd
import csv
import os
from typing import Sequence, Dict

def write_to_csv(response_data: Sequence[Dict], filename: str) -> None:
    """
    Appends the specified response dictionary to a .csv specified
    :param response_data: List of *flat* dictionaries, where each dictionary represent one data point (i.e. one Tweet)
    :param filename: full filepath specified for the .csv

    """
    if not os.path.exists(filename):
        print(f'File with filepath {filename} was not found. Reinitializing with empty file and headers.')
        with open(filename, 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile)
            spamwriter.writerow(response_data[0])

    with open(filename, 'a', newline='') as csvfile:
        for row in response_data:
            spamwriter = csv.writer(csvfile)
            spamwriter.writerow(row.values())

    # TODO this should probably at least throw a warning if the file does not exist

def flatten_response(response: Sequence[Dict]) -> Sequence[Dict]:
    """
    :param response: List of flattened responses
    :return: A flattened dict of the response that contains only the Tweets (meta information is cut)
    """
    output = [pd.json_normalize(line, sep='_').to_dict(orient='records')[0] for line in response]
    # TODO might want to update the flattening so we do no longer require this messy Pandas stuff
    return output
