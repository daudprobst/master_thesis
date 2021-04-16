from requests_base import make_request, create_params
from response_handler import flatten_response, write_to_csv

import json

if __name__ == "__main__":
    params = create_params(query='#pinkygloves', fields=['id', 'text', 'created_at', 'public_metrics'])
    json_response = make_request('https://api.twitter.com/2/tweets/search/recent', params)
    print(json_response)
    # print(json.dumps(json_response, indent=4, sort_keys=True))
    # print(json.dumps(flatten_response(json_response['data']), indent=4))
    write_to_csv(flatten_response(json_response['data']), 'data/eggs.csv')