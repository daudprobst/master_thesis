from requests_base import make_request, create_params

import json

if __name__ == "__main__":
    params = create_params(query='#pinkygloves')
    json_response = make_request('https://api.twitter.com/2/tweets/search/recent', params)
    print(json.dumps(json_response, indent=4, sort_keys=True))