import json
import os

import requests


def PingAPI(url):
    """
    Pings the given API endpoint to check for health and returns response

    Args:
        url (string)         - API endpoint URL

    Returns:
        health_flag (str)    - Contains the info about the Response status
        request (res)        - API response
        error_msg (str)      - Contains information if API doesnt responsd
    """
    print(f"Pinging the API {url}")
    request = requests.get(url, verify=False)
    if request.status_code == 200:
        print(f"API responded with status code {request.status_code}")
        health_flag = "Healthy"
        error_msg = ""
    else:
        health_flag = "Unhealthy"
        error_msg = "The status code of the API is " + str(request.status_code)
        print("unhealthy json response from API")
        print(f"API responded with status code {request.status_code}")
    return health_flag, request, error_msg


def GenerateJSON(file, req):
    """
    Generates a JSON file from API response

    Args:
        file (str)           - a location on UNIX server to store the JSON file
        req (res)            - API response object
    Returns:
    """
    print(f"Generating JSON at {file}")
    entity = req.json()
    if os.path.exists(file):
        os.remove(file)
        with open(file, "w") as outfile:
            json.dump(entity, outfile)
    else:
        with open(file, "w") as outfile:
            json.dump(entity, outfile)
    return None
