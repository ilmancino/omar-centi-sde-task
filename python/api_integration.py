from logging import getLogger
from my_utils import get_my_logger
import os, requests

# TODO add documentation for the methods here

_logger = get_my_logger(__name__)

BASE_URL = "https://api.nytimes.com/svc/books/v3/lists/{}.json"
AUTH_TOKEN = os.environ["NYT_API_KEY"]


def fetch_data(endpoint: str, params: dict = None):
    """
    Fetches data from an endpoint of the NYT API Books
    and returns a dict with the results.

    :param endpoint: Name of the endpoint to call
    :type endpoint: str
    :param params: parameters to be passed in the HTTP request
    :type params: dict

    :return: a dictionary containing the results of the call
    """

    headers = {
        "Accept": "application/json",
        }

    url = BASE_URL.format(endpoint)

    request_params = { 'api-key': AUTH_TOKEN }
    if params:
        request_params.update(params)

    # argument "params" DO NOT include the api-key ;)
    _logger.info(f"Fetching from endpoint: {endpoint} with params: {params}")
    raw_response = requests.get(
        url=url,
        headers=headers,
        params=request_params,
        )


    if raw_response.status_code == 200:
        response = raw_response.json()
    else:
        _logger.error("There were problems fetching from the API")
        raw_response.raise_for_status()


    return response['results']
