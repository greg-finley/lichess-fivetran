# flake8: noqa: E501

import requests
import json
from requests.adapters import HTTPAdapter, Retry
from flask import jsonify  # type: ignore
from time import sleep


import functions_framework  # type: ignore
import os

EARLIEST_LICHESS_TIME = 1356998400070
SIX_HOUR_BUFFER = 21600000
LICHESS_TOKEN = os.environ["LICHESS_TOKEN"]
USERS = [
    "AlphaBotical",
    "tmftmftmf",
    "gbfgbfgbf",
    "MinOpponentMoves",
    # "HalfStockfishBot",
]
LIMIT = 3000


class HttpClient:
    def __init__(self):
        self.s = requests.Session()
        retries = Retry(total=3, backoff_factor=60)
        self.s.mount("https://", HTTPAdapter(max_retries=retries))
        self.s.mount("http://", HTTPAdapter(max_retries=retries))

    def get(self, url: str, *, params=None, headers=None):
        return self.s.get(url, params=params, headers=headers)


http_client = HttpClient()


def get_user_games(user_name: str, since: int, is_retry: bool = False):
    url = f"https://lichess.org/api/games/user/{user_name}"
    response = http_client.get(
        url,
        headers={
            "Accept": "application/x-ndjson",
            "Authorization": f"Bearer {LICHESS_TOKEN}",
        },
        params={
            "since": str(since),
            "pgnInJson": "true",
            "sort": "dateAsc",
            "lastFen": "true",
            "max": LIMIT,
            "tags": "false",
            "moves": "false",
        },
    )
    if response.status_code == 429:
        if is_retry:
            response.raise_for_status()
        sleep(60)
        return get_user_games(user_name, since, is_retry=True)
    response.raise_for_status()
    return [json.loads(game) for game in response.text.strip().split("\n") if game]


def to_fivetran_format(games, has_more, state):
    return {
        "hasMore": has_more,
        "insert": {"games": games},
        "state": state,
        "schema": {"games": {"primary_key": ["id"]}},
    }


@functions_framework.http
def main(request):
    request_json = request.get_json(silent=True) or {}
    previous_state = request_json.get('state', {})
    print(f"Previous state: {previous_state}")

    games = []
    state = {}
    has_more = False
    for i, user in enumerate(USERS):
        if (not i == 0) and len(games) > 300:
            sleep(2)
        user_games = get_user_games(user, previous_state.get(user, EARLIEST_LICHESS_TIME))
        print(f"Found {len(user_games)} recent games for {user}")
        games.extend(user_games)
        state[user] = user_games[-1]["createdAt"] - SIX_HOUR_BUFFER if user_games else previous_state.get(user, EARLIEST_LICHESS_TIME)
        has_more = has_more or len(user_games) == LIMIT
    print(f"State: {state}")
    fivetran_format = to_fivetran_format(games, has_more=has_more, state=state)
    return jsonify(fivetran_format)
