# flake8: noqa: E501

import requests
import json
from requests.adapters import HTTPAdapter, Retry
from flask import jsonify  # type: ignore
from time import sleep
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
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
    "MateCheckCapture",
    # "HalfStockfishBot",
]
LIMIT = 3000

def add_pacific_date(game):
    created_at_ms = game.get('created_at')
    if created_at_ms:
        utc_dt = datetime.fromtimestamp(created_at_ms / 1000, tz=timezone.utc)
        pacific_dt = utc_dt.astimezone(ZoneInfo('America/Los_Angeles'))
        game['pacific_date'] = pacific_dt.date().isoformat()
    else:
        game['pacific_date'] = None
    return game


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
            "tags": "true",
            "moves": "true",
        },
    )
    if response.status_code == 429:
        if is_retry:
            response.raise_for_status()
        sleep(60)
        return get_user_games(user_name, since, is_retry=True)
    response.raise_for_status()
    return [add_pacific_date(json.loads(game)) for game in response.text.strip().split("\n") if game]


def to_fivetran_format(games, has_more, state):
    return {
        "hasMore": has_more,
        "insert": {"games": games},
        "state": state,
        "schema": {
            "games": {
                "columns": [
                    {"name": "id", "type": "string"},
                    {"name": "last_move_at", "type": "integer"},
                    {"name": "players", "type": "string"},
                    {"name": "last_move", "type": "string"},
                    {"name": "last_fen", "type": "string"},
                    {"name": "clock", "type": "string"},
                    {"name": "source", "type": "string"},
                    {"name": "speed", "type": "string"},
                    {"name": "rated", "type": "boolean"},
                    {"name": "created_at", "type": "integer"},
                    {"name": "winner", "type": "string"},
                    {"name": "pgn", "type": "string"},
                    {"name": "variant", "type": "string"},
                    {"name": "full_id", "type": "string"},
                    {"name": "perf", "type": "string"},
                    {"name": "status", "type": "string"},
                    {"name": "initial_fen", "type": "string"},
                    {"name": "days_per_turn", "type": "integer"},
                    {"name": "tournament", "type": "string"},
                    {"name": "swiss", "type": "string"},
                    {"name": "moves", "type": "string"},
                    {"name": "pacific_date", "type": "date"}
                ],
                "primary_key": ["id", "pacific_date"]
            }
        },
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
        if not i == 0:
            sleep(2)
        user_games = get_user_games(user, previous_state.get(user, EARLIEST_LICHESS_TIME))
        print(f"Found {len(user_games)} recent games for {user}")
        games.extend(user_games)
        state[user] = user_games[-1]["createdAt"] - SIX_HOUR_BUFFER if user_games else previous_state.get(user, EARLIEST_LICHESS_TIME)
        has_more = has_more or len(user_games) == LIMIT
    print(f"State: {state}")
    fivetran_format = to_fivetran_format(games, has_more=has_more, state=state)
    return jsonify(fivetran_format)
