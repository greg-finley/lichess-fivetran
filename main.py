import requests
import json
from requests.adapters import HTTPAdapter, Retry
from time import sleep, time


from flask import Response  # type: ignore
import os

TWO_DAYS_AGO = round(time() * 1000) - 172800000
LICHESS_TOKEN = os.environ["LICHESS_TOKEN"]
USERS = [
    "AlphaBotical",
    # "tmftmftmf",
    # "gbfgbfgbf",
    "MinOpponentMoves",
    # "HalfStockfishBot",
]


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
            # "pgnInJson": "true",
            "sort": "dateAsc",
            # "lastFen": "true",
            "max": 1,
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


def main(request):
    for i, user in enumerate(USERS):
        # if not i == 0:
        #     sleep(2)
        user_games = get_user_games(user, TWO_DAYS_AGO)
        if not user_games:
            raise Exception(f"Failed to get games for {user}")
        print(f"Found recent games for {user}")

    return Response(json.dumps({"ok": True}), mimetype="application/json")
