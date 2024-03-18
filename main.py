import requests
import json
from pprint import pprint


def get_user_games(user_name: str):
    url = f"https://lichess.org/api/games/user/{user_name}"
    response = requests.get(
        url, headers={"Accept": "application/x-ndjson"}, params={"max": 5}
    )
    response.raise_for_status()
    games = [json.loads(game) for game in response.text.strip().split("\n") if game]
    pprint(games)


get_user_games("gbfgbfgbf")
