import copy
import requests
import json

from flask import Response  # type: ignore
import os

OLDEST_EPOCH = 1356998400070
LICHESS_TOKEN = os.environ["LICHESS_TOKEN"]
LIMIT_PER_USER = 200


def get_user_games(user_name: str, since: int):
    url = f"https://lichess.org/api/games/user/{user_name}"
    response = requests.get(
        url,
        headers={
            "Accept": "application/x-ndjson",
            "Authorization": f"Bearer {LICHESS_TOKEN}",
        },
        params={
            "since": str(since),
            "pgnInJson": "true",
            "max": str(LIMIT_PER_USER),
            "sort": "dateDesc",
            "lastFen": "true",
        },
    )
    response.raise_for_status()
    return [json.loads(game) for game in response.text.strip().split("\n") if game]


def to_fivetran_format(games, has_more, state):
    return {
        "hasMore": has_more,
        "insert": {"games": games},
        "state": state,
        "schema": {"games": {"primary_key": ["id"]}},
    }


def main(request):
    request_json = request.get_json(silent=True)
    old_state = (
        request_json.get("state", {})
        if request_json and "state" in request_json
        else {}
    )
    new_state = copy.deepcopy(old_state)
    print(f"Old state: {old_state}")
    games = []
    has_more = False
    for user in ["AlphaBotical"]:
        last_epoch = old_state.get(user, OLDEST_EPOCH)
        user_games = get_user_games(user, last_epoch)
        if user_games:
            newest_game = user_games[0]
            new_state[user] = newest_game["createdAt"]
            if len(user_games) == LIMIT_PER_USER:
                has_more = True
            games.extend(get_user_games(user, last_epoch))

    fivetran_format = to_fivetran_format(games, has_more, new_state)

    return Response(json.dumps(fivetran_format), mimetype="application/json")
