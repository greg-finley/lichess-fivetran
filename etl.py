# flake8: noqa: E501

from typing import Literal
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
TOURNEY_TYPE = Literal["swiss", "arena"]
TOURNEYS_TO_FETCH: dict[str, dict[TOURNEY_TYPE, list[str]]] = {
    "darkonclassical": {
        "swiss": ["DarkOnClassical"],
        "arena": []
    },
    "darkonrapid": {
        "swiss": ["DarkOnRapid"],
        "arena": []
    },
    "darkonteams": {
        "swiss": ["Hourly Rapid", "Hourly Blitz", "Blitz Shield"],
        "arena": ["Hourly Ultrabullet"]
    }
}
LIMIT = 3000

def add_metadata(item, team_name: str | None = None, tourney_name: str | None = None, tourney_type: TOURNEY_TYPE | None = None):
    created_at_ms = item.get('createdAt', item.get('startsAt'))
    if created_at_ms:
        utc_dt = datetime.fromtimestamp(created_at_ms / 1000, tz=timezone.utc)
        pacific_dt = utc_dt.astimezone(ZoneInfo('America/Los_Angeles'))
        item['pacificDate'] = pacific_dt.date().isoformat()
    else:
        item['pacificDate'] = None
    if team_name:
        item['teamName'] = team_name
    if tourney_name:
        item['tourneyName'] = tourney_name
    if tourney_type:
        item['tourneyType'] = tourney_type
    return item


class HttpClient:
    def __init__(self):
        self.s = requests.Session()
        retries = Retry(total=3, backoff_factor=60)
        self.s.mount("https://", HTTPAdapter(max_retries=retries))
        self.s.mount("http://", HTTPAdapter(max_retries=retries))

    def get(self, url: str, *, params=None, headers=None):
        return self.s.get(url, params=params, headers=headers, stream=True)


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
    for game in response.iter_lines():
        if game:
            yield add_metadata(json.loads(game))

def get_tournaments(tourney_name: str, team_name: str, tourney_type: TOURNEY_TYPE, since: int, is_retry: bool = False):
    url = f"https://lichess.org/api/team/{team_name}/{tourney_type}"
    response = http_client.get(
        url,
        headers={
            "Accept": "application/x-ndjson",
            "Authorization": f"Bearer {LICHESS_TOKEN}",
        },
        params={
            "max": 10000,
            "status": "finished",
            "name": tourney_name.replace(' ', '%20'),
        })
    if response.status_code == 429:
        if is_retry:
            response.raise_for_status()
        sleep(60)
        return get_tournaments(tourney_name, team_name, tourney_type, since, is_retry=True)
    response.raise_for_status()
    for tournament in response.iter_lines():
        if tournament:
            yield add_metadata(json.loads(tournament), team_name=team_name, tourney_name=tourney_name, tourney_type=tourney_type)

def to_fivetran_format(games, swiss_tournaments, arena_tournaments, has_more, state):
    return {
        "hasMore": has_more,
        "insert": {"games": games, "swissTournaments": swiss_tournaments, "arenaTournaments": arena_tournaments},
        "state": state,
        "schema": {
            "games": {
                "columns": [
                    {"name": "id", "type": "string"},
                    {"name": "lastMoveAt", "type": "integer"},
                    {"name": "players", "type": "string"},
                    {"name": "lastMove", "type": "string"},
                    {"name": "lastFen", "type": "string"},
                    {"name": "clock", "type": "string"},
                    {"name": "source", "type": "string"},
                    {"name": "speed", "type": "string"},
                    {"name": "rated", "type": "boolean"},
                    {"name": "createdAt", "type": "integer"},
                    {"name": "winner", "type": "string"},
                    {"name": "pgn", "type": "string"},
                    {"name": "variant", "type": "string"},
                    {"name": "fullId", "type": "string"},
                    {"name": "perf", "type": "string"},
                    {"name": "status", "type": "string"},
                    {"name": "initialFen", "type": "string"},
                    {"name": "daysPerTurn", "type": "integer"},
                    {"name": "tournament", "type": "string"},
                    {"name": "swiss", "type": "string"},
                    {"name": "moves", "type": "string"},
                    {"name": "pacificDate", "type": "date"}
                ],
                "primary_key": ["id", "pacificDate"]
            },
            "swissTournaments": {
                "columns": [
                    {"name": "id", "type": "string"},
                    {"name": "lastMoveAt", "type": "integer"},
                    {"name": "players", "type": "string"},
                    {"name": "lastMove", "type": "string"},
                    {"name": "lastFen", "type": "string"},
                    {"name": "clock", "type": "string"},
                    {"name": "source", "type": "string"},
                    {"name": "speed", "type": "string"},
                    {"name": "rated", "type": "boolean"},
                    {"name": "createdAt", "type": "integer"},
                    {"name": "winner", "type": "string"},
                    {"name": "pgn", "type": "string"},
                    {"name": "variant", "type": "string"},
                    {"name": "fullId", "type": "string"},
                    {"name": "perf", "type": "string"},
                    {"name": "status", "type": "string"},
                    {"name": "initialFen", "type": "string"},
                    {"name": "daysPerTurn", "type": "integer"},
                    {"name": "tournament", "type": "string"},
                    {"name": "swiss", "type": "string"},
                    {"name": "moves", "type": "string"},
                    {"name": "pacificDate", "type": "date"}
                ],
                "primary_key": ["id", "pacificDate"]
            }
        },
    }


@functions_framework.http
def main(request):
    request_json = request.get_json(silent=True) or {}
    previous_state = request_json.get('state', {})
    print(f"Previous state: {previous_state}")

    games = []
    swiss_tournaments = []
    arena_tournaments = []
    state = {}
    # Just assume we can do tourneys in one shot
    has_more = False

    for team_name in TOURNEYS_TO_FETCH:
        for tourney_type in TOURNEYS_TO_FETCH[team_name]:
            for tourney_name in TOURNEYS_TO_FETCH[team_name][tourney_type]:
                this_tournaments = get_tournaments(tourney_name, team_name, tourney_type, previous_state.get(tourney_name, EARLIEST_LICHESS_TIME))
                print(f"Found {len(this_tournaments)} recent tournaments for {tourney_name}")
                if tourney_type == "swiss":
                    swiss_tournaments.extend(this_tournaments)
                else:
                    arena_tournaments.extend(this_tournaments)
                # state[tourney_name] = this_tournaments[-1]["createdAt"] - SIX_HOUR_BUFFER if this_tournaments else previous_state.get(tourney_name, EARLIEST_LICHESS_TIME)
                # TODO: Eventually get tourney results and games

    for user in enumerate(USERS):
        sleep(2)
        user_games = get_user_games(user, previous_state.get(user, EARLIEST_LICHESS_TIME))
        print(f"Found {len(user_games)} recent games for {user}")
        games.extend(user_games)
        state[user] = user_games[-1]["createdAt"] - SIX_HOUR_BUFFER if user_games else previous_state.get(user, EARLIEST_LICHESS_TIME)
        has_more = has_more or len(user_games) == LIMIT
    print(f"State: {state}")
    fivetran_format = to_fivetran_format(games, swiss_tournaments, arena_tournaments, has_more=has_more, state=state)
    return jsonify(fivetran_format)
