"""Offline game evidence, clock export and bounded archive reads."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
import pytest

from chess_crawl.api import create_app
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers import registry
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.db import connection
from chess_crawl.storage.raw import store_raw_payload


def archived_game(database_url: str, pgn: str, *, variant: str = "standard", key: str = "evidence") -> tuple[int,int]:
    with connection(database_url,mode="rw") as conn:
        data = {"id":key,"variant":variant,"status":"mate","pgn":pgn,
                "clocks":[29998,0,None],"unfamiliar":{"kept":True},
                "players":{"white":{"user":{"id":"alice"}},"black":{"user":{"id":"bob"}}}}
        raw_id = store_raw_payload(conn,RawRecord(provider="lichess",endpoint_type="game",
            request_url=f"https://lichess.org/api/game/{key}",canonical_source_key=f"lichess/game/{key}",
            fetched_at=1,body=json.dumps(data).encode(),media_type="application/json"))
        return normalize_games_payload(conn,raw_id)[0],raw_id


def test_rich_game_reads_export_preserve_clock_and_source_evidence(
    database_url: str,fixtures_dir: Path,monkeypatch: pytest.MonkeyPatch,
) -> None:
    pgn = (fixtures_dir/"pgn/evidence.pgn").read_text()
    game_id,raw_id = archived_game(database_url,pgn)
    def forbidden(*args: Any,**kwargs: Any) -> Any:
        pytest.fail("Stored evidence reads must remain offline")
    monkeypatch.setattr(registry,"create_provider_client",forbidden)
    with TestClient(create_app(database_url,"local-token"),headers={"Authorization":"Bearer local-token"}) as api:
        evidence = api.get(f"/v1/games/{game_id}")
        assert evidence.status_code == 200,evidence.text
        stored = evidence.json()
        assert stored["parse_status"] == "complete"
        assert stored["headers"]["UnfamiliarTag"] == "preserved value"
        assert stored["source_metadata"]["unfamiliar"] == {"kept":True}
        assert stored["sources"][0]["raw_payload_id"] == raw_id
        assert stored["clocks"][0]["seconds"] == "299.990"
        page = api.get(f"/v1/games/{game_id}/moves",params={"limit":2,"mainline":"true"}).json()
        assert len(page["items"]) == 2 and page["next_cursor"] is not None
        assert page["items"][0]["clock_observations"][0]["seconds"] == "299.990"
        tail = api.get(f"/v1/games/{game_id}/moves",params={"limit":2,"mainline":"true","after":page["next_cursor"]}).json()
        assert tail["items"][0]["node_index"] > page["next_cursor"]
        exported = api.get(f"/v1/games/{game_id}/pgn")
        assert exported.status_code == 200
        assert "[%clk 0:04:59.990]" in exported.text
        assert "UnfamiliarTag" in exported.text and "[%custom unusual value]" in exported.text
        assert "attachment" in exported.headers["content-disposition"]
        assert api.get(f"/v1/games/{game_id}",params={"version_id":999999}).status_code == 404
        assert api.get(f"/v1/games/{game_id}/versions").json()["items"][0]["id"] == stored["id"]


def test_partial_variant_export_requires_explicit_opt_in(database_url: str) -> None:
    game_id,_ = archived_game(database_url,'[Variant "Chess960"]\n[Result "*"]\n\n1. e4 {[%clk 0:05:00]} *',variant="chess960")
    with TestClient(create_app(database_url,"local-token"),headers={"Authorization":"Bearer local-token"}) as api:
        assert api.get(f"/v1/games/{game_id}").json()["parse_status"] == "unsupported"
        assert api.get(f"/v1/games/{game_id}/pgn").status_code == 409
        allowed = api.get(f"/v1/games/{game_id}/pgn",params={"allow_partial":"true"})
        assert allowed.status_code == 200 and "[%clk 0:05:00]" in allowed.text


def test_profiles_history_ratings_and_scoped_resources_are_queryable_without_fetching(
    database_url:str,monkeypatch:pytest.MonkeyPatch,
) -> None:
    from test_player_resources import _profile,_resource
    with connection(database_url,mode="rw") as conn:
        _profile(conn,{"id":"alice","username":"Alice","title":"FM","profile":{"bio":"Chess960 enthusiast"}},at=100)
        _profile(conn,{"id":"alice","username":"Alice","title":"IM","profile":{"bio":"Chess960 enthusiast"}},at=200)
        _resource(conn,"lichess","rating-history",[{"name":"Blitz","points":[[2024,0,1,1500],[2024,0,2,1510]]}],at=200)
        _resource(conn,"lichess","teams",[{"id":"private-a","name":"Team Alpha"}],authenticated=True,owner_scope="alpha")
        _resource(conn,"lichess","teams",[{"id":"private-b","name":"Team Beta"}],authenticated=True,owner_scope="beta")
    def forbidden(*args:Any,**kwargs:Any) -> Any:
        pytest.fail("Profile reads must remain offline")
    monkeypatch.setattr(registry,"create_provider_client",forbidden)
    tokens={"alpha":"alpha-secret","beta":"beta-secret"}
    with TestClient(create_app(database_url,workspace_tokens=tokens),headers={"Authorization":"Bearer alpha-secret"}) as alpha:
        profile=alpha.get("/v1/users/lichess/alice")
        assert profile.status_code==200,profile.text
        assert profile.json()["profile"]["title"]=="IM"
        assert profile.json()["profile"]["native_data"]["profile"]["bio"]=="Chess960 enthusiast"
        resources=alpha.get("/v1/users/lichess/alice/resources")
        assert "Team Alpha" in resources.text and "Team Beta" not in resources.text
        history=alpha.get("/v1/users/lichess/alice/history",params={"limit":1}).json()
        assert history["items"][0]["title"]=="FM" and history["next_cursor"] is not None
        tail=alpha.get("/v1/users/lichess/alice/history",params={"after":history["next_cursor"]}).json()
        assert tail["items"][0]["title"]=="IM"
        ratings=alpha.get("/v1/users/lichess/alice/rating-history",params={"limit":1}).json()
        assert ratings["items"][0]["rating"]==1500 and ratings["next_cursor"] is not None
        following=alpha.get("/v1/users/lichess/alice/rating-history",params={"snapshot_id":ratings["snapshot_id"],"after":ratings["next_cursor"]}).json()
        assert following["items"][0]["rating"]==1510
        assert alpha.get("/v1/users/lichess/alice/coverage").json()["complete_history"] is None
        assert alpha.get("/v1/users/lichess/alice/games").json()["items"]==[]
        assert "Team Beta" not in alpha.get("/v1/users/lichess/alice/resources/history").text
        assert alpha.get("/v1/users/lichess/missing").status_code==404
