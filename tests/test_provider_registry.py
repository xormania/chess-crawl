from chess_crawl.providers.registry import get_provider_info, known_keys, list_provider_infos


def test_provider_registry_lists_chesscom_and_lichess() -> None:
    assert known_keys() == ["chess.com", "lichess"]

    infos = {info.key: info for info in list_provider_infos()}
    assert infos["chess.com"].single_game_by_id is False
    assert infos["lichess"].single_game_by_id is True
    assert get_provider_info("lichess").policy.fixed_429_backoff_s == 60.0
