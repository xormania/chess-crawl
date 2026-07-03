from __future__ import annotations

import pytest

from chess_crawl import __main__, cli


def test_top_level_help_includes_phase3_commands(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.run(["--help"])

    out = capsys.readouterr()
    assert exc.value.code == 0
    assert "crawl" in out.out
    assert "jobs" in out.out
    assert "report" in out.out
    assert "export" in out.out


def test_package_main_delegates_to_cli() -> None:
    assert __main__.main is cli.main
