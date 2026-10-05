"""A broken startup handshake must fail rather than hang the test process."""
import sys

import pytest

from helpers.processes import child_process, expect_output


@pytest.mark.parametrize("output", ["", "partial"])
def test_startup_timeout_kills_child_even_with_unterminated_output(output: str) -> None:
    with pytest.raises(AssertionError, match="startup timeout"):
        with child_process([
            sys.executable, "-c",
            f"import sys; sys.stdout.write({output!r}); sys.stdout.flush(); sys.stdin.read()",
        ]) as child:
            expect_output(child, "ready", timeout=0.2)
    assert child.poll() is not None


def test_startup_eof_reports_failure_and_reaps_child() -> None:
    with pytest.raises(AssertionError, match="exited before"):
        with child_process([sys.executable, "-c", "pass"]) as child:
            expect_output(child, "ready")
    assert child.returncode == 0


def test_successful_handshake_does_not_consume_subsequent_output() -> None:
    with child_process([sys.executable, "-c", "print('ready'); print('after')"]) as child:
        expect_output(child, "ready")
        output, error = child.communicate(timeout=10)
        assert output == "after\n" and error == ""
        assert child.returncode == 0
