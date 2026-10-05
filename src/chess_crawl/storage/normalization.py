"""Per-game normalization checkpoints; partial sources are never certified."""
from __future__ import annotations

from chess_crawl.storage.db import Connection, atomic, operation_lock, require_row


def observation_id(conn: Connection, raw_id: int) -> int:
    return int(require_row(conn.execute(
        "SELECT COALESCE(MAX(id),0) FROM fetch_logs WHERE raw_payload_id=%s AND status_code IN (200,304)",
        (raw_id,),
    ))[0])


@atomic
def begin_normalization(conn: Connection, raw_id: int, parser_version: str) -> int:
    operation_lock(conn, "payload-normalization", raw_id)
    raw = require_row(conn.execute("SELECT parser_version,normalization_status FROM raw_payloads WHERE id=%s", (raw_id,)))
    observed = observation_id(conn, raw_id)
    progress = conn.execute(
        "SELECT * FROM payload_normalization_runs WHERE raw_payload_id=%s AND parser_version=%s FOR UPDATE",
        (raw_id, parser_version),
    ).fetchone()
    reset = (progress is None or progress["observation_id"] != observed
             or (progress["state"] == "complete" and raw["parser_version"] != parser_version))
    if reset:
        conn.execute("DELETE FROM normalization_items WHERE raw_payload_id=%s AND parser_version=%s", (raw_id, parser_version))
        conn.execute(
            """INSERT INTO payload_normalization_runs(raw_payload_id,parser_version,observation_id,state)
               VALUES(%s,%s,%s,'pending') ON CONFLICT(raw_payload_id,parser_version)
               DO UPDATE SET observation_id=excluded.observation_id,state='pending'""",
            (raw_id, parser_version, observed),
        )
        conn.execute("UPDATE raw_payloads SET normalization_status='pending' WHERE id=%s", (raw_id,))
    return observed


def processed_items(conn: Connection, raw_id: int, parser_version: str, observed: int) -> dict[str, int]:
    return {str(row["json_pointer"]): int(row["game_id"]) for row in conn.execute(
        "SELECT json_pointer,game_id FROM normalization_items WHERE raw_payload_id=%s AND parser_version=%s AND observation_id=%s",
        (raw_id, parser_version, observed),
    )}


@atomic
def mark_item(conn: Connection, raw_id: int, parser_version: str, pointer: str, observed: int, game_id: int) -> None:
    conn.execute(
        """INSERT INTO normalization_items(raw_payload_id,parser_version,json_pointer,observation_id,game_id)
           VALUES(%s,%s,%s,%s,%s) ON CONFLICT(raw_payload_id,parser_version,json_pointer)
           DO UPDATE SET observation_id=excluded.observation_id,game_id=excluded.game_id
           WHERE excluded.observation_id>=normalization_items.observation_id""",
        (raw_id, parser_version, pointer, observed, game_id),
    )


@atomic
def finish_normalization(conn: Connection, raw_id: int, parser_version: str, observed: int, pointers: list[str]) -> bool:
    operation_lock(conn, "payload-normalization", raw_id)
    current = observation_id(conn, raw_id)
    completed = set(processed_items(conn, raw_id, parser_version, observed))
    complete = current == observed and set(pointers) <= completed
    if complete:
        conn.execute(
            """UPDATE payload_normalization_runs SET state='complete'
                 WHERE raw_payload_id=%s AND parser_version=%s AND observation_id=%s""",
            (raw_id, parser_version, observed),
        )
    return complete
