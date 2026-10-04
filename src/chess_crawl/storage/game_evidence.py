"""Store immutable game revisions and read their queryable evidence."""
from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from typing import Any

from psycopg.types.json import Jsonb

from chess_crawl.normalize.codes import canonical_hash
from chess_crawl.normalize.game_evidence import EVIDENCE_VERSION, GameEvidence, MoveNode, parse_game_evidence
from chess_crawl.providers.base import NormalizedGame
from chess_crawl.storage.db import Connection, atomic, require_row


@atomic
def store_game_evidence(
    conn: Connection, *, game_id: int, game: NormalizedGame, raw_payload_id: int,
    json_pointer: str, fetched_at: int, evidence: GameEvidence | None = None,
) -> int:
    """Reuse an identical revision and retain every source association."""
    revision_hash = canonical_hash({"source": dict(game.source_data), "pgn": game.pgn,
                                    "game_hash": game.content_hash})
    row = conn.execute(
        "SELECT id FROM game_versions WHERE game_id = %s AND content_hash = %s AND parser_version = %s",
        (game_id, revision_hash, EVIDENCE_VERSION),
    ).fetchone()
    if row is None:
        parsed = evidence or parse_game_evidence(game)
        row = require_row(conn.execute(
            """INSERT INTO game_versions(game_id,content_hash,parser_version,first_seen_at,
                   headers,header_items,source_metadata,starting_fen,variant,parse_status,
                   parse_issues,played_ply_count,time_control_rules)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (game_id, revision_hash, EVIDENCE_VERSION, fetched_at, Jsonb(parsed.headers),
             Jsonb(parsed.header_items), Jsonb(parsed.source_metadata), parsed.starting_fen,
             parsed.variant, parsed.parse_status, Jsonb(parsed.parse_issues), parsed.played_ply_count,
             Jsonb(parsed.time_control_rules)),
        ))
        version_id = int(row["id"])
        _insert_evidence(conn, version_id, parsed)
    else:
        version_id = int(row["id"])
    conn.execute(
        """INSERT INTO game_version_sources(version_id,raw_payload_id,json_pointer,first_seen_at)
           VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (version_id, raw_payload_id, json_pointer, fetched_at),
    )
    # Fetch observations decide current state, including A -> B -> A body
    # recurrence. Replaying an old body never makes it current just because
    # its normalized revision received a larger generated ID.
    conn.execute(
        """UPDATE games SET current_version_id = (
             SELECT v.id FROM game_versions v
             JOIN game_version_sources s ON s.version_id = v.id
             JOIN raw_payloads r ON r.id = s.raw_payload_id
             LEFT JOIN fetch_logs f ON f.raw_payload_id = r.id AND f.status_code IN (200,304)
             WHERE v.game_id = games.id
             ORDER BY COALESCE(f.attempted_at,r.fetched_at) DESC,
                      f.id DESC NULLS LAST,r.id DESC,v.id DESC LIMIT 1)
           WHERE id = %s""", (game_id,),
    )
    conn.execute(
        """UPDATE games SET ply_count = v.played_ply_count
             FROM game_versions v WHERE games.id = %s AND v.id = games.current_version_id
               AND v.parse_status = 'complete'""", (game_id,),
    )
    return version_id


def _insert_evidence(conn: Connection, version_id: int, parsed: GameEvidence) -> None:
    with conn.cursor() as cursor:
        cursor.executemany(
            """INSERT INTO game_move_nodes(version_id,node_index,parent_index,variation_index,ply,
                 is_mainline,mover,move_uci,move_san,fen_before,fen_after,position_key_before,
                 position_key_after,comments,starting_comments,nags,annotations)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            [(version_id, node.node_index, node.parent_index, node.variation_index, node.ply,
              node.is_mainline, node.mover, node.move_uci, node.move_san, node.fen_before, node.fen_after,
              node.position_key_before, node.position_key_after, Jsonb(node.comments),
              Jsonb(node.starting_comments), Jsonb(node.nags), Jsonb(node.annotations)) for node in parsed.nodes],
        )
        cursor.executemany(
            """INSERT INTO game_pgn_tokens(version_id,token_index,kind,token_text,start_offset,end_offset,interpretation_status)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            [(version_id, token["token_index"], token["kind"], token["token_text"], token["start_offset"],
              token["end_offset"], token["interpretation_status"]) for token in parsed.tokens],
        )
        cursor.executemany(
            """INSERT INTO game_clock_observations(version_id,node_index,kind,source,source_pointer,
                   raw_text,seconds,unit,precision_seconds,status,reference)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            [(version_id, clock.node_index, clock.kind, clock.source, clock.source_pointer,
              clock.raw_text, clock.seconds, clock.unit, clock.precision_seconds, clock.status,
              clock.reference) for clock in parsed.clocks],
        )


def is_latest_game_source(conn: Connection, game_id: int, raw_payload_id: int) -> bool:
    """Protect mutable game metadata while preserving older source revisions."""
    row = conn.execute(
        """SELECT r.id FROM raw_payloads r
             LEFT JOIN fetch_logs f ON f.raw_payload_id = r.id AND f.status_code IN (200,304)
             WHERE r.id = %s OR r.id IN (
               SELECT raw_payload_id FROM source_records WHERE entity_type = 'game' AND entity_id = %s)
             ORDER BY COALESCE(f.attempted_at,r.fetched_at) DESC,
                      f.id DESC NULLS LAST,r.id DESC LIMIT 1""", (raw_payload_id, game_id),
    ).fetchone()
    return row is not None and int(row["id"]) == raw_payload_id


def read_game_version(conn: Connection, game_id: int, version_id: int | None = None) -> dict[str, Any] | None:
    """Return a revision belonging to this game; decimal evidence stays exact."""
    row = conn.execute(
        """SELECT v.* FROM game_versions v JOIN games g ON g.id = v.game_id
           WHERE g.id = %s AND v.id = COALESCE(%s,g.current_version_id)""", (game_id, version_id),
    ).fetchone()
    if row is None:
        return None
    result = dict(row)
    selected = int(row["id"])
    result["nodes"] = [dict(item) for item in conn.execute(
        "SELECT * FROM game_move_nodes WHERE version_id = %s ORDER BY node_index", (selected,))]
    result["clocks"] = [_exact_values(dict(item)) for item in conn.execute(
        "SELECT * FROM game_clock_observations WHERE version_id = %s ORDER BY id", (selected,))]
    result["derived_timings"] = [_exact_values(dict(item)) for item in conn.execute(
        "SELECT * FROM game_derived_timings WHERE version_id = %s ORDER BY node_index,method_version", (selected,))]
    result["sources"] = [dict(item) for item in conn.execute(
        "SELECT * FROM game_version_sources WHERE version_id = %s ORDER BY raw_payload_id,json_pointer", (selected,))]
    return result


def _exact_values(values: dict[str, Any]) -> dict[str, Any]:
    return {key: str(value) if isinstance(value, Decimal) else value for key, value in values.items()}


def export_game_version_pgn(
    conn: Connection, game_id: int, version_id: int | None = None, *, allow_partial: bool = False,
) -> str:
    """Reconstruct PGN from lexical DB records, without accessing the backup.

    Retains source headers, comments, variations, annotations and unknown
    syntax. Whitespace is normalized. Partial/unsupported exports need explicit
    permission so callers cannot confuse preserved notation with legal replay.
    """
    revision = read_game_version(conn, game_id, version_id)
    if revision is None:
        raise ValueError("Game evidence version does not exist")
    if revision["parse_status"] != "complete" and not allow_partial:
        raise ValueError("Game evidence is not completely interpreted; explicit partial export is required")
    tokens = conn.execute("SELECT kind,token_text FROM game_pgn_tokens WHERE version_id = %s ORDER BY token_index",
                          (revision["id"],)).fetchall()
    if not tokens:
        raise ValueError("Game move evidence is unavailable")
    output = []
    in_headers = True
    for token in tokens:
        kind, text = token["kind"], str(token["token_text"])
        if kind == "header":
            output.append(text + "\n")
        else:
            if in_headers:
                output.append("\n")
                in_headers = False
            if kind == "escape":
                output.append("\n" + text + "\n")
            elif kind == "comment_eol":
                output.append(text + "\n")
            else:
                output.append(text + " ")
    return "".join(output).strip() + "\n"


def evidence_contract() -> dict[str, Any]:
    """A versioned, machine-readable vocabulary for normalization clients."""
    return {"version": EVIDENCE_VERSION, "clock_kinds": ["remaining", "elapsed"],
            "clock_sources": ["pgn", "provider"], "statuses": ["complete", "partial", "unavailable", "unsupported"],
            "root_node": asdict(MoveNode(0, None, 0, 0, True))}
