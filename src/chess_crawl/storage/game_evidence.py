"""Store immutable game revisions and read their queryable evidence."""
from __future__ import annotations

from dataclasses import asdict
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from psycopg.types.json import Jsonb

from chess_crawl.normalize.codes import canonical_hash
from chess_crawl.normalize.game_evidence import EVIDENCE_VERSION, GameEvidence, MoveNode
from chess_crawl.providers.base import NormalizedGame
from chess_crawl.storage.db import Connection, atomic, consistent_read, require_row


class EvidencePreparationRequired(Exception):
    """A preflight revision vanished; retry with evidence parsed outside writes."""


def _revision_hash(game: NormalizedGame) -> str:
    return canonical_hash({"source": dict(game.source_data), "pgn": game.pgn,
                           "game_hash": game.content_hash})


def reusable_game_sources(
    conn: Connection, raw_payload_id: int, games: Iterable[tuple[str, NormalizedGame]],
) -> set[str]:
    """Read a payload's reusable revisions once, including large monthly sources."""
    hashes: dict[str, set[str]] = {}
    for row in conn.execute(
        """SELECT s.json_pointer,v.content_hash
           FROM game_version_sources s JOIN game_versions v ON v.id = s.version_id
           WHERE s.raw_payload_id = %s AND v.parser_version = %s""",
        (raw_payload_id, EVIDENCE_VERSION),
    ):
        hashes.setdefault(row["json_pointer"], set()).add(row["content_hash"])
    return {pointer for pointer, game in games if pointer in hashes and _revision_hash(game) in hashes[pointer]}


@atomic
def store_game_evidence(
    conn: Connection, *, game_id: int, game: NormalizedGame, raw_payload_id: int,
    json_pointer: str, fetched_at: int, evidence: GameEvidence | None = None,
) -> int:
    """Reuse an identical revision and retain every source association."""
    revision_hash = _revision_hash(game)
    row = conn.execute(
        "SELECT id FROM game_versions WHERE game_id = %s AND content_hash = %s AND parser_version = %s",
        (game_id, revision_hash, EVIDENCE_VERSION),
    ).fetchone()
    if row is None:
        if evidence is None:
            raise EvidencePreparationRequired("Game evidence must be prepared before persistence")
        parsed = evidence
        row = require_row(conn.execute(
            """INSERT INTO game_versions(game_id,content_hash,parser_version,first_seen_at,
                   headers,header_items,source_metadata,move_text_origin,starting_fen,variant,parse_status,
                   parse_issues,played_ply_count,time_control_rules)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (game_id, revision_hash, EVIDENCE_VERSION, fetched_at, Jsonb(parsed.headers),
             Jsonb(parsed.header_items), Jsonb(parsed.source_metadata), parsed.move_text_origin, parsed.starting_fen,
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


def game_source_needs_refresh(conn: Connection, game_id: int, raw_payload_id: int) -> bool:
    """A previously processed source can become current through a new observation.

    Reusing its normalized ID is safe while it already supplies the current
    revision, or while a newer source still supersedes it. A recurring latest
    body must refresh both the revision pointer and mutable game metadata.
    """
    current = conn.execute(
        """SELECT 1 FROM games g JOIN game_version_sources s ON s.version_id = g.current_version_id
             WHERE g.id = %s AND s.raw_payload_id = %s LIMIT 1""",
        (game_id, raw_payload_id),
    ).fetchone()
    return current is None and is_latest_game_source(conn, game_id, raw_payload_id)


@consistent_read
def read_game_version(conn: Connection, game_id: int, version_id: int | None = None) -> dict[str, Any] | None:
    """Return one coherent revision belonging to this game; decimals stay exact."""
    # One statement remains coherent in an inherited READ COMMITTED transaction.
    # PGN lexical tokens are only read by the export path below.
    row = conn.execute(
        """SELECT v.*,
           COALESCE((SELECT jsonb_agg(to_jsonb(n) ORDER BY n.node_index)
             FROM game_move_nodes n WHERE n.version_id=v.id),'[]'::jsonb) AS nodes,
           COALESCE((SELECT jsonb_agg(to_jsonb(c) || jsonb_build_object(
               'seconds',c.seconds::text,'precision_seconds',c.precision_seconds::text) ORDER BY c.id)
             FROM game_clock_observations c WHERE c.version_id=v.id),'[]'::jsonb) AS clocks,
           COALESCE((SELECT jsonb_agg(to_jsonb(t) || jsonb_build_object('seconds',t.seconds::text)
               ORDER BY t.node_index,t.method_version)
             FROM game_derived_timings t WHERE t.version_id=v.id),'[]'::jsonb) AS derived_timings,
           COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY s.raw_payload_id,s.json_pointer)
             FROM game_version_sources s WHERE s.version_id=v.id),'[]'::jsonb) AS sources
           FROM game_versions v JOIN games g ON g.id = v.game_id
           WHERE g.id = %s AND v.id = COALESCE(%s,g.current_version_id)""",
        (game_id, version_id),
    ).fetchone()
    if row is None:
        return None
    result = dict(row)
    result["clocks"] = [_exact_values(item, numeric_fields=("seconds", "precision_seconds"))
                        for item in result["clocks"]]
    result["derived_timings"] = [_exact_values(item, numeric_fields=("seconds",))
                                for item in result["derived_timings"]]
    return result


def _exact_values(values: dict[str, Any], *, numeric_fields: tuple[str, ...] = ()) -> dict[str, Any]:
    # NUMERIC enters JSON as text, never through binary floating point. Decimal
    # restores the existing string notation, including very small exponents.
    values = {key: Decimal(value) if key in numeric_fields and value is not None else value
              for key, value in values.items()}
    return {key: str(value) if isinstance(value, Decimal) else value for key, value in values.items()}


@consistent_read
def export_game_version_pgn(
    conn: Connection, game_id: int, version_id: int | None = None, *, allow_partial: bool = False,
) -> str:
    """Reconstruct PGN from lexical DB records, without accessing the backup.

    Retains source headers, comments, variations, annotations and unknown
    syntax. Whitespace is normalized. Partial/unsupported exports need explicit
    permission so callers cannot confuse preserved notation with legal replay.
    """
    # Select status and lexical text together so a concurrent current-version
    # switch/deletion cannot combine the status of one revision with another.
    revision = conn.execute(
        """SELECT v.parse_status,
           COALESCE((SELECT jsonb_agg(jsonb_build_object('kind',p.kind,'token_text',p.token_text)
               ORDER BY p.token_index) FROM game_pgn_tokens p WHERE p.version_id=v.id),'[]'::jsonb) AS tokens
           FROM game_versions v JOIN games g ON g.id = v.game_id
           WHERE g.id = %s AND v.id = COALESCE(%s,g.current_version_id)""",
        (game_id, version_id),
    ).fetchone()
    if revision is None:
        raise ValueError("Game evidence version does not exist")
    if revision["parse_status"] != "complete" and not allow_partial:
        raise ValueError("Game evidence is not completely interpreted; explicit partial export is required")
    tokens = revision["tokens"]
    if not tokens:
        raise ValueError("Game move evidence is unavailable")
    output = []
    in_headers = True
    for token in tokens:
        kind, text = token["kind"], str(token["token_text"])
        if kind == "byte_order_mark":
            output.append(text)
        elif kind == "header":
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
