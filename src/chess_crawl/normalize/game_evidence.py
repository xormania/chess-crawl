"""Lossless lexical evidence plus interpreted move and clock observations.

pgn-read supplies the PGN grammar and standard-chess validation. This adapter
does not implement chess rules. Unsupported variant notation remains queryable
without inventing board positions, UCI moves, or legality claims.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import re
from typing import Any

from pgn_read.core import constants as c  # type: ignore[import-untyped]
from pgn_read.core.game import Game  # type: ignore[import-untyped]
from pgn_read.core.game_text_pgn import import_format  # type: ignore[import-untyped]
from pgn_read.core.gamedata import generate_fen_for_position  # type: ignore[import-untyped]
from pgn_read.core.parser import add_token_to_game  # type: ignore[import-untyped]

from chess_crawl.providers.base import NormalizedGame


EVIDENCE_VERSION = "game-evidence-v2"
INITIAL_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"
_MOVES = {c.IFG_PIECE_MOVE, c.IFG_PIECE_DESTINATION, c.IFG_PAWN_TO_RANK,
          c.IFG_PAWN_PROMOTE_TO_RANK, c.IFG_PAWN_PROMOTE_PIECE, c.IFG_CASTLES, c.IFG_PASS}
_KINDS = {
    c.IFG_END_TAG: "header", c.IFG_GAME_TERMINATION: "result", c.IFG_MOVE_NUMBER: "move_number",
    c.IFG_DOTS: "dots", c.IFG_COMMENT: "comment", c.IFG_COMMENT_TO_EOL: "comment_eol",
    c.IFG_START_RAV: "variation_start", c.IFG_END_RAV: "variation_end",
    c.IFG_NUMERIC_ANNOTATION_GLYPH: "nag", c.IFG_TRADITIONAL_ANNOTATION: "symbolic_nag",
    c.IFG_CHECK_INDICATOR: "check", c.IFG_ESCAPE: "escape", c.IFG_RESERVED: "reserved",
    c.IFG_END_OF_FILE_MARKER: "eof_marker",
}
_ANNOTATION = re.compile(r"\[%([A-Za-z0-9_]+)([^\]]*)\]")


@dataclass
class MoveNode:
    node_index: int
    parent_index: int | None
    variation_index: int
    ply: int
    is_mainline: bool
    mover: str | None = None
    move_uci: str | None = None
    move_san: str | None = None
    fen_before: str | None = None
    fen_after: str | None = None
    position_key_before: str | None = None
    position_key_after: str | None = None
    comments: list[str] = field(default_factory=list)
    starting_comments: list[str] = field(default_factory=list)
    nags: list[int | str] = field(default_factory=list)
    annotations: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ClockObservation:
    node_index: int | None
    kind: str
    source: str
    source_pointer: str
    raw_text: str
    seconds: Decimal | None
    unit: str
    precision_seconds: Decimal | None
    status: str
    reference: str


@dataclass
class GameEvidence:
    headers: dict[str, str]
    header_items: list[dict[str, str]]
    source_metadata: dict[str, Any]
    move_text_origin: str
    starting_fen: str | None
    variant: str
    parse_status: str
    parse_issues: list[dict[str, Any]]
    time_control_rules: dict[str, Any]
    nodes: list[MoveNode]
    tokens: list[dict[str, Any]]
    clocks: list[ClockObservation]

    @property
    def played_ply_count(self) -> int:
        return sum(node.is_mainline and node.node_index > 0 for node in self.nodes)


def parse_game_evidence(game: NormalizedGame) -> GameEvidence:
    """Preserve every lexical token and native field, even on parsing errors."""
    native = dict(game.source_data)
    text = game.pgn
    moves_only = not text and isinstance(native.get("moves"), str)
    if moves_only:
        # Lichess JSON can carry SAN moves without pgnInJson. Synthetic tags
        # are parser context, not falsely represented as supplied PGN tags.
        text = native["moves"] + " *"
    # Omit notation represented by tokens, retaining parallel or unusable native
    # moves unchanged when PGN supplies the interpreted text.
    tokenized_fields = {"pgn", "moves"} if moves_only else {"pgn"}
    metadata = {key: value for key, value in native.items() if key not in tokenized_fields}
    origin = "provider.moves" if moves_only else "pgn" if text else "unavailable"
    if not text:
        unavailable = GameEvidence({}, [], metadata, origin, _initial_fen({}, native), game.variant_key,
                                   "unavailable", [{"kind": "moves_unavailable"}],
                                   parse_clock_rules(game.time_control_raw, native),
                                   [MoveNode(0, None, 0, 0, True)], [], [])
        _provider_observations(unavailable, native)
        return unavailable

    bom = text.startswith("\ufeff")
    matches = list(import_format.finditer(text, 1 if bom else 0))
    headers: dict[str, str] = {}
    header_items: list[dict[str, str]] = []
    for match in matches:
        if match.lastindex == c.IFG_END_TAG:
            name, value = match.group(c.IFG_TAG_NAME), match.group(c.IFG_TAG_VALUE)
            # Preserve duplicate headers in ordered items as well as the
            # conventional final-value mapping.
            headers[name] = value
            header_items.append({"name": name, "value": value})
    variant = headers.get("Variant", game.variant_key).lower().replace(" ", "")
    supported = variant in {"standard", "chess", "fromposition"} and game.variant_key in {"standard", "fromposition"}
    start_fen = _initial_fen(headers, native)
    root = MoveNode(0, None, 0, 0, True, fen_after=start_fen if supported else None)
    evidence = GameEvidence(headers, header_items, metadata, origin, start_fen, game.variant_key,
                            "complete" if supported else "unsupported", [],
                            parse_clock_rules(headers.get("TimeControl", game.time_control_raw), native),
                            [root], [], [])
    if not supported:
        evidence.parse_issues.append({"kind": "variant_rules_unavailable", "variant": variant})
    validator = Game()
    if "FEN" not in headers and start_fen != INITIAL_FEN:
        validator.pgn_tags.update(FEN=start_fen, SetUp="1")
    if supported:
        initial_validator = Game()
        initial_validator.pgn_tags.update(headers)
        if "FEN" not in headers and start_fen != INITIAL_FEN:
            initial_validator.pgn_tags.update(FEN=start_fen, SetUp="1")
        try:
            if initial_validator.set_initial_position():
                root.fen_after = _board_fen(initial_validator)
            else:
                root.fen_after = None
                validator.set_game_error()
                evidence.parse_issues.append({"kind": "invalid_starting_position"})
        except (ValueError, KeyError, IndexError, TypeError):
            root.fen_after = None
            validator.set_game_error()
            evidence.parse_issues.append({"kind": "invalid_starting_position"})
    current = 0
    rav: list[int] = []
    children: dict[int, int] = {}
    pending_comments: list[str] = []
    at_variation_start = False
    terminated = False
    previous_end = 1 if bom else 0
    if bom:
        evidence.tokens.append({"token_index": len(evidence.tokens), "kind": "byte_order_mark", "token_text": text[:1],
                                "start_offset": 0, "end_offset": 1, "interpretation_status": "parsed"})
    for match in matches:
        group = match.lastindex
        token = match.group()
        kind = "move" if group in _MOVES else _KINDS.get(group, "unknown")
        if text[previous_end:match.start()].strip():
            evidence.parse_issues.append({"kind": "unrecognized_gap", "start": previous_end, "end": match.start()})
            evidence.tokens.append({"token_index": len(evidence.tokens), "kind": "unknown_gap",
                                    "token_text": text[previous_end:match.start()], "start_offset": previous_end,
                                    "end_offset": match.start(), "interpretation_status": "preserved"})
        previous_end = match.end()
        index = len(evidence.tokens)
        lexical = {"token_index": index, "kind": kind, "token_text": token,
                   "start_offset": match.start(), "end_offset": match.end(),
                   "interpretation_status": "parsed" if supported else "preserved"}
        evidence.tokens.append(lexical)
        if supported:
            try:
                add_token_to_game(text, validator, match.start())
            except (ValueError, KeyError, IndexError, TypeError) as error:
                validator.set_game_error()
                evidence.parse_issues.append({"kind": "parser_error", "token": index, "detail": str(error)})
        if kind == "variation_start":
            rav.append(current)
            parent_index = evidence.nodes[current].parent_index
            if parent_index is None:
                evidence.parse_issues.append({"kind": "variation_without_move", "token": index})
            current = parent_index if parent_index is not None else 0
            at_variation_start = True
        elif kind == "variation_end":
            if rav:
                current = rav.pop()
            else:
                evidence.parse_issues.append({"kind": "unmatched_variation_end", "token": index})
            at_variation_start = False
        elif kind == "move":
            parent_node = evidence.nodes[current]
            before = parent_node.fen_after
            mover = _mover_from_fen(before) if before else _mover_for_ply(start_fen, parent_node.ply)
            node = MoveNode(len(evidence.nodes), current, children.get(current, 0), parent_node.ply + 1,
                            not rav, mover=mover, move_san=token.strip(), fen_before=before,
                            starting_comments=pending_comments)
            pending_comments = []
            children[current] = node.variation_index + 1
            if supported and validator.state is None and group != c.IFG_PASS:
                node.fen_after = _board_fen(validator)
                node.move_uci = _uci_from_delta(validator, mover)
                node.position_key_before = _position_key(before, game.variant_key)
                node.position_key_after = _position_key(node.fen_after, game.variant_key)
            elif supported:
                lexical["interpretation_status"] = "invalid"
                evidence.parse_issues.append({"kind": "invalid_move", "token": index, "san": token})
            evidence.nodes.append(node)
            current = node.node_index
            at_variation_start = False
        elif kind in {"comment", "comment_eol"}:
            comment = token[1:-1] if kind == "comment" else token[1:]
            if at_variation_start:
                pending_comments.append(comment)
            else:
                evidence.nodes[current].comments.append(comment)
            _comment_observations(evidence, None if at_variation_start else current, comment, index)
        elif kind in {"nag", "symbolic_nag"}:
            nag: int | str = int(token[1:]) if kind == "nag" else token
            evidence.nodes[current].nags.append(nag)
        elif kind == "check" and current:
            evidence.nodes[current].move_san = (evidence.nodes[current].move_san or "") + token
        elif kind == "result":
            if terminated:
                evidence.parse_issues.append({"kind": "multiple_games", "token": index})
            if not rav:
                terminated = True
        elif kind == "unknown":
            lexical["interpretation_status"] = "preserved"
            evidence.parse_issues.append({"kind": "uninterpreted_token", "token": index, "text": token})
    if text[previous_end:].strip():
        evidence.parse_issues.append({"kind": "unrecognized_tail", "start": previous_end})
        evidence.tokens.append({"token_index": len(evidence.tokens), "kind": "unknown_tail",
                                "token_text": text[previous_end:], "start_offset": previous_end,
                                "end_offset": len(text), "interpretation_status": "preserved"})
    if rav:
        evidence.parse_issues.append({"kind": "unclosed_variation", "depth": len(rav)})
    if not terminated:
        evidence.parse_issues.append({"kind": "missing_result"})
    if pending_comments:
        root.starting_comments.extend(pending_comments)
    if supported and (evidence.parse_issues or not validator.game_ok):
        evidence.parse_status = "partial"
    _provider_observations(evidence, native)
    return evidence


def _initial_fen(headers: dict[str, str], native: dict[str, Any]) -> str:
    return str(headers.get("FEN") or native.get("initialFen") or native.get("initial_setup") or INITIAL_FEN)


def _board_fen(game: Any) -> str:
    return str(generate_fen_for_position(game.piece_placement_data.values(), game.active_color,
                                       game.castling_availability, game.en_passant_target_square,
                                       game.halfmove_clock, game.fullmove_number))


def _uci_from_delta(game: Any, mover: str | None) -> str | None:
    removed, placed = game.position_deltas[-1]
    origins = [(square, piece) for square, piece in removed[0] if piece.name.isupper() == (mover == "white")]
    destinations = [(square, piece) for square, piece in placed[0] if piece.name.isupper() == (mover == "white")]
    if len(origins) > 1:  # The established parser supplies the castling delta.
        origins = [(square, piece) for square, piece in origins if piece.name.lower() == "k"]
        destinations = [(square, piece) for square, piece in destinations if piece.name.lower() == "k"]
    if len(origins) != 1 or len(destinations) != 1:
        return None
    origin, piece_before = origins[0]
    destination, piece_after = destinations[0]
    promotion = piece_after.name.lower() if piece_before.name.lower() == "p" and piece_after.name.lower() != "p" else ""
    return str(origin + destination + promotion)


def _mover_from_fen(fen: str) -> str:
    return "white" if fen.split()[1] == "w" else "black"


def _mover_for_ply(fen: str | None, ply: int) -> str:
    white = not fen or len(fen.split()) < 2 or fen.split()[1] == "w"
    return "white" if white != bool(ply % 2) else "black"


def _position_key(fen: str | None, variant: str) -> str | None:
    if not fen:
        return None
    # Versioned conservative identity includes the FEN en-passant square even
    # when no legal capture exists. It never incorrectly merges positions.
    state = " ".join(fen.split()[:4])
    return "fen-state-v1:sha256:" + hashlib.sha256((variant + "\n" + state).encode()).hexdigest()


def _comment_observations(evidence: GameEvidence, node_index: int | None, comment: str, token_index: int) -> None:
    for index, match in enumerate(_ANNOTATION.finditer(comment)):
        command, value = match.group(1), match.group(2).strip()
        annotation = {"command": command, "value": value, "raw_text": match.group(),
                      "source": "pgn", "source_pointer": f"/tokens/{token_index}/annotations/{index}"}
        evidence.nodes[node_index or 0].annotations.append(annotation)
        if command.lower() in {"clk", "emt"}:
            seconds, resolution = _parse_clock(value)
            evidence.clocks.append(ClockObservation(
                node_index, "remaining" if command.lower() == "clk" else "elapsed", "pgn",
                annotation["source_pointer"], match.group(), seconds, "seconds", resolution,
                "parsed" if seconds is not None and node_index not in {None, 0} else "invalid" if seconds is None else "unmapped",
                "after_move" if command.lower() == "clk" and node_index else "elapsed_move" if node_index else "unknown",
            ))


def _parse_clock(value: str) -> tuple[Decimal | None, Decimal | None]:
    # PGN clock/elapsed extension is hours:minutes:seconds, optionally with a
    # decimal second part. Never use float for observed timing evidence.
    match = re.fullmatch(r"([0-9]+):([0-9]{2}):([0-9]{2})(?:\.([0-9]+))?", value)
    if not match or int(match[2]) >= 60 or int(match[3]) >= 60:
        return None, None
    fraction = match[4] or ""
    # PostgreSQL unconstrained NUMERIC supports at most 16,383 fractional
    # digits. Out-of-range values remain textual invalid observations.
    if len(fraction) > 16383:
        return None, None
    try:
        with localcontext() as context:
            context.prec = max(28, len(match[1]) + len(fraction) + 10)
            seconds = Decimal(match[1]) * 3600 + Decimal(match[2]) * 60 + Decimal(match[3])
            if fraction:
                seconds += Decimal("0." + fraction)
            if seconds.adjusted() > 131071:
                return None, None
            return seconds, Decimal(1).scaleb(-len(fraction))
    except InvalidOperation:
        return None, None


def _provider_observations(evidence: GameEvidence, native: dict[str, Any]) -> None:
    played = [node for node in evidence.nodes if node.node_index > 0 and node.is_mainline]
    clocks = native.get("clocks")
    if isinstance(clocks, list):
        for index, value in enumerate(clocks):
            seconds = None
            try:
                if isinstance(value, int) and not isinstance(value, bool):
                    # The documented provider representation is integer
                    # centiseconds. Reject other shapes, preserving their JSON.
                    with localcontext() as context:
                        context.prec = max(28, len(str(value)) + 4)
                        candidate = Decimal(value) / 100
                    if candidate.is_finite() and candidate >= 0:
                        seconds = candidate
            except InvalidOperation:
                pass
            node_index = played[index].node_index if index < len(played) else None
            evidence.clocks.append(ClockObservation(
                node_index, "remaining", "provider", f"/clocks/{index}", json.dumps(value, separators=(",", ":")), seconds,
                "centiseconds", Decimal("0.01"), "invalid" if seconds is None else "parsed" if node_index else "unmapped",
                "after_move" if node_index else "unknown",
            ))
    analysis = native.get("analysis")
    if isinstance(analysis, list):
        for index, value in enumerate(analysis):
            node = played[index] if index < len(played) else evidence.nodes[0]
            node.annotations.append({"source": "provider", "source_pointer": f"/analysis/{index}", "payload": value})


def parse_clock_rules(label: str | None, native: dict[str, Any]) -> dict[str, Any]:
    """Interpret PGN time-control periods, preserving all original rules."""
    periods = []
    status = "parsed"
    if not label or label in {"?", "-"}:
        status = "unavailable"
    else:
        for raw_period in label.split(":"):
            match = re.fullmatch(r"(?:(\d+)/)?(\d+(?:\.\d+)?)(?:\+(\d+(?:\.\d+)?))?", raw_period)
            if not match:
                status = "preserved"
                break
            periods.append({"moves": int(match[1]) if match[1] else None, "seconds": match[2],
                            "increment_seconds": match[3], "raw_text": raw_period})
    return {"raw_label": label, "interpretation_status": status, "periods": periods,
            "provider_clock": native.get("clock"), "provider_time_control": native.get("time_control")}
