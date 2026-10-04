# Player evidence and supplementary resources

Player data belongs to the archive's provider account, independently of any
application user. A matching name on two providers does not establish the same
person. Stable Chess.com `player_id` evidence can reconcile a rename and a sparse
placeholder; conflicting stable IDs still fail reconciliation. The archive
retains observed aliases and the original names attached to game participants.
Alias lookup uses the current username first and falls back only when an alias
identifies exactly one account, so a reused username does not select its former
holder.

## Complete profiles and observations

Migration `0008_player_resources` adds queryable JSONB `native_data` containing
the complete profile/statistics JSON. Unknown fields, null, false, and zero are
preserved. Common account facts also have typed snapshot columns: creation and
last activity times, name, location, avatar/profile URLs, verification, streamer
status, and FIDE rating. Original provider timestamps retain their precision in
native JSON; convenience timestamps are Unix seconds. Chess.com's `fide` field
is a rating, never an identity number. A Lichess flag is not inferred to be a
country. Supplied FIDE identities remain separately named native fields.
The streamer convenience flag maps Chess.com's `is_streamer` and Lichess's
`streaming`; the latter describes current streaming activity. Native fields
retain these provider-specific meanings and the separate Lichess streamer data.

`user_rating_records` makes supplied ratings, best/lowest ratings and dates, deviations,
provisional flags, records, game totals, and progress queryable by provider
performance name. Unreported counts remain null. All native rating/statistic
fields remain available in JSONB. Historical ratings reconstructed from games
are a separate future calculation, not provider-supplied history.
The rich player read includes the latest Chess.com statistics snapshot separately
from its latest profile. Replaying older statistics follows its recorded account
identity and cannot recreate or rename an account from an outdated request name.

Snapshot content is deduplicated; `user_observations` records each successful
HTTP 200/304 occurrence separately. An A → B → A profile therefore has two
distinct bodies and three observations. Replaying source bytes updates their
interpretation without inventing a fetch. Fetch-log IDs break equal timestamp
ties. The migration recovers occurrences from retained fetch logs and source
links; a raw-only source supplies one observation when no fetch was recorded.

Schema migration does not download player data. Existing snapshots initially
retain their older projections and observations; replay archived responses
with `users-normalizer-v5` to populate complete facts and ratings. Historical
Lichess profile captures are conservatively quarantined at migration time,
because retained request metadata cannot establish whether OAuth was used.
This works for inline, compressed, and external source bodies without reading
them inside the schema transaction. A successful relationship-free replay
publishes the capture as public; unrelated sources remain usable throughout.

The public Lichess profile collector explicitly requests profile, trophies,
performance ranks, and public FIDE ID. It omits OAuth authorization because
authenticated profile responses can contain caller-relative following/blocking
facts. If a legacy profile contains those relationship fields, normalization
preserves and quarantines its raw payload under
`unassigned:legacy-profile`, then reports the need for explicit ownership.
It does not put those facts into the shared profile. Assigning and processing
private profile relationships requires a separately scoped collector; the
original compressed source remains retained for that work.

## Supplementary resource catalog

`providers/resources.py` owns registered resource keys, URL templates, required
parameters, authentication, pagination, freshness, access scope, and coverage
notes. Collectors accept a registered key rather than a caller-supplied URL.
These resources use the existing provider pacing, retry logging, raw persistence,
normalization, and replay implementations. Each current resource is one response
with no documented pagination. Resources from future adapters can be registered
explicitly; the provider database constraint permits new provider rows, but a
registration alone does not implement acquisition or imply an available FIDE
game corpus.

| Provider | Resource key | Parameters | Authentication / coverage |
| --- | --- | --- | --- |
| Chess.com | `clubs` | None | Public; supplied current memberships and join/activity dates. |
| Chess.com | `matches` | None | Public; provider-listed registered, active, and finished team matches. |
| Chess.com | `tournaments` | None | Public; provider-listed tournament participation. |
| Chess.com | `online` | None | Public; transient online observation. |
| Lichess | `rating-history` | None | Optional OAuth can generate history. An anonymous empty array may mean no cached history. |
| Lichess | `performance` | Required `perf` | Public; a documented performance/variant key such as `blitz` or `chess960`. |
| Lichess | `activity` | None | Public; provider activity window, not lifetime coverage. |
| Lichess | `teams` | None | OAuth with `team:read`; hidden memberships depend on the authenticated caller. |

Chess.com refreshes respect conditional validators and response cache headers.
Lichess acquisition is serialized per provider with a full-minute wait after HTTP
429; separate local-processing jobs can continue. Refreshes
are explicit resource jobs; opening a stored player profile does not fetch
resources or historical games.

`user_resource_snapshots` retains complete native JSON, parameters, parser
version, and coverage status (`observed`, `empty`, `partial`, or `unknown`).
`user_resource_observations` supplies occurrence history independently of body
deduplication. Failed/unavailable HTTP requests remain recorded in
`user_resource_acquisition` and fetch logs; they are not normalized as empty
lists. `rating_history_points` additionally stores valid Lichess daily rating
points with their source pointers. Zero-based source months are converted to
calendar dates. Malformed/new point formats remain in JSONB and mark coverage
partial instead of silently disappearing.

## Ownership and API integration

Public sources use `owner_scope = public`. Authenticated-sensitive team sources
require a workspace scope on both raw payloads and normalized resource data.
Their source keys and body deduplication are scoped. Public resource/history
reads exclude another workspace's data; callers pass the authenticated
workspace to include their own data. The literal `public` is reserved for the
shared archive and must not be accepted as a tenant workspace identifier.

The configured OAuth token must also belong to the requesting workspace.
`CHESS_CRAWL_LICHESS_TOKEN_OWNER_SCOPE` defaults to `local`; set it to the same
trusted workspace identifier as the API credential. A teams request with
another workspace fails before contacting Lichess. This prevents a single
operator credential from granting its hidden memberships to every SaaS
workspace. A future credentials registry can select each workspace's provider
token using the same ownership contract. Tokens are never stored in resource
parameters, source keys, or fetch headers.

The storage readers `player_profile`, `profile_history`, `resource_current`,
`resource_history`, and `resource_attempts` are reusable by API handlers. History
uses observation-ID cursors with page sizes 1–1000. Resource acquisition is
`ingest.fetch_user_resource`; its job payload contains the provider, username,
resource key, validated parameters, and trusted owner scope. API routes and the
worker dispatch are integrated in their companion changes.

API raw reads, replay selections, counts, and exports must filter raw
`owner_scope` to public plus the authenticated workspace. Internal normalizers
can read a known authorized raw ID; `read_raw_payload` is not itself a public
authorization boundary.

## Primary provider references

- [Chess.com Published-Data API](https://www.chess.com/news/view/published-data-api)
- [Lichess public profile and optional fields](https://github.com/lichess-org/api/blob/master/doc/specs/tags/users/api-user-username.yaml)
- [Lichess authenticated-only relationship fields](https://github.com/lichess-org/api/blob/master/doc/specs/schemas/UserExtended.yaml)
- [Lichess rating history](https://github.com/lichess-org/api/blob/master/doc/specs/tags/users/api-user-username-rating-history.yaml)
- [Lichess performance statistics](https://github.com/lichess-org/api/blob/master/doc/specs/tags/users/api-user-username-perf-perf.yaml)
- [Lichess activity](https://github.com/lichess-org/api/blob/master/doc/specs/tags/users/api-user-username-activity.yaml)
- [Lichess teams](https://github.com/lichess-org/api/blob/master/doc/specs/tags/teams/api-team-of-username.yaml)
- [Lichess team controller authentication/visibility](https://github.com/lichess-org/lila/blob/master/app/controllers/TeamApi.scala)
- [Lichess public profile controller](https://github.com/lichess-org/lila/blob/master/app/controllers/Api.scala)
