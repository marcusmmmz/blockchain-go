# Networking and competitive mining plan

## Goal

Run a private experimental blockchain with friends. One node starts the chain
and serves as the initial peer; everyone runs a full node and mines independently.
Nodes relay blocks, validate them locally, and converge on the valid chain with
the most accumulated proof of work. The initial peer has no special authority
over which blocks win.

Implementation is underway on `feat/websocket-mining`. The core workflow is
implemented; see README.md for runnable commands and current limitations. This
changes the original block format and genesis and starts a new experimental chain.

## First release scope

- A shared, deterministic genesis and versioned chain configuration.
- Direct peer connections, initial synchronization, and block propagation.
- Independent mining with configurable CPU workers and miner labels.
- Block validation, competing branches, and reorganizations.
- Local persistence and restart recovery.
- Status showing canonical block counts per miner, stale blocks, and hash rate.

Initially accept only coinbase transactions: each block awards one experimental
unit to its miner label. Labels are display names, not authenticated identities
or spendable wallets. Ordinary transactions, signatures, a mempool, automatic
Internet peer discovery, and mining pools are later work.

## Current issues to resolve first

`main.go` currently generates genesis with the current time, so independent
processes start different chains. Mining starts at nonce zero with identical
coinbase contents, so peers can duplicate searches. Blocks append without
validation, state exists only in memory, and mining cannot be interrupted when
a peer finds a block. The single mutable target also cannot validate forks.

The starting target is 2^256, which accepts every hash. Difficulty uses unsigned
timestamp subtraction without timestamp validation or bounds on adjustment.
Those rules need to become deterministic, bounded consensus rules before peers
can accept each other's blocks.

## Design decisions

### 1. Shared consensus rules

Define a versioned network configuration with fixed genesis bytes, initial
target, maximum target, ten-second target spacing, and the per-block 1% difficulty rule.
Compute a chain ID from the canonical configuration, including genesis; peers
with different rules must fail the handshake. Ship a single default friends
network configuration rather than let every miner choose its own target.

Use canonical binary encoding for consensus hashes. Include previous hash,
timestamp, coinbase commitment, expected target, and a uint64 nonce in the
header. Encode the target as a fixed 32-byte unsigned integer, bounded to
1..2^256-1. The first version permits exactly one coinbase with a bounded miner
label, a random per-process mining tag, and a fixed reward. Validate its exact
shape and commitment; this is a single-transaction commitment until multiple
transactions and a real Merkle tree are introduced.

Derive expected target from the candidate's parent branch, never local arrival
order or a peer's claimed target. Compare only the most recent completed block
with its parent: intervals below ten seconds raise the next block's difficulty
by 1%, intervals above ten seconds lower it by 1%, and exactly ten seconds
leaves it unchanged. Use target multipliers `100/101` and `100/99` respectively,
with integer division rounding down and targets clamped to `1..2^256-1`.
Equal or decreasing timestamps count as fast without unsigned subtraction.
The first mined block uses the configured initial target; thereafter every
completed interval, including the first block versus genesis, drives an update.
Protocol version 2 and a new chain ID separate these rules from the old 60-block
retarget. Existing version-1 data directories cannot be reused.

Require timestamps greater than median time of up to 11 ancestors and no more
than two minutes ahead of local time. Treat future-dated blocks as temporarily
ineligible; allow resubmission once eligible. Validate target equality, PoW
(`hash < target`), parent linkage, encoding limits, and coinbase commitment
before accepting a block. Keep clocks synchronized for the experiment.

### 2. Chain selection and concurrency

Index valid blocks by hash, retaining parent, height, target, and cumulative
work. With the strict `hash < target` rule, define block work as
floor(2^256 / target); sum it from genesis. Select the branch with the greatest
cumulative work, rather than the greatest block count. On equal work retain
the current tip; a later block resolves the race.

Store side branches and switch the canonical index back to the common ancestor
on reorganization. Recompute miner standings from the canonical chain so stale
blocks never count as wins. There is no UTXO rollback in the coinbase-only MVP.

One chain manager serializes state changes. Network handlers and mining workers
submit candidates through the same validator. Workers receive immutable tip
snapshots and cancellation signals. A tip change cancels stale mining; results
are validated again before acceptance to handle races.

### 3. Mining without duplicate work

Give every mining job a cryptographically random tag in its coinbase.
Within a process, worker i searches nonces i, i+workerCount, and so on. This
separates work across processes and cores. Refresh timestamp and restart the
job when needed, preserving unique worker search ranges. Mine continuously
until shutdown instead of stopping after 1,000 blocks.

Expose `--workers` (default available logical CPUs) and `--mine=false` for a
relay-only node. Count actual hashes and show hashes/second over a rolling
window. Use measured hash rate to choose a shared initial target before
starting the experiment; the CLI exposes shared initial difficulty bits. A
dedicated benchmark command remains a later improvement.

### 4. Minimal peer protocol

Use WebSockets: Go `net/http` for the HTTP server and `github.com/coder/websocket`
for WebSocket connections. Serve `/p2p` and send one JSON protocol message per
WebSocket message, with explicitly exported wire DTOs. Use `wss://` for remote
peers and permit `ws://` for explicit local development connections. Current unexported struct fields
cannot be serialized directly. Encode hashes and targets as fixed-length hex,
and uint64 fields as decimal strings. Decode strictly into consensus types and
hash canonical binary bytes, never the JSON representation.

Messages:

- `hello`: protocol version, chain ID, node session ID, tip hash and height.
- `getblocks`: bounded block locator (recent ancestors, then exponential steps).
- `blocks`: parent-first batches of up to 128 blocks after the common ancestor.
- `block`: announce a newly mined or accepted block.
- WebSocket ping/pong control frames: connection health.

Choose a 1 MiB maximum reassembled message, 64 KiB maximum encoded block, and a 32-byte miner
label limit. Bound peer counts, queues, inbound rates, locator length, and
missing-parent storage. Set handshake/read/write deadlines; reject malformed
messages with a WebSocket read limit, including fragmented messages. Use one
reader and a bounded serialized writer per connection; keep compression disabled
initially. Deduplicate blocks by hash and
avoid echoing announcements to the sender. Resolve duplicate connections by
session ID, reject self-connections, and reconnect with bounded backoff.

On connection, use locators to find a common ancestor, download bounded batches,
and validate every block. Reported height/work are hints, never acceptance
criteria. Keep requesting batches until synchronized; support peers joining
late or returning after disconnection. On an unknown-parent announcement,
request synchronization and discard the announcement; canonical blocks are
recovered by synchronization, without an unbounded pending set. Relay accepted
side-branch blocks as well as tip changes.

### 5. Connectivity and persistence

Default to `127.0.0.1:7331`, serving the peer endpoint at `/p2p`. Use a named
Cloudflare Tunnel public hostname, for example `chain.example.com`, forwarding
to `http://127.0.0.1:7331`. Friends dial `wss://chain.example.com/p2p` using
repeatable `--peer` flags. Cloudflare terminates public TLS; the origin HTTP
connection stays local. Remote peers must verify TLS certificates normally.
Raw TCP published through Cloudflare requires a different client setup; the
WebSocket endpoint works with a normal public HTTP tunnel and ordinary clients.

Each peer connection is bidirectional: friends behind NAT can initiate a
connection to the host and both receive and announce blocks over it. They do
not each need a tunnel. The host relays blocks between connected friends;
optionally expose a second node through its own tunnel and connect everyone to
both for resilience. Without another reachable node, host downtime interrupts
communication between friends who have no direct connection.

Peer connections and status are open by default; no application token is
required. Every received block must pass consensus validation. Cloudflare Access
is optional: if enabled, noninteractive node clients need supported service-token
headers rather than a browser login. Keep Access credentials out of Git.

Use WebSocket ping/pong keepalives, deadlines, and reconnect with jittered backoff.
Cloudflare or origin restarts can close long-lived connections; on reconnect,
repeat the handshake and locator sync to recover missed blocks. Test this path
explicitly. Cloudflare Tunnel is connectivity infrastructure, not consensus;
it does not choose winning blocks. Document the named tunnel's hostname mapping
and how to start `cloudflared` separately from the Go node. Tunnel credentials
stay on the machine running `cloudflared`.

Persist validated blocks in an append-only, length-prefixed log under each
node's `--data-dir`. Serialize writes through the chain manager, flush before
announcing acceptance, and rebuild indexes by replaying and validating at
startup. Detect and truncate an incomplete final record after a crash; fail
clearly on corruption elsewhere. Record chain ID and hold an exclusive data
directory lock. Keep generated state outside tracked files.

## Implementation sequence and acceptance checks

1. **Consensus foundation:** separate consensus types/encoding/validation from
   `main`; fixed genesis and chain configuration; header nonce and miner tag;
   explicit difficulty and timestamp rules. Tests cover identical genesis,
   encoding vectors, invalid PoW/target/coinbase/time, and adjustment boundaries.
2. **Chain manager and fork choice:** branch index, cumulative work,
   reorganizations, persistence, and replay. Test a shorter harder branch
   beating a longer easier branch, ties, duplicates, forks with different per-block adjustments, restart, partial final writes, and corrupted records.
3. **Cancelable parallel miner:** worker partitioning, tip notifications, worker
   counts, hash-rate reporting, and graceful shutdown. Test distinct jobs and
   cancellation, including a remote block arriving during local completion.
4. **Peer transport and synchronization:** framing, handshake, bounded messages,
   reconnects, block relay, and locator synchronization. Test wrong chain/version,
   malformed/oversized messages, open upgrades without credentials, unknown
   parents, idle keepalives, and a late joining node.
5. **CLI and friends rehearsal:** wire `--listen`, repeatable URL-valued `--peer`,
   `--miner`, `--workers`, `--mine`, and `--data-dir`; document launch and Cloudflare Tunnel
   steps. Run three local processes with separate data directories and fixed
   test difficulty, then repeat on two real machines through an actual Cloudflare Tunnel.

Use easy, deterministic test configurations and injected clocks/controllable
mining for automated tests; do not assert a random miner wins a quota in a
short run. Run `go test -race ./...`, `go vet ./...`, and `go build ./...`.
The multi-process test must show all peers reaching the same tip after block
relay, late join, restart, and a partition/reconnect. Include a reorganization
and ensure mining continues after the winning remote tip arrives. Keep any
Cloudflare Tunnel rehearsal explicitly unverified until it has actually run.

## Running the competition

Proposed CLI after implementation:

```sh
# Initial peer; named tunnel forwards chain.example.com to this local listener:
blockchain-go --listen 127.0.0.1:7331 --miner alice --data-dir ./alice-data

# Friend; no inbound port or tunnel needed on this machine:
blockchain-go --listen 127.0.0.1:7331 --peer wss://chain.example.com/p2p \
  --miner bob --data-dir ./bob-data
```

Prepare the shared configuration together, benchmark each machine, and start
mining at approximately the same time after synchronization. Everyone must use
the same binary/rules. Print tip, height, cumulative work, peer count, hash rate,
and canonical blocks per miner. At a jointly chosen end time, stop mining but
leave nodes connected until tips converge, then record the final standings.

Expected long-run block share is each miner's hash rate divided by total hash
rate; the friends' combined CPU count alone does not predict their share.
Ten-second blocks can produce noticeable propagation races, especially over
the Internet. Report stale blocks separately and use a longer shared block
interval in a fresh network if propagation delays dominate. A group controlling
most hash power can reorganize the chain if they deliberately coordinate an
alternative branch; simply mining independently does not combine their wins
into a single miner or give them special control.
