# blockchain-go

A small experimental proof-of-work blockchain for competing with friends.
Every node validates blocks and mines independently. Nodes relay blocks over
WebSockets and follow the valid branch with the most accumulated work.

Requires Go 1.26.5 or later. This version starts a **new chain** with a fixed
genesis and a new block format; it does not import the original demo's blocks.
Only coinbase rewards are supported: one experimental unit per block, assigned
to a miner display label. Labels are not authenticated wallets; there are no
ordinary transactions, signatures, or spendable balances yet.

## Build and run

```sh
go mod download
go build -o bin/blockchain-go .
./bin/blockchain-go --miner alice --workers 4
```

No shared peer token is required. Anyone who can reach the listener or tunnel
can connect through `/p2p` and read `/status`. Every received block is still
validated against the same consensus rules. Cloudflare Access can optionally
restrict who reaches the tunnel.

Useful flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `127.0.0.1:7331` | HTTP origin listener |
| `--peer` | none | Repeatable `wss://hostname/p2p` URL |
| `--miner` | `miner` | Display label, 1–32 UTF-8 bytes |
| `--workers` | logical CPU count | Parallel nonce searches; use fewer for other work |
| `--mine` | `true` | Set `false` for a validating relay |
| `--data-dir` | `./blockchain-data` | Exclusive directory for this node's block log |
| `--initial-bits` | `22` | Initial difficulty; must match across the network |

For a second node on your machine, use a different listener and data directory:

```sh
./bin/blockchain-go --listen 127.0.0.1:7332 --data-dir /tmp/bob-chain \
  --peer ws://127.0.0.1:7331/p2p --miner bob --workers 2
```

Only loopback peers may use unencrypted `ws://`. Remote peers require `wss://`
with ordinary certificate verification. Outbound connections are bidirectional:
joining friends can announce their blocks without an inbound port or tunnel.

## Cloudflare Tunnel

On the host, configure a named Cloudflare Tunnel with a public hostname such as
`chain.example.com` pointing to the local HTTP origin. A typical `cloudflared`
configuration is:

```yaml
tunnel: YOUR-TUNNEL-UUID
credentials-file: /path/to/YOUR-TUNNEL-UUID.json
ingress:
  - hostname: chain.example.com
    service: http://127.0.0.1:7331
  - service: http_status:404
```

Create the hostname's DNS route with Cloudflare, ensure WebSockets are enabled
for the zone, then run the named tunnel with `cloudflared tunnel run <name>`.
Keep tunnel credentials on the host, outside this repository. The node itself
continues listening on localhost; Cloudflare provides public HTTPS and forwards
WebSocket upgrades to `/p2p`.

Friends use the same network difficulty:

```sh
./bin/blockchain-go --peer wss://chain.example.com/p2p \
  --miner bob --workers 4 --data-dir ./blockchain-data
```

Only the host needs `cloudflared`. If Cloudflare Access protects the hostname,
create an appropriate Service Auth policy and set `CF_ACCESS_CLIENT_ID` and
`CF_ACCESS_CLIENT_SECRET` on dialing nodes. Both must be set together. They are
sent as Cloudflare Access service-token headers on outbound `wss://` connections;
no application bearer token is required. Configured service
credentials apply to all configured WSS peers, so only configure trusted peer
hostnames. Browser-login redirects are not supported; redirects are rejected.

Connections send keepalives and reconnect with backoff. Every reconnect runs
chain synchronization to recover missed blocks. If all friends connect only to
the host, a host outage partitions them: they may mine competing branches until
it returns. You can add a second reachable node using another tunnel and another
`--peer` argument. The host relays blocks but has no consensus authority.

An actual Cloudflare Tunnel rehearsal is still required on your hostname.
Automated tests exercise local WebSockets and a TLS reverse proxy instead.

## Observe and stop

Every five seconds the node logs height, tip hash, peer count, recent hashes per
second, canonical wins by miner, and stale-block count. The `/status`
endpoint returns the same data plus chain ID and cumulative work:

```sh
curl --fail http://127.0.0.1:7331/status
```

Stop with Ctrl+C. Blocks are flushed before being accepted and indexes are rebuilt
from the block log at restart. The data directory is locked against simultaneous
writers. An incomplete final record is removed after a crash; checksum failures
or invalid complete records stop startup rather than silently discarding data.
A storage failure stops the running node. Back up the data directory while stopped.

The network targets ten-second blocks and adjusts every 60 blocks. Initial bits
22 is a starting guess, not a calibrated speed guarantee. Use the reported hash
rate to estimate initial bits: approximately `log2(total hashes/second * 10)`.
To change initial difficulty, everyone must agree, stop, and use new data
folders; the setting is part of the chain identity. Difficulty adjustments are
bounded to a factor of four. The first adjustment measures heights 1 through 59 (58 time intervals),
excluding the historical genesis timestamp; subsequent windows use 60 intervals.
Keep machine clocks synchronized. Extremely easy settings can outrun the allowed
timestamp window and temporarily pause mining until wall time catches up.

Winning share depends on hash rate and luck, not CPU count alone. Compare
canonical wins after everyone stops mining and remaining relay nodes synchronize.
Equal-work forks retain the current local tip until another block resolves the
tie; short races have no absolute finality. Stale blocks do not count as wins.

## Validation

```sh
go test -race ./...
go vet ./...
go build -o /tmp/blockchain-go .
python3 scripts/smoke.py --binary /tmp/blockchain-go
```

Tests cover consensus/encoding vectors, difficulty boundaries, invalid blocks,
fork choice (including a shorter harder chain), replay/corruption, open peer connections,
message limits, paginated sync, relaying, reconnects, miner cancellation, and TLS
proxy upgrades. The separate-process smoke test exercises real mining, a late
joiner, a host outage, isolated forks, restarts, and final convergence.

This is an experimental chain. Miner labels are self-reported, and peers can
consume storage with valid side branches. There is no disk pruning, automatic peer discovery, or mining pool.
