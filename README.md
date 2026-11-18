# kvstore: a key-value store in C++ with an LSM-tree engine and Raft

[![CI](https://github.com/Dhruva0101/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/Dhruva0101/kvstore/actions/workflows/ci.yml)
![C++17](https://img.shields.io/badge/C%2B%2B17-00599C?logo=cplusplus&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Tech:** C++17 · CMake · GoogleTest · epoll/kqueue · Docker · GitHub Actions

A small database built from scratch to learn how storage engines (LevelDB/RocksDB) and replicated systems work.

```
client ──TCP──▶ event loops (epoll / kqueue) ──▶ storage engine
                                                 ├── write-ahead log (WAL, CRC32, fsync)
                                                 ├── memtable (sorted std::map)
                                                 └── SSTables (sorted, immutable, Bloom filters)
                                                      + size-tiered compaction

Raft consensus core (leader election, log replication, commit) tested on a simulated network
```

## What works

- **Write-ahead log:** every write is appended (with a CRC32) and `fsync`ed before it is applied; replay on startup skips torn or corrupt tail records.
- **LSM storage engine:** the memtable flushes to SSTables (checksummed data blocks, sparse index, footer). Reads check the memtable, then SSTables newest to oldest. Deletes are tombstones. A `MANIFEST` is swapped atomically, so a crash mid-flush is recoverable.
- **Bloom filters** per SSTable (~1% false positives) skip disk reads for missing keys.
- **Size-tiered compaction** merges similar-sized adjacent SSTables and drops overwritten values and tombstones.
- **TCP server:** one event loop per core (epoll on Linux, kqueue on macOS), simple text protocol (`PUT`/`GET`/`DEL`/`PING`/`STATS`).
- **`kvbench`** load generator: throughput and p50/p99 latency.
- **Raft core:** leader election, log replication and commitment, persisted term/vote/log. Tested with 1-, 3- and 5-node clusters on a simulated network with message loss and partitions, including the "Figure 8" case from the Raft paper.
- **59 GoogleTest tests**, run in GitHub Actions on every push.

## Build and run

Requires CMake 3.16+ and a C++17 compiler.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure

./build/kvserver --port 7000 --dir data            # start the server
printf 'PUT name dhruva\nGET name\n' | nc localhost 7000
./build/kvbench --port 7000 --threads 8 --ops 10000  # benchmark it
```

Or with Docker: `docker build -t kvstore . && docker run -p 7000:7000 kvstore --port 7000`.

## Benchmark

Measured with `kvbench` (100-byte values, 50% reads), server and client in one Docker container on an Apple-silicon laptop. Server ran with `--no-sync`.

| Clients | Throughput | p50 latency | p99 latency |
|---|---|---|---|
| 1 | ~18k ops/s | 52 µs | 78 µs |
| 8 | ~57k ops/s | 99 µs | 540 µs |
| 32 | ~73k ops/s | 258 µs | 1.9 ms |

In-process (no network), one thread: ~450k ops/s.

## Design notes

- **WAL first, then memtable:** a crash after acknowledging a write cannot lose it; replay rebuilds state.
- **LSM vs B-tree:** LSM turns random writes into sequential ones; the cost is read and space amplification, which Bloom filters and compaction reduce.
- **Why only adjacent tables are compacted:** it keeps "newer table wins" correct without per-key sequence numbers. Tombstones are dropped only when the oldest table is part of the merge.
- **Raft commit rule:** a leader only commits entries from its own term by counting replicas (paper §5.4.2). The Figure 8 test fails if this rule is removed.

## Next steps

- Wire the Raft core to the network and run a 3-node cluster in Docker Compose with a kill-the-leader test.

## License

[MIT](LICENSE)
