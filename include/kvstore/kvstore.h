#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "kvstore/sstable.h"
#include "kvstore/wal.h"

namespace kv {

struct Options {
  // fsync the WAL after every write (durable, slower). Off = faster, may lose
  // the last few writes on power loss.
  bool sync_every_write = true;
  // Flush the memtable to a new SSTable once it holds about this many bytes.
  size_t memtable_max_bytes = 4u << 20;
  size_t block_size = 4096;
  int bloom_bits_per_key = 10;
  // Size-tiered compaction: merge a run of at least this many adjacent
  // SSTables whose sizes are all within [low, high] x the run's average size.
  // 0 disables automatic compaction.
  size_t compaction_min_tables = 4;
  double compaction_bucket_low = 0.5;
  double compaction_bucket_high = 1.5;
};

struct Stats {
  uint64_t flushes = 0;
  uint64_t compactions = 0;
  uint64_t sstables = 0;
  uint64_t sstable_bytes = 0;
  uint64_t filter_skips = 0;
  uint64_t memtable_bytes = 0;
};

// LSM-tree key-value store.
//
// Writes: append to the WAL, then insert into the in-memory memtable (a sorted
// std::map). When the memtable is full it is written out as an immutable
// SSTable and a fresh WAL is started.
// Reads: memtable first, then SSTables from newest to oldest; the first hit
// wins, and a tombstone means "deleted".
//
// The directory holds NNNNNN.wal, NNNNNN.sst and a MANIFEST that lists the live
// SSTables (oldest first) and the active WAL. The MANIFEST is replaced
// atomically (write temp file, fsync, rename), so a crash at any point leaves
// either the old or the new set of files, never a mix.
//
// Thread-safe: every public method takes an internal mutex.
class KvStore {
 public:
  explicit KvStore(std::string dir, Options opts = {});
  ~KvStore();

  KvStore(const KvStore&) = delete;
  KvStore& operator=(const KvStore&) = delete;

  bool put(const std::string& key, const std::string& value);
  std::optional<std::string> get(const std::string& key) const;
  bool del(const std::string& key);  // returns false if the key did not exist

  // Visits every live key in sorted order (merges memtable and SSTables).
  void scan(const std::function<void(const std::string&, const std::string&)>& fn) const;
  // Number of live keys. O(total entries): it does a full scan.
  size_t size() const;

  // Writes the memtable to an SSTable now, even if it is not full.
  bool flush();
  // Merges every SSTable into one, dropping overwritten values and tombstones.
  void compact_all();

  size_t recovered_records() const { return recovered_; }
  size_t sstable_count() const;
  Stats stats() const;
  const std::string& dir() const { return dir_; }

 private:
  bool put_locked(const std::string& key, Entry e);
  std::optional<Entry> lookup_locked(const std::string& key) const;
  void flush_locked();
  void maybe_compact_locked();
  void compact_range_locked(size_t first, size_t last);
  void write_manifest_locked();
  void load_manifest_locked();
  void remove_unreferenced_files_locked();
  void merge_all_locked(const std::function<void(const KeyEntry&)>& fn,
                        size_t first_table, size_t last_table, bool include_memtable) const;
  std::string file_path(uint64_t num, const char* ext) const;
  uint64_t new_file_number() { return next_file_++; }

  std::string dir_;
  Options opts_;
  mutable std::mutex mu_;

  std::map<std::string, Entry> memtable_;
  size_t memtable_bytes_ = 0;
  std::unique_ptr<Wal> wal_;
  uint64_t wal_number_ = 0;

  struct Table {
    uint64_t number;
    std::shared_ptr<SsTable> sst;
  };
  std::vector<Table> tables_;  // oldest first
  uint64_t next_file_ = 1;

  size_t recovered_ = 0;
  Stats stats_;
};

}  // namespace kv
