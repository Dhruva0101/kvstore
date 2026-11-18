#pragma once

// Sorted String Table: an immutable, sorted, on-disk file of key/value entries.
//
// File layout:
//   [data block 0][crc32] ... [data block N-1][crc32]
//   [filter block][crc32]            (Bloom filter over all keys; may be empty)
//   [index block][crc32]             (one entry per data block)
//   [footer, 40 bytes]
//
// Data block entry:  [type u8][key_len u32][val_len u32][key][value]
//                    type 1 = value, 2 = tombstone (deleted key)
// Index entry:       [key_len u32][last key in block][offset u64][size u32]
// Footer:            [index_off u64][index_len u32][filter_off u64][filter_len u32]
//                    [entry_count u64][magic u64]
//
// A lookup binary-searches the in-memory index for the first block whose last
// key is >= the target, then reads and scans just that block.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace kv {

enum class EntryType : uint8_t { kValue = 1, kTombstone = 2 };

struct Entry {
  EntryType type = EntryType::kValue;
  std::string value;
  bool deleted() const { return type == EntryType::kTombstone; }
};

struct KeyEntry {
  std::string key;
  Entry entry;
};

constexpr uint64_t kSstMagic = 0x4b5653535441424cull;  // "KVSSTABL"

struct SstWriteOptions {
  size_t block_size = 4096;
  int bloom_bits_per_key = 10;  // 0 disables the filter
};

// Writes a complete SSTable from entries that are already sorted by key with no
// duplicates. The file is fsync()ed before returning. Throws on I/O errors.
void write_sstable(const std::string& path, const std::vector<KeyEntry>& sorted,
                   const SstWriteOptions& opts = {});

// Bloom filter over the keys of one SSTable (filter.cpp). An empty string means
// "no filter"; filter_may_contain() then always returns true.
std::string build_filter(const std::vector<KeyEntry>& sorted, int bits_per_key);
bool filter_may_contain(const std::string& filter, const std::string& key);

class SsTable {
 public:
  // Opens the file, validates the footer and loads the index and filter into
  // memory. Throws std::runtime_error if the file is missing or corrupt.
  static std::shared_ptr<SsTable> open(const std::string& path);
  ~SsTable();

  SsTable(const SsTable&) = delete;
  SsTable& operator=(const SsTable&) = delete;

  // Point lookup. Returns the entry (which may be a tombstone) if this table
  // has the key, nullopt otherwise.
  std::optional<Entry> get(const std::string& key) const;

  // Sequential iterator over every entry in key order.
  class Iterator {
   public:
    explicit Iterator(const SsTable* t);
    bool valid() const { return valid_; }
    const KeyEntry& current() const { return cur_; }
    void next();

   private:
    void load_block(size_t b);
    const SsTable* t_;
    size_t block_ = 0;
    std::vector<KeyEntry> entries_;
    size_t pos_ = 0;
    KeyEntry cur_;
    bool valid_ = false;
  };
  Iterator iter() const { return Iterator(this); }

  const std::string& path() const { return path_; }
  uint64_t file_size() const { return file_size_; }
  uint64_t entry_count() const { return entry_count_; }
  const std::string& smallest_key() const { return smallest_; }
  const std::string& largest_key() const { return largest_; }

  // Counts lookups that the Bloom filter answered without reading a block.
  uint64_t filter_skips() const { return filter_skips_; }

 private:
  struct IndexEntry {
    std::string last_key;
    uint64_t offset;
    uint32_t size;
  };

  SsTable() = default;
  std::string read_checked(uint64_t off, uint32_t len) const;
  std::vector<KeyEntry> read_block(size_t b) const;
  bool may_contain(const std::string& key) const;

  std::string path_;
  int fd_ = -1;
  uint64_t file_size_ = 0;
  uint64_t entry_count_ = 0;
  std::vector<IndexEntry> index_;
  std::string filter_;
  std::string smallest_, largest_;
  mutable uint64_t filter_skips_ = 0;
};

}  // namespace kv
