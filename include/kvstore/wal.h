#pragma once

#include <cstdint>
#include <functional>
#include <string>

namespace kv {

enum class Op : uint8_t { kPut = 1, kDelete = 2 };

struct WalRecord {
  Op op;
  std::string key;
  std::string value;  // empty for deletes
};

// Append-only write-ahead log.
// Record layout: [crc32 u32][op u8][key_len u32][val_len u32][key][value]
// The CRC covers every byte after the CRC field.
class Wal {
 public:
  // Opens (or creates) the log for appending. Throws std::runtime_error if the
  // file cannot be opened.
  Wal(std::string path, bool sync_every_write);
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  // Appends one record. With sync_every_write, the record is fsync()ed to
  // stable storage before this returns. Returns false if the write failed.
  bool append(const WalRecord& rec);

  // Forces everything appended so far to stable storage.
  bool sync();

  // Reads every valid record from the start of the file, in order.
  // Stops at the first truncated or corrupt record (e.g. a crash mid-write).
  // Returns the number of records replayed.
  static size_t replay(const std::string& path,
                       const std::function<void(const WalRecord&)>& apply);

  const std::string& path() const { return path_; }
  uint64_t bytes_written() const { return bytes_written_; }

 private:
  std::string path_;
  bool sync_every_write_;
  int fd_ = -1;
  uint64_t bytes_written_ = 0;
};

}  // namespace kv
