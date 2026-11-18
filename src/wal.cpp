#include "kvstore/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <vector>

#include "kvstore/coding.h"
#include "kvstore/crc32.h"

namespace kv {

namespace {

constexpr size_t kHeaderAfterCrc = 1 + 4 + 4;  // op + key_len + val_len

// write() may write fewer bytes than asked (signals, pipes); loop until done.
bool write_all(int fd, const char* p, size_t n) {
  while (n > 0) {
    const ssize_t w = ::write(fd, p, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

}  // namespace

Wal::Wal(std::string path, bool sync_every_write)
    : path_(std::move(path)), sync_every_write_(sync_every_write) {
  fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) {
    throw std::runtime_error("cannot open WAL " + path_ + ": " + std::strerror(errno));
  }
}

Wal::~Wal() {
  if (fd_ >= 0) {
    ::fsync(fd_);
    ::close(fd_);
  }
}

bool Wal::append(const WalRecord& rec) {
  std::string body;
  body.reserve(kHeaderAfterCrc + rec.key.size() + rec.value.size());
  body.push_back(static_cast<char>(rec.op));
  put_u32(body, static_cast<uint32_t>(rec.key.size()));
  put_u32(body, static_cast<uint32_t>(rec.value.size()));
  body += rec.key;
  body += rec.value;

  // One write() per record so a crash leaves at most one partial record at the
  // tail, which replay() detects by length or checksum.
  std::string record;
  record.reserve(4 + body.size());
  put_u32(record, crc32(body.data(), body.size()));
  record += body;

  if (!write_all(fd_, record.data(), record.size())) return false;
  bytes_written_ += record.size();
  if (sync_every_write_) return sync();
  return true;
}

bool Wal::sync() { return ::fsync(fd_) == 0; }

size_t Wal::replay(const std::string& path,
                   const std::function<void(const WalRecord&)>& apply) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return 0;  // no log yet: fresh store
  std::vector<char> data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());

  size_t pos = 0;
  size_t count = 0;
  while (pos + 4 + kHeaderAfterCrc <= data.size()) {
    const uint32_t stored_crc = get_u32(&data[pos]);
    const char* body = &data[pos + 4];
    const auto op = static_cast<uint8_t>(body[0]);
    const uint64_t key_len = get_u32(body + 1);
    const uint64_t val_len = get_u32(body + 5);
    const uint64_t body_len = kHeaderAfterCrc + key_len + val_len;

    if (pos + 4 + body_len > data.size()) break;     // truncated tail
    if (crc32(body, body_len) != stored_crc) break;  // corrupt record
    if (op != static_cast<uint8_t>(Op::kPut) && op != static_cast<uint8_t>(Op::kDelete)) break;

    WalRecord rec;
    rec.op = static_cast<Op>(op);
    rec.key.assign(body + kHeaderAfterCrc, key_len);
    rec.value.assign(body + kHeaderAfterCrc + key_len, val_len);
    apply(rec);

    pos += 4 + body_len;
    ++count;
  }
  return count;
}

}  // namespace kv
