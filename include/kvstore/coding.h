#pragma once

// Little-endian fixed-width encoding helpers shared by the WAL, SSTables and
// the Raft wire format, so on-disk and on-wire formats do not depend on the
// machine's byte order.

#include <cstdint>
#include <string>

namespace kv {

inline void put_u32(std::string& buf, uint32_t v) {
  for (int i = 0; i < 4; ++i) buf.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

inline void put_u64(std::string& buf, uint64_t v) {
  for (int i = 0; i < 8; ++i) buf.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

inline uint32_t get_u32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

inline uint64_t get_u64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

// Bounds-checked sequential reader over a byte buffer.
class Reader {
 public:
  Reader(const char* data, size_t len) : p_(data), end_(data + len) {}

  bool u8(uint8_t& v) {
    if (remaining() < 1) return false;
    v = static_cast<uint8_t>(*p_++);
    return true;
  }
  bool u32(uint32_t& v) {
    if (remaining() < 4) return false;
    v = get_u32(p_);
    p_ += 4;
    return true;
  }
  bool u64(uint64_t& v) {
    if (remaining() < 8) return false;
    v = get_u64(p_);
    p_ += 8;
    return true;
  }
  bool bytes(size_t n, std::string& out) {
    if (remaining() < n) return false;
    out.assign(p_, n);
    p_ += n;
    return true;
  }
  size_t remaining() const { return static_cast<size_t>(end_ - p_); }

 private:
  const char* p_;
  const char* end_;
};

}  // namespace kv
