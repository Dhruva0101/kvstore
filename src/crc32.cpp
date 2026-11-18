#include "kvstore/crc32.h"

#include <array>

namespace kv {

namespace {

std::array<uint32_t, 256> make_table() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) {
      c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : (c >> 1);
    }
    table[i] = c;
  }
  return table;
}

}  // namespace

uint32_t crc32(const void* data, size_t len, uint32_t seed) {
  static const std::array<uint32_t, 256> table = make_table();
  const auto* bytes = static_cast<const unsigned char*>(data);
  uint32_t c = seed ^ 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    c = table[(c ^ bytes[i]) & 0xFFu] ^ (c >> 8);
  }
  return c ^ 0xFFFFFFFFu;
}

}  // namespace kv
