// Bloom filter for SSTables.
//
// A Bloom filter answers "is this key possibly in the table?" with no false
// negatives and a tunable false-positive rate. With 10 bits per key and k = 7
// hash functions the false-positive rate is about 1%, so ~99% of lookups for
// keys that are not in a table skip the disk read entirely.
//
// Encoding: [bit array bytes][k u8]. Hashes use double hashing
// (h_i = h1 + i * h2), which behaves like k independent hashes
// (Kirsch & Mitzenmacher, 2006).
#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "kvstore/sstable.h"

namespace kv {

namespace {

// 64-bit FNV-1a followed by the splitmix64 finalizer for good bit mixing.
uint64_t hash64(const std::string& s) {
  uint64_t h = 1469598103934665603ull;
  for (unsigned char c : s) {
    h ^= c;
    h *= 1099511628211ull;
  }
  h ^= h >> 30;
  h *= 0xbf58476d1ce4e5b9ull;
  h ^= h >> 27;
  h *= 0x94d049bb133111ebull;
  h ^= h >> 31;
  return h;
}

}  // namespace

std::string build_filter(const std::vector<KeyEntry>& sorted, int bits_per_key) {
  if (bits_per_key <= 0 || sorted.empty()) return {};
  // Optimal k = bits_per_key * ln(2).
  const int k = std::clamp(static_cast<int>(bits_per_key * 0.69), 1, 30);
  const size_t bits = std::max<size_t>(64, sorted.size() * static_cast<size_t>(bits_per_key));
  const size_t bytes = (bits + 7) / 8;
  const size_t nbits = bytes * 8;

  std::string filter(bytes, '\0');
  for (const auto& e : sorted) {
    const uint64_t h = hash64(e.key);
    const uint32_t h1 = static_cast<uint32_t>(h);
    const uint32_t h2 = static_cast<uint32_t>(h >> 32) | 1;
    for (int i = 0; i < k; ++i) {
      const size_t bit = (h1 + static_cast<uint64_t>(i) * h2) % nbits;
      filter[bit / 8] = static_cast<char>(filter[bit / 8] | (1 << (bit % 8)));
    }
  }
  filter.push_back(static_cast<char>(k));
  return filter;
}

bool filter_may_contain(const std::string& filter, const std::string& key) {
  if (filter.size() < 2) return true;  // no usable filter
  const int k = static_cast<unsigned char>(filter.back());
  const size_t nbits = (filter.size() - 1) * 8;
  const uint64_t h = hash64(key);
  const uint32_t h1 = static_cast<uint32_t>(h);
  const uint32_t h2 = static_cast<uint32_t>(h >> 32) | 1;
  for (int i = 0; i < k; ++i) {
    const size_t bit = (h1 + static_cast<uint64_t>(i) * h2) % nbits;
    if (!(filter[bit / 8] & (1 << (bit % 8)))) return false;
  }
  return true;
}

}  // namespace kv
