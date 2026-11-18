#pragma once

#include <cstddef>
#include <cstdint>

namespace kv {

// Standard CRC-32 (IEEE 802.3, polynomial 0xEDB88320), same as zlib's crc32().
uint32_t crc32(const void* data, size_t len, uint32_t seed = 0);

}  // namespace kv
