#include <gtest/gtest.h>

#include <fstream>
#include <string>
#include <vector>

#include "kvstore/crc32.h"
#include "kvstore/wal.h"
#include "test_util.h"

namespace {

using WalTest = kvtest::TempDirTest;

std::vector<kv::WalRecord> read_all(const std::string& path) {
  std::vector<kv::WalRecord> out;
  kv::Wal::replay(path, [&](const kv::WalRecord& r) { out.push_back(r); });
  return out;
}

TEST(Crc32, MatchesKnownValue) {
  // Well-known check value for CRC-32/IEEE.
  const char* s = "123456789";
  EXPECT_EQ(kv::crc32(s, 9), 0xCBF43926u);
}

TEST(Wal, ReplayOfMissingFileIsEmpty) {
  size_t n = kv::Wal::replay("does_not_exist.wal", [](const kv::WalRecord&) {});
  EXPECT_EQ(n, 0u);
}

TEST_F(WalTest, AppendThenReplay) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/log.wal";
  {
    kv::Wal w(path, true);
    ASSERT_TRUE(w.append({kv::Op::kPut, "a", "1"}));
    ASSERT_TRUE(w.append({kv::Op::kDelete, "a", ""}));
  }
  auto recs = read_all(path);
  ASSERT_EQ(recs.size(), 2u);
  EXPECT_EQ(recs[0].op, kv::Op::kPut);
  EXPECT_EQ(recs[0].value, "1");
  EXPECT_EQ(recs[1].op, kv::Op::kDelete);
}

TEST_F(WalTest, TruncatedTailIsIgnored) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/log.wal";
  {
    kv::Wal w(path, false);
    w.append({kv::Op::kPut, "a", "1"});
    w.append({kv::Op::kPut, "b", "2"});
  }
  // Simulate a crash mid-write: append a few garbage bytes (half a record).
  {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    const char partial[] = {0x01, 0x02, 0x03};
    out.write(partial, sizeof(partial));
  }
  auto recs = read_all(path);
  ASSERT_EQ(recs.size(), 2u);
  EXPECT_EQ(recs[1].key, "b");
}

TEST_F(WalTest, CorruptRecordStopsReplay) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/log.wal";
  {
    kv::Wal w(path, false);
    w.append({kv::Op::kPut, "a", "1"});
    w.append({kv::Op::kPut, "b", "2"});
  }
  // Flip one byte inside the LAST record's value so its CRC no longer matches.
  {
    std::fstream f(path, std::ios::binary | std::ios::in | std::ios::out);
    f.seekg(0, std::ios::end);
    const std::streamoff size = f.tellg();
    f.seekp(size - 1);
    f.put('X');
  }
  auto recs = read_all(path);
  ASSERT_EQ(recs.size(), 1u);  // only "a" survives
  EXPECT_EQ(recs[0].key, "a");
}

}  // namespace
