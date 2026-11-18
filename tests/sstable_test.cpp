#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "kvstore/sstable.h"
#include "test_util.h"

namespace {

using SstableTest = kvtest::TempDirTest;

std::vector<kv::KeyEntry> make_entries(int n) {
  std::vector<kv::KeyEntry> v;
  for (int i = 0; i < n; ++i) {
    char key[16];
    std::snprintf(key, sizeof(key), "key%05d", i);
    kv::KeyEntry e;
    e.key = key;
    e.entry.value = "value" + std::to_string(i);
    if (i % 10 == 3) {
      e.entry.type = kv::EntryType::kTombstone;
      e.entry.value.clear();
    }
    v.push_back(std::move(e));
  }
  return v;
}

TEST_F(SstableTest, WriteThenPointLookups) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/t.sst";
  kv::SstWriteOptions o;
  o.block_size = 128;  // many blocks
  kv::write_sstable(path, make_entries(1000), o);

  auto t = kv::SsTable::open(path);
  EXPECT_EQ(t->entry_count(), 1000u);
  EXPECT_EQ(t->smallest_key(), "key00000");
  EXPECT_EQ(t->largest_key(), "key00999");
  EXPECT_EQ(t->get("key00500")->value, "value500");
  EXPECT_TRUE(t->get("key00013")->deleted());
  EXPECT_FALSE(t->get("key01000").has_value());
  EXPECT_FALSE(t->get("aaa").has_value());
  EXPECT_FALSE(t->get("key00500x").has_value());
}

TEST_F(SstableTest, IteratorVisitsAllEntriesInOrder) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/t.sst";
  kv::SstWriteOptions o;
  o.block_size = 100;
  const auto entries = make_entries(257);
  kv::write_sstable(path, entries, o);
  auto t = kv::SsTable::open(path);
  size_t i = 0;
  for (auto it = t->iter(); it.valid(); it.next(), ++i) {
    ASSERT_LT(i, entries.size());
    EXPECT_EQ(it.current().key, entries[i].key);
    EXPECT_EQ(it.current().entry.value, entries[i].entry.value);
  }
  EXPECT_EQ(i, entries.size());
}

TEST_F(SstableTest, EmptyTable) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/t.sst";
  kv::write_sstable(path, {});
  auto t = kv::SsTable::open(path);
  EXPECT_EQ(t->entry_count(), 0u);
  EXPECT_FALSE(t->get("x").has_value());
  EXPECT_FALSE(t->iter().valid());
}

TEST_F(SstableTest, CorruptBlockIsDetected) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/t.sst";
  kv::write_sstable(path, make_entries(50));
  {
    std::fstream f(path, std::ios::binary | std::ios::in | std::ios::out);
    f.seekp(10);
    f.put('\x7f');
  }
  // Block 0 is read at open() (to learn the smallest key) or on lookup; either
  // way the checksum mismatch must surface as an error, never as bad data.
  EXPECT_THROW(
      {
        auto t = kv::SsTable::open(path);
        t->get("key00001");
      },
      std::runtime_error);
}

TEST_F(SstableTest, BadMagicIsRejected) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/t.sst";
  { std::ofstream(path) << std::string(100, 'x'); }
  EXPECT_THROW(kv::SsTable::open(path), std::runtime_error);
}

}  // namespace
