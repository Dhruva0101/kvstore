#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <string>

#include "kvstore/kvstore.h"
#include "test_util.h"

namespace {

using KvStoreTest = kvtest::TempDirTest;

kv::Options small_memtable() {
  kv::Options o;
  o.sync_every_write = false;
  o.memtable_max_bytes = 4096;  // flush often so tests exercise SSTables
  o.block_size = 256;
  return o;
}

TEST_F(KvStoreTest, PutThenGet) {
  kv::KvStore s(dir_);
  ASSERT_TRUE(s.put("a", "1"));
  EXPECT_EQ(s.get("a").value(), "1");
}

TEST_F(KvStoreTest, MissingKeyReturnsNullopt) {
  kv::KvStore s(dir_);
  EXPECT_FALSE(s.get("nope").has_value());
}

TEST_F(KvStoreTest, OverwriteKeepsLatestValue) {
  kv::KvStore s(dir_);
  s.put("a", "1");
  s.put("a", "2");
  EXPECT_EQ(s.get("a").value(), "2");
  EXPECT_EQ(s.size(), 1u);
}

TEST_F(KvStoreTest, DeleteRemovesKey) {
  kv::KvStore s(dir_);
  s.put("a", "1");
  EXPECT_TRUE(s.del("a"));
  EXPECT_FALSE(s.get("a").has_value());
  EXPECT_FALSE(s.del("a"));  // already gone
}

TEST_F(KvStoreTest, ValuesWithSpacesAndEmptyValues) {
  kv::KvStore s(dir_);
  s.put("greeting", "hello world");
  s.put("empty", "");
  EXPECT_EQ(s.get("greeting").value(), "hello world");
  EXPECT_EQ(s.get("empty").value(), "");
}

TEST_F(KvStoreTest, RecoversStateAfterRestart) {
  {
    kv::KvStore s(dir_);
    s.put("a", "1");
    s.put("b", "2");
    s.put("a", "3");
    s.del("b");
  }  // store destroyed: simulates the process exiting
  kv::KvStore reopened(dir_);
  EXPECT_EQ(reopened.recovered_records(), 4u);
  EXPECT_EQ(reopened.get("a").value(), "3");
  EXPECT_FALSE(reopened.get("b").has_value());
  EXPECT_EQ(reopened.size(), 1u);
}

// ---- Step 1: memtable flush to SSTables ----

TEST_F(KvStoreTest, ExplicitFlushMovesDataToSstable) {
  kv::KvStore s(dir_);
  s.put("a", "1");
  s.put("b", "2");
  ASSERT_TRUE(s.flush());
  EXPECT_EQ(s.sstable_count(), 1u);
  EXPECT_EQ(s.get("a").value(), "1");
  EXPECT_EQ(s.get("b").value(), "2");
}

TEST_F(KvStoreTest, MemtableFlushesAutomaticallyWhenFull) {
  kv::KvStore s(dir_, small_memtable());
  for (int i = 0; i < 500; ++i) s.put("key" + std::to_string(i), std::string(20, 'v'));
  EXPECT_GT(s.sstable_count(), 1u);
  for (int i = 0; i < 500; ++i) {
    ASSERT_TRUE(s.get("key" + std::to_string(i)).has_value()) << i;
  }
}

TEST_F(KvStoreTest, NewerSstableShadowsOlder) {
  kv::KvStore s(dir_);
  s.put("k", "old");
  s.flush();
  s.put("k", "new");
  s.flush();
  EXPECT_EQ(s.sstable_count(), 2u);
  EXPECT_EQ(s.get("k").value(), "new");
}

TEST_F(KvStoreTest, TombstoneInNewerSstableHidesOlderValue) {
  kv::KvStore s(dir_);
  s.put("k", "v");
  s.flush();
  EXPECT_TRUE(s.del("k"));
  s.flush();
  EXPECT_FALSE(s.get("k").has_value());
  EXPECT_EQ(s.size(), 0u);
}

TEST_F(KvStoreTest, SstablesSurviveRestart) {
  {
    kv::KvStore s(dir_, small_memtable());
    for (int i = 0; i < 300; ++i) s.put("key" + std::to_string(i), "v" + std::to_string(i));
    s.del("key7");
  }
  kv::KvStore s(dir_, small_memtable());
  EXPECT_GT(s.sstable_count(), 0u);
  EXPECT_EQ(s.get("key42").value(), "v42");
  EXPECT_FALSE(s.get("key7").has_value());
  EXPECT_EQ(s.size(), 299u);
}

TEST_F(KvStoreTest, ScanReturnsLiveKeysInOrder) {
  kv::KvStore s(dir_);
  s.put("c", "3");
  s.put("a", "1");
  s.flush();
  s.put("b", "2");
  s.del("c");
  std::string seen;
  s.scan([&](const std::string& k, const std::string& v) { seen += k + "=" + v + ";"; });
  EXPECT_EQ(seen, "a=1;b=2;");
}

TEST_F(KvStoreTest, OrphanFilesFromCrashedFlushAreRemoved) {
  { kv::KvStore s(dir_); s.put("a", "1"); }
  // A crash after writing an SSTable but before the MANIFEST update leaves an
  // unreferenced file behind.
  const auto orphan = std::filesystem::path(dir_) / "999999.sst";
  { std::ofstream(orphan.string()) << "garbage"; }
  kv::KvStore s(dir_);
  EXPECT_FALSE(std::filesystem::exists(orphan));
  EXPECT_EQ(s.get("a").value(), "1");
}

// Random operations checked against a std::map model, across restarts.
TEST_F(KvStoreTest, RandomizedAgainstModel) {
  std::mt19937 rng(42);
  std::map<std::string, std::string> model;
  for (int round = 0; round < 3; ++round) {
    kv::KvStore s(dir_, small_memtable());
    for (int i = 0; i < 3000; ++i) {
      const std::string key = "k" + std::to_string(rng() % 400);
      if (rng() % 4 == 0) {
        EXPECT_EQ(s.del(key), model.erase(key) == 1);
      } else {
        const std::string val = "v" + std::to_string(rng());
        s.put(key, val);
        model[key] = val;
      }
    }
    for (int k = 0; k < 400; ++k) {
      const std::string key = "k" + std::to_string(k);
      auto got = s.get(key);
      auto it = model.find(key);
      if (it == model.end()) {
        EXPECT_FALSE(got.has_value()) << key;
      } else {
        ASSERT_TRUE(got.has_value()) << key;
        EXPECT_EQ(*got, it->second);
      }
    }
    EXPECT_EQ(s.size(), model.size());
  }
}

}  // namespace

// ---- Step 2: compaction ----

namespace {

TEST_F(KvStoreTest, SizeTieredCompactionBoundsTableCount) {
  kv::Options o = small_memtable();
  o.compaction_min_tables = 4;
  kv::KvStore s(dir_, o);
  for (int i = 0; i < 5000; ++i) s.put("key" + std::to_string(i), std::string(30, 'x'));
  // Without compaction this would be ~60 tables.
  EXPECT_LT(s.sstable_count(), 12u);
  EXPECT_GT(s.stats().compactions, 0u);
  for (int i = 0; i < 5000; i += 97) EXPECT_TRUE(s.get("key" + std::to_string(i)).has_value());
}

TEST_F(KvStoreTest, CompactionKeepsNewestValue) {
  kv::Options o = small_memtable();
  o.compaction_min_tables = 0;  // manual only
  kv::KvStore s(dir_, o);
  for (int v = 0; v < 5; ++v) {
    for (int i = 0; i < 50; ++i) s.put("k" + std::to_string(i), "v" + std::to_string(v));
    s.flush();
  }
  s.compact_all();
  EXPECT_EQ(s.sstable_count(), 1u);
  for (int i = 0; i < 50; ++i) EXPECT_EQ(s.get("k" + std::to_string(i)).value(), "v4");
}

TEST_F(KvStoreTest, FullCompactionDropsTombstonesAndOverwrites) {
  kv::Options o;
  o.sync_every_write = false;
  o.compaction_min_tables = 0;
  kv::KvStore s(dir_, o);
  for (int i = 0; i < 100; ++i) s.put("k" + std::to_string(i), std::string(100, 'a'));
  s.flush();
  for (int i = 0; i < 100; ++i) s.put("k" + std::to_string(i), std::string(100, 'b'));
  s.flush();
  for (int i = 0; i < 90; ++i) s.del("k" + std::to_string(i));
  s.flush();
  const auto before = s.stats().sstable_bytes;
  s.compact_all();
  const auto after = s.stats();
  EXPECT_EQ(after.sstables, 1u);
  EXPECT_LT(after.sstable_bytes, before / 10);  // only 10 live keys remain
  EXPECT_EQ(s.size(), 10u);
  EXPECT_FALSE(s.get("k5").has_value());
  EXPECT_EQ(s.get("k95").value(), std::string(100, 'b'));
}

TEST_F(KvStoreTest, PartialCompactionKeepsTombstonesThatHideOlderTables) {
  kv::Options o;
  o.sync_every_write = false;
  o.compaction_min_tables = 0;
  {
    kv::KvStore s(dir_, o);
    s.put("victim", "old-value");
    s.put("pad", std::string(4000, 'p'));  // makes table 1 much bigger
    s.flush();                             // table 1 (oldest)
  }
  // Four small tables after it, the first holding a tombstone for "victim".
  o.compaction_min_tables = 4;
  kv::KvStore s(dir_, o);
  s.del("victim");
  s.flush();
  for (int i = 0; i < 3; ++i) {
    s.put("x" + std::to_string(i), "y");
    s.flush();
  }
  // The four small tables were merged (the big oldest one was not), so the
  // tombstone had to survive the merge to keep hiding "old-value".
  EXPECT_EQ(s.sstable_count(), 2u);
  EXPECT_FALSE(s.get("victim").has_value());
}

TEST_F(KvStoreTest, DataSurvivesRestartAfterCompaction) {
  kv::Options o = small_memtable();
  {
    kv::KvStore s(dir_, o);
    for (int i = 0; i < 3000; ++i) s.put("key" + std::to_string(i), "v" + std::to_string(i));
    for (int i = 0; i < 3000; i += 3) s.del("key" + std::to_string(i));
  }
  kv::KvStore s(dir_, o);
  EXPECT_EQ(s.size(), 2000u);
  EXPECT_FALSE(s.get("key300").has_value());
  EXPECT_EQ(s.get("key301").value(), "v301");
}

}  // namespace
