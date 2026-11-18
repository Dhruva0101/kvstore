#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "kvstore/sstable.h"
#include "test_util.h"

namespace {

std::vector<kv::KeyEntry> keys(const std::string& prefix, int n) {
  std::vector<kv::KeyEntry> v;
  for (int i = 0; i < n; ++i) v.push_back({prefix + std::to_string(i), {}});
  return v;
}

TEST(BloomFilter, NoFalseNegatives) {
  const auto in = keys("present", 5000);
  const std::string f = kv::build_filter(in, 10);
  for (const auto& e : in) ASSERT_TRUE(kv::filter_may_contain(f, e.key)) << e.key;
}

TEST(BloomFilter, FalsePositiveRateAroundOnePercent) {
  const std::string f = kv::build_filter(keys("present", 10000), 10);
  int fp = 0;
  const int probes = 100000;
  for (int i = 0; i < probes; ++i) {
    if (kv::filter_may_contain(f, "absent" + std::to_string(i))) ++fp;
  }
  const double rate = static_cast<double>(fp) / probes;
  EXPECT_LT(rate, 0.02) << "false-positive rate " << rate;
}

TEST(BloomFilter, DisabledFilterAcceptsEverything) {
  const std::string f = kv::build_filter(keys("k", 100), 0);
  EXPECT_TRUE(f.empty());
  EXPECT_TRUE(kv::filter_may_contain(f, "anything"));
}

using FilterSstTest = kvtest::TempDirTest;

TEST_F(FilterSstTest, SstableSkipsBlockReadsForAbsentKeys) {
  std::filesystem::create_directories(dir_);
  const std::string path = dir_ + "/t.sst";
  std::vector<kv::KeyEntry> entries;
  for (int i = 0; i < 1000; i += 2) {  // even keys only
    char k[16];
    std::snprintf(k, sizeof(k), "key%05d", i);
    entries.push_back({k, {kv::EntryType::kValue, "v"}});
  }
  kv::write_sstable(path, entries);
  auto t = kv::SsTable::open(path);
  for (int i = 1; i < 1000; i += 2) {  // odd keys: inside the key range, never present
    char k[16];
    std::snprintf(k, sizeof(k), "key%05d", i);
    EXPECT_FALSE(t->get(k).has_value());
  }
  EXPECT_GT(t->filter_skips(), 480u);  // ~99% of 500 lookups
}

}  // namespace
