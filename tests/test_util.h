#pragma once

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace kvtest {

// A fresh, empty directory per test, deleted afterwards.
class TempDirTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    dir_ = (std::filesystem::temp_directory_path() /
            (std::string("kvtest_") + info->test_suite_name() + "_" + info->name()))
               .string();
    std::filesystem::remove_all(dir_);
  }
  void TearDown() override { std::filesystem::remove_all(dir_); }
  std::string dir_;
};

}  // namespace kvtest
