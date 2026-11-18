#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "kvstore/client.h"
#include "kvstore/kvstore.h"
#include "kvstore/protocol.h"
#include "kvstore/server.h"
#include "test_util.h"

namespace {

TEST(Protocol, ParsesCommandKeyAndValueWithSpaces) {
  kv::Request r;
  ASSERT_TRUE(kv::parse_request("put greeting hello big world", r));
  EXPECT_EQ(r.cmd, "PUT");
  EXPECT_EQ(r.key, "greeting");
  EXPECT_EQ(r.value, "hello big world");
  ASSERT_TRUE(kv::parse_request("GET k", r));
  EXPECT_EQ(r.value, "");
  EXPECT_FALSE(kv::parse_request("   ", r));
}

class ServerTest : public kvtest::TempDirTest {
 protected:
  void SetUp() override {
    TempDirTest::SetUp();
    kv::Options o;
    o.sync_every_write = false;
    store_ = std::make_unique<kv::KvStore>(dir_, o);
    server_ = std::make_unique<kv::Server>(0, 4, [this](const std::string& line) {
      return kv::handle_request(*store_, line);
    });
    server_->start();
  }
  void TearDown() override {
    server_.reset();
    store_.reset();
    TempDirTest::TearDown();
  }
  kv::Client connect() {
    kv::Client c;
    EXPECT_TRUE(c.connect("127.0.0.1", server_->port()));
    return c;
  }
  std::unique_ptr<kv::KvStore> store_;
  std::unique_ptr<kv::Server> server_;
};

TEST_F(ServerTest, BasicCommands) {
  auto c = connect();
  EXPECT_EQ(c.request("PING").value(), "PONG");
  EXPECT_EQ(c.request("PUT name dhruva k").value(), "OK");
  EXPECT_EQ(c.request("GET name").value(), "VALUE dhruva k");
  EXPECT_EQ(c.request("DEL name").value(), "OK");
  EXPECT_EQ(c.request("GET name").value(), "NOT_FOUND");
  EXPECT_EQ(c.request("DEL name").value(), "NOT_FOUND");
  EXPECT_EQ(c.request("FROB x").value(), "ERR unknown command FROB");
  EXPECT_EQ(c.request("GET").value(), "ERR missing key");
}

TEST_F(ServerTest, PipelinedRequestsAnsweredInOrder) {
  auto c = connect();
  // Send 200 requests in one write, then read the 200 responses.
  std::string batch;
  for (int i = 0; i < 100; ++i) {
    batch += "PUT k" + std::to_string(i) + " v" + std::to_string(i) + "\n";
    batch += "GET k" + std::to_string(i) + "\n";
  }
  batch.pop_back();  // send_line() appends the final newline
  ASSERT_TRUE(c.send_line(batch));
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(c.read_line().value(), "OK");
    EXPECT_EQ(c.read_line().value(), "VALUE v" + std::to_string(i));
  }
}

TEST_F(ServerTest, ManyConcurrentClients) {
  constexpr int kClients = 16;
  constexpr int kOps = 300;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kClients; ++t) {
    threads.emplace_back([&, t] {
      kv::Client c;
      if (!c.connect("127.0.0.1", server_->port())) {
        ++failures;
        return;
      }
      for (int i = 0; i < kOps; ++i) {
        const std::string key = "c" + std::to_string(t) + "_" + std::to_string(i);
        if (c.request("PUT " + key + " " + std::to_string(i)) != std::optional<std::string>("OK")) ++failures;
        if (c.request("GET " + key) != std::optional<std::string>("VALUE " + std::to_string(i))) ++failures;
      }
    });
  }
  for (auto& th : threads) th.join();
  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(store_->size(), static_cast<size_t>(kClients * kOps));
  EXPECT_GE(server_->requests_served(), static_cast<uint64_t>(2 * kClients * kOps));
}

TEST_F(ServerTest, LargeValueRoundTrip) {
  auto c = connect();
  const std::string big(200000, 'z');
  EXPECT_EQ(c.request("PUT big " + big).value(), "OK");
  EXPECT_EQ(c.request("GET big").value(), "VALUE " + big);
}

TEST_F(ServerTest, ClientDisconnectDoesNotAffectOthers) {
  {
    auto c1 = connect();
    c1.request("PUT a 1");
  }  // c1 closes
  auto c2 = connect();
  EXPECT_EQ(c2.request("GET a").value(), "VALUE 1");
}

}  // namespace
