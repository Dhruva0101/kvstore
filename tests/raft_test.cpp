// Raft tests on a simulated network. Everything is single-threaded and
// seeded, so failures are reproducible.
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "kvstore/raft.h"
#include "test_util.h"

namespace {

using kv::Message;
using kv::RaftCore;
using kv::Role;

class Cluster {
 public:
  Cluster(int n, uint64_t seed) : rng_(seed) {
    for (int i = 1; i <= n; ++i) ids_.push_back(i);
    for (int id : ids_) {
      storage_[id] = std::make_unique<kv::MemoryRaftStorage>();
      start(id);
    }
  }

  void start(int id) {
    kv::RaftConfig cfg;
    cfg.id = id;
    for (int p : ids_) {
      if (p != id) cfg.peers.push_back(p);
    }
    cfg.seed = rng_() | 1;
    applied_[id].clear();
    nodes_[id] = std::make_unique<RaftCore>(cfg, storage_[id].get(), [this, id](uint64_t idx, const kv::LogEntry& e) {
      auto& log = applied_[id];
      EXPECT_EQ(idx, log.size() + 1) << "node " << id << " applied out of order";
      log.push_back(e.command);
    });
    down_.erase(id);
  }

  void crash(int id) {
    nodes_.erase(id);
    down_.insert(id);
  }

  // Cuts all links between `group` and everyone else.
  void partition(const std::set<int>& group) {
    cut_.clear();
    for (int a : ids_) {
      for (int b : ids_) {
        if (group.count(a) != group.count(b)) cut_.insert({a, b});
      }
    }
  }
  void heal() { cut_.clear(); }
  void set_drop_rate(double p) { drop_ = p; }

  // One step of simulated time: tick every node, then deliver all messages
  // (in random order, with random loss), repeating until the network is quiet.
  void step() {
    for (auto& [id, n] : nodes_) n->tick();
    for (int round = 0; round < 10; ++round) {
      std::vector<Message> in_flight;
      for (auto& [id, n] : nodes_) {
        n->apply_committed();
        for (auto& m : n->take_messages()) in_flight.push_back(std::move(m));
      }
      if (in_flight.empty()) break;
      std::shuffle(in_flight.begin(), in_flight.end(), rng_);
      std::uniform_real_distribution<double> u(0, 1);
      for (auto& m : in_flight) {
        if (cut_.count({m.from, m.to}) || !nodes_.count(m.to) || u(rng_) < drop_) continue;
        nodes_[m.to]->step(m);
      }
    }
    for (auto& [id, n] : nodes_) n->apply_committed();
    check_election_safety();
    check_state_machine_safety();
  }

  void run(int steps) {
    for (int i = 0; i < steps; ++i) step();
  }

  // Runs until some live node is leader (and a majority agrees on its term).
  int wait_for_leader(int max_steps = 500) {
    for (int i = 0; i < max_steps; ++i) {
      step();
      const int l = leader();
      if (l) return l;
    }
    return 0;
  }

  int leader() const {
    int best = 0;
    uint64_t best_term = 0;
    for (const auto& [id, n] : nodes_) {
      if (n->role() == Role::kLeader && n->term() >= best_term && !isolated(id)) {
        best = id;
        best_term = n->term();
      }
    }
    return best;
  }

  bool isolated(int id) const {
    for (int p : ids_) {
      if (p != id && !cut_.count({id, p})) return false;
    }
    return ids_.size() > 1;
  }

  RaftCore& node(int id) { return *nodes_.at(id); }
  const std::vector<std::string>& applied(int id) { return applied_[id]; }
  const std::vector<int>& ids() const { return ids_; }

  // Commands only (no-op entries filtered out).
  std::vector<std::string> commands(int id) {
    std::vector<std::string> out;
    for (const auto& c : applied_[id]) {
      if (!c.empty()) out.push_back(c);
    }
    return out;
  }

 private:
  // Election safety: at most one leader per term, ever.
  void check_election_safety() {
    for (const auto& [id, n] : nodes_) {
      if (n->role() != Role::kLeader) continue;
      auto [it, inserted] = leader_of_term_.emplace(n->term(), id);
      ASSERT_EQ(it->second, id) << "two leaders in term " << n->term();
    }
  }

  // State machine safety: no two nodes apply different commands at an index.
  void check_state_machine_safety() {
    for (const auto& [id, log] : applied_) {
      for (size_t i = 0; i < log.size(); ++i) {
        auto [it, inserted] = applied_at_.emplace(i, log[i]);
        ASSERT_EQ(it->second, log[i]) << "nodes applied different entries at index " << i + 1;
      }
    }
  }

  std::mt19937_64 rng_;
  std::vector<int> ids_;
  std::map<int, std::unique_ptr<kv::MemoryRaftStorage>> storage_;
  std::map<int, std::unique_ptr<RaftCore>> nodes_;
  std::map<int, std::vector<std::string>> applied_;
  std::set<int> down_;
  std::set<std::pair<int, int>> cut_;
  double drop_ = 0;
  std::map<uint64_t, int> leader_of_term_;
  std::map<size_t, std::string> applied_at_;
};

TEST(RaftMessage, EncodeDecodeRoundTrip) {
  Message m;
  m.type = kv::MsgType::kAppendEntries;
  m.from = 2;
  m.to = 3;
  m.term = 7;
  m.prev_index = 41;
  m.prev_term = 6;
  m.commit = 40;
  m.entries = {{6, "PUT a 1"}, {7, ""}, {7, std::string(1000, 'x')}};
  Message d;
  ASSERT_TRUE(kv::decode_message(kv::encode_message(m), d));
  EXPECT_EQ(d.type, m.type);
  EXPECT_EQ(d.from, 2);
  EXPECT_EQ(d.term, 7u);
  EXPECT_EQ(d.prev_index, 41u);
  ASSERT_EQ(d.entries.size(), 3u);
  EXPECT_EQ(d.entries[2].command.size(), 1000u);
  std::string truncated = kv::encode_message(m);
  truncated.pop_back();
  EXPECT_FALSE(kv::decode_message(truncated, d));
}

TEST(Raft, SingleNodeElectsItselfAndCommits) {
  Cluster c(1, 1);
  ASSERT_EQ(c.wait_for_leader(), 1);
  ASSERT_TRUE(c.node(1).propose("x").has_value());
  c.step();
  EXPECT_EQ(c.commands(1), std::vector<std::string>{"x"});
}

TEST(Raft, ElectsExactlyOneLeader) {
  Cluster c(3, 2);
  const int l = c.wait_for_leader();
  ASSERT_NE(l, 0);
  c.run(50);
  int leaders = 0;
  for (int id : c.ids()) leaders += c.node(id).role() == Role::kLeader;
  EXPECT_EQ(leaders, 1);
  EXPECT_EQ(c.leader(), l);  // stable without failures
}

TEST(Raft, FollowerRejectsProposals) {
  Cluster c(3, 3);
  const int l = c.wait_for_leader();
  const int f = l % 3 + 1;
  EXPECT_FALSE(c.node(f).propose("nope").has_value());
}

TEST(Raft, ReplicatesToAllNodesInOrder) {
  Cluster c(3, 4);
  const int l = c.wait_for_leader();
  std::vector<std::string> want;
  for (int i = 0; i < 100; ++i) {
    want.push_back("cmd" + std::to_string(i));
    ASSERT_TRUE(c.node(l).propose(want.back()).has_value());
    if (i % 10 == 0) c.step();
  }
  c.run(20);
  for (int id : c.ids()) EXPECT_EQ(c.commands(id), want) << "node " << id;
}

TEST(Raft, NewLeaderAfterLeaderIsPartitionedAndOldLeaderCatchesUp) {
  Cluster c(3, 5);
  const int old = c.wait_for_leader();
  c.node(old).propose("before");
  c.run(10);
  const uint64_t old_term = c.node(old).term();

  c.partition({old});
  // The isolated leader still accepts a proposal, but it can never commit.
  c.node(old).propose("lost");
  const int fresh = c.wait_for_leader();
  ASSERT_NE(fresh, 0);
  ASSERT_NE(fresh, old);
  EXPECT_GT(c.node(fresh).term(), old_term);
  c.node(fresh).propose("after");
  c.run(10);

  c.heal();
  c.run(30);
  EXPECT_EQ(c.node(old).role(), Role::kFollower);
  const std::vector<std::string> want = {"before", "after"};
  for (int id : c.ids()) EXPECT_EQ(c.commands(id), want) << "node " << id;
}

TEST(Raft, MinorityCannotCommit) {
  Cluster c(5, 6);
  const int l = c.wait_for_leader();
  // Leader plus one follower on the minority side.
  const int buddy = l % 5 + 1;
  c.partition({l, buddy});
  const uint64_t commit_before = c.node(l).commit_index();
  c.node(l).propose("minority-write");
  c.run(30);
  EXPECT_EQ(c.node(l).commit_index(), commit_before);
}

TEST(Raft, CrashedFollowerRestartsFromStorageAndCatchesUp) {
  Cluster c(3, 7);
  const int l = c.wait_for_leader();
  const int f = l % 3 + 1;
  for (int i = 0; i < 20; ++i) c.node(l).propose("a" + std::to_string(i));
  c.run(10);
  c.crash(f);
  for (int i = 0; i < 20; ++i) c.node(l).propose("b" + std::to_string(i));
  c.run(10);
  c.start(f);  // same storage: keeps term, vote and log
  EXPECT_GE(c.node(f).last_index(), 21u);
  c.run(30);
  EXPECT_EQ(c.commands(f).size(), 40u);
  EXPECT_EQ(c.commands(f), c.commands(l));
}

TEST(Raft, FiveNodesTolerateTwoFailures) {
  Cluster c(5, 8);
  const int l = c.wait_for_leader();
  int crashed = 0;
  for (int id : c.ids()) {
    if (id != l && crashed < 2) {
      c.crash(id);
      ++crashed;
    }
  }
  c.node(l).propose("still-works");
  c.run(10);
  EXPECT_EQ(c.commands(l), std::vector<std::string>{"still-works"});
}

// Random message loss and random partitions; the safety checks in step()
// assert that no two leaders share a term and that applied logs never diverge.
class RaftChaos : public ::testing::TestWithParam<uint64_t> {};

TEST_P(RaftChaos, SafetyUnderLossAndPartitions) {
  const uint64_t seed = GetParam();
  Cluster c(5, seed);
  std::mt19937_64 rng(seed);
  c.set_drop_rate(0.1);
  int proposed = 0;
  for (int epoch = 0; epoch < 30; ++epoch) {
    if (rng() % 3 == 0) {
      std::set<int> side;
      for (int id : c.ids()) {
        if (rng() % 2) side.insert(id);
      }
      c.partition(side);
    } else {
      c.heal();
    }
    for (int s = 0; s < 40; ++s) {
      for (int id : c.ids()) {
        if (c.node(id).role() == Role::kLeader && rng() % 4 == 0) {
          c.node(id).propose("e" + std::to_string(epoch) + "_" + std::to_string(proposed++));
        }
      }
      c.step();
    }
  }
  // Liveness once the network is healthy again: everyone converges.
  c.heal();
  c.set_drop_rate(0);
  const int l = c.wait_for_leader();
  ASSERT_NE(l, 0);
  c.node(l).propose("final");
  c.run(100);
  for (int id : c.ids()) {
    EXPECT_EQ(c.commands(id), c.commands(l)) << "node " << id;
    ASSERT_FALSE(c.commands(id).empty());
    EXPECT_EQ(c.commands(id).back(), "final");
  }
}

INSTANTIATE_TEST_SUITE_P(Seeds, RaftChaos, ::testing::Values(11, 22, 33, 44, 55, 66, 77, 88));

// The scenario from Figure 8 of the Raft paper, driven message by message.
// A leader must not commit an entry from an earlier term just because it is
// stored on a majority: a later leader can still overwrite it. Removing the
// "only count replicas for current-term entries" rule makes this test fail.
class ManualCluster {
 public:
  ManualCluster() {
    for (int id = 1; id <= 5; ++id) {
      storage_[id] = std::make_unique<kv::MemoryRaftStorage>();
      start(id);
    }
  }
  void start(int id) {
    kv::RaftConfig cfg;
    cfg.id = id;
    for (int p = 1; p <= 5; ++p) {
      if (p != id) cfg.peers.push_back(p);
    }
    cfg.election_ticks = 1000000;  // no spontaneous elections
    cfg.heartbeat_ticks = 1;       // a leader's tick() sends a heartbeat
    cfg.max_entries_per_message = 1;
    nodes_[id] = std::make_unique<RaftCore>(cfg, storage_[id].get(), [this, id](uint64_t idx, const kv::LogEntry& e) {
      auto [it, inserted] = ever_applied_.emplace(idx, e.command + "@" + std::to_string(e.term));
      EXPECT_EQ(it->second, e.command + "@" + std::to_string(e.term))
          << "node " << id << " applied a different entry at index " << idx;
    });
  }
  void crash(int id) { nodes_.erase(id); }
  RaftCore& node(int id) { return *nodes_.at(id); }

  // Delivers messages for which allow(m) is true until the network is quiet;
  // everything else is dropped.
  void deliver(const std::function<bool(const Message&)>& allow) {
    for (int round = 0; round < 50; ++round) {
      std::vector<Message> q;
      for (auto& [id, n] : nodes_) {
        for (auto& m : n->take_messages()) q.push_back(std::move(m));
      }
      if (q.empty()) break;
      for (auto& m : q) {
        if (nodes_.count(m.to) && allow(m)) nodes_[m.to]->step(m);
      }
      for (auto& [id, n] : nodes_) n->apply_committed();
    }
  }
  static std::function<bool(const Message&)> among(std::set<int> s) {
    return [s](const Message& m) { return s.count(m.from) && s.count(m.to); };
  }

 private:
  std::map<int, std::unique_ptr<kv::MemoryRaftStorage>> storage_;
  std::map<int, std::unique_ptr<RaftCore>> nodes_;
  std::map<uint64_t, std::string> ever_applied_;
};

TEST(Raft, Figure8LeaderDoesNotCommitOldTermEntriesByCounting) {
  ManualCluster c;
  const auto votes_only = [](const Message& m) {
    return m.type == kv::MsgType::kRequestVote || m.type == kv::MsgType::kRequestVoteReply;
  };

  // Term 1: S1 leads; its no-op (index 1) reaches everyone.
  c.node(1).campaign();
  c.deliver(ManualCluster::among({1, 2, 3, 4, 5}));
  ASSERT_EQ(c.node(1).role(), Role::kLeader);

  // S1 appends "A" at index 2, but only S2 receives it. S1 crashes.
  c.node(1).propose("A");
  c.deliver(ManualCluster::among({1, 2}));
  c.crash(1);

  // Term 2: S5 wins with votes from S3 and S4, appends a no-op at index 2,
  // and crashes before sending it anywhere.
  c.node(5).campaign();
  c.deliver(votes_only);
  ASSERT_EQ(c.node(5).role(), Role::kLeader);
  c.node(5).take_messages();
  c.crash(5);

  // S1 restarts. Term 2 fails (S3 and S4 already voted for S5); term 3 wins.
  c.start(1);
  c.node(1).campaign();
  c.deliver(votes_only);
  ASSERT_NE(c.node(1).role(), Role::kLeader);
  c.node(1).campaign();
  c.deliver(votes_only);
  ASSERT_EQ(c.node(1).role(), Role::kLeader);
  ASSERT_EQ(c.node(1).term(), 3u);

  // S1 replicates to S2 and S3, but S3 never receives index 3 (S1's term-3
  // no-op). "A" (index 2, term 1) is now stored on S1, S2 and S3: a majority.
  // A correct leader still must not commit it.
  c.node(1).tick();  // heartbeat
  c.deliver([](const Message& m) {
    const bool among_123 = m.from <= 3 && m.to <= 3;
    const bool carries_index3_to_s3 = m.to == 3 && m.type == kv::MsgType::kAppendEntries &&
                                      m.prev_index == 2 && !m.entries.empty();
    return among_123 && !carries_index3_to_s3;
  });
  EXPECT_LT(c.node(1).commit_index(), 2u) << "leader committed an old-term entry by counting replicas";
  c.crash(1);

  // Term 4: S5 restarts and wins with S3 and S4 (its last log term, 2, beats
  // their 1), then overwrites index 2 on S3 with its own entry and commits.
  c.start(5);
  c.node(5).campaign();  // term 3: everyone already voted
  c.deliver(votes_only);
  c.node(5).campaign();  // term 4
  c.deliver(votes_only);
  ASSERT_EQ(c.node(5).role(), Role::kLeader);
  c.node(5).tick();
  c.deliver(ManualCluster::among({3, 4, 5}));
  EXPECT_GE(c.node(5).commit_index(), 3u);
  // ManualCluster's apply callback has been checking all along that no index
  // was ever applied with two different entries.
}

using RaftStorageTest = kvtest::TempDirTest;

TEST_F(RaftStorageTest, FileStoragePersistsStateAndLog) {
  {
    kv::FileRaftStorage s(dir_);
    uint64_t term;
    int vote;
    std::vector<kv::LogEntry> log;
    s.load(term, vote, log);
    EXPECT_EQ(term, 0u);
    s.save_hard_state(5, 2);
    s.truncate_and_append(1, {{1, "a"}, {1, "b"}, {2, "c"}});
    s.truncate_and_append(3, {{3, "c2"}, {3, "d"}});  // overwrite a conflicting suffix
    s.sync();
  }
  kv::FileRaftStorage s(dir_);
  uint64_t term;
  int vote;
  std::vector<kv::LogEntry> log;
  s.load(term, vote, log);
  EXPECT_EQ(term, 5u);
  EXPECT_EQ(vote, 2);
  ASSERT_EQ(log.size(), 4u);
  EXPECT_EQ(log[1].command, "b");
  EXPECT_EQ(log[2].command, "c2");
  EXPECT_EQ(log[3].term, 3u);
}

TEST_F(RaftStorageTest, TornTailIsDiscarded) {
  {
    kv::FileRaftStorage s(dir_);
    uint64_t t;
    int v;
    std::vector<kv::LogEntry> l;
    s.load(t, v, l);
    s.truncate_and_append(1, {{1, "a"}, {1, "b"}});
    s.sync();
  }
  {
    std::ofstream out(dir_ + "/raft_log", std::ios::binary | std::ios::app);
    out << "garbage";
  }
  kv::FileRaftStorage s(dir_);
  uint64_t t;
  int v;
  std::vector<kv::LogEntry> l;
  s.load(t, v, l);
  ASSERT_EQ(l.size(), 2u);
  s.truncate_and_append(3, {{2, "c"}});
  s.sync();
  kv::FileRaftStorage again(dir_);
  again.load(t, v, l);
  ASSERT_EQ(l.size(), 3u);
  EXPECT_EQ(l[2].command, "c");
}

}  // namespace
