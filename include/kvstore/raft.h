#pragma once

// Raft consensus (Ongaro & Ousterhout, 2014): leader election, log
// replication and commitment.
//
// RaftCore is a pure state machine with no threads, clocks or sockets. The
// caller drives it:
//   tick()           every fixed interval (e.g. 10 ms) for timeouts
//   step(msg)        for every message received from a peer
//   propose(cmd)     to append a client command (leader only)
//   take_messages()  to collect messages to send
//   storage->sync()  before sending them (votes/acks must be durable first)
//   apply_committed() to feed newly committed entries to the state machine
// This keeps the protocol logic deterministic and lets tests run whole
// clusters over a simulated, lossy network.
//
// Not implemented (see README): log compaction/snapshots, membership changes,
// pre-vote, and ReadIndex/lease reads (reads go through the log instead).

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace kv {

struct LogEntry {
  uint64_t term = 0;
  std::string command;  // empty = no-op appended by a new leader
};

enum class MsgType : uint8_t {
  kRequestVote = 1,
  kRequestVoteReply = 2,
  kAppendEntries = 3,
  kAppendEntriesReply = 4,
};

struct Message {
  MsgType type = MsgType::kAppendEntries;
  int from = 0;
  int to = 0;
  uint64_t term = 0;
  // RequestVote
  uint64_t last_log_index = 0;
  uint64_t last_log_term = 0;
  bool granted = false;
  // AppendEntries
  uint64_t prev_index = 0;
  uint64_t prev_term = 0;
  uint64_t commit = 0;
  std::vector<LogEntry> entries;
  // AppendEntriesReply: on success, the follower's last matching index; on
  // failure, a hint for the next index the leader should try.
  bool success = false;
  uint64_t match_index = 0;
};

std::string encode_message(const Message& m);
bool decode_message(const std::string& data, Message& out);

// Durable Raft state: current term, vote, and the log.
class RaftStorage {
 public:
  virtual ~RaftStorage() = default;
  // Loads persisted state; the log excludes the dummy entry at index 0.
  virtual void load(uint64_t& term, int& voted_for, std::vector<LogEntry>& log) = 0;
  // Persists term and vote durably before returning.
  virtual void save_hard_state(uint64_t term, int voted_for) = 0;
  // Removes entries at index >= first_index, then appends `entries` starting at
  // first_index. Durable after the next sync().
  virtual void truncate_and_append(uint64_t first_index, const std::vector<LogEntry>& entries) = 0;
  virtual void sync() = 0;
};

// In-memory storage for tests; survives "restarts" if the object is reused.
class MemoryRaftStorage : public RaftStorage {
 public:
  void load(uint64_t& term, int& voted_for, std::vector<LogEntry>& log) override;
  void save_hard_state(uint64_t term, int voted_for) override;
  void truncate_and_append(uint64_t first_index, const std::vector<LogEntry>& entries) override;
  void sync() override {}

 private:
  uint64_t term_ = 0;
  int voted_for_ = 0;
  std::vector<LogEntry> log_;
};

// File-backed storage: <dir>/raft_state (term + vote, replaced atomically) and
// <dir>/raft_log (append-only records [crc32][term u64][len u32][command]).
class FileRaftStorage : public RaftStorage {
 public:
  explicit FileRaftStorage(std::string dir);
  ~FileRaftStorage() override;
  void load(uint64_t& term, int& voted_for, std::vector<LogEntry>& log) override;
  void save_hard_state(uint64_t term, int voted_for) override;
  void truncate_and_append(uint64_t first_index, const std::vector<LogEntry>& entries) override;
  void sync() override;

 private:
  std::string dir_;
  int log_fd_ = -1;
  bool dirty_ = false;
  std::vector<uint64_t> offsets_;  // offsets_[i] = file offset of entry i+1
  uint64_t end_ = 0;
};

enum class Role { kFollower, kCandidate, kLeader };
const char* role_name(Role r);

struct RaftConfig {
  int id = 1;                   // ids are positive; 0 means "none"
  std::vector<int> peers;       // other members' ids
  int election_ticks = 15;      // election timeout is random in [n, 2n) ticks
  int heartbeat_ticks = 3;
  size_t max_entries_per_message = 512;
  uint64_t seed = 0;            // 0 = derive from id
};

class RaftCore {
 public:
  using ApplyFn = std::function<void(uint64_t index, const LogEntry& entry)>;

  RaftCore(RaftConfig cfg, RaftStorage* storage, ApplyFn apply);

  void tick();
  void step(const Message& m);
  // Appends a command if this node is the leader; returns (index, term).
  std::optional<std::pair<uint64_t, uint64_t>> propose(std::string command);
  std::vector<Message> take_messages();
  void apply_committed();
  // Starts an election now instead of waiting for the timeout (used by tests
  // and for leadership transfer).
  void campaign() { become_candidate(); }

  Role role() const { return role_; }
  uint64_t term() const { return term_; }
  int leader() const { return leader_; }
  int id() const { return cfg_.id; }
  uint64_t commit_index() const { return commit_; }
  uint64_t last_applied() const { return last_applied_; }
  uint64_t last_index() const { return log_.size() - 1; }
  uint64_t term_at(uint64_t index) const { return index < log_.size() ? log_[index].term : 0; }
  const LogEntry& entry(uint64_t index) const { return log_[index]; }

 private:
  size_t quorum() const { return (cfg_.peers.size() + 1) / 2 + 1; }
  void reset_election_timer();
  void become_follower(uint64_t term, int leader);
  void become_candidate();
  void become_leader();
  void persist_hard_state();
  void append_local(uint64_t first_index, std::vector<LogEntry> entries);
  void send_append(int peer);
  void broadcast_append();
  void maybe_advance_commit();
  void handle_request_vote(const Message& m);
  void handle_vote_reply(const Message& m);
  void handle_append(const Message& m);
  void handle_append_reply(const Message& m);
  void send(Message m);

  RaftConfig cfg_;
  RaftStorage* storage_;
  ApplyFn apply_;
  std::mt19937_64 rng_;

  // Persistent state.
  uint64_t term_ = 0;
  int voted_for_ = 0;
  std::vector<LogEntry> log_;  // log_[0] is a dummy entry with term 0

  // Volatile state.
  Role role_ = Role::kFollower;
  int leader_ = 0;
  uint64_t commit_ = 0;
  uint64_t last_applied_ = 0;
  int election_elapsed_ = 0;
  int election_timeout_ = 0;
  int heartbeat_elapsed_ = 0;
  std::set<int> votes_;
  std::map<int, uint64_t> next_index_;
  std::map<int, uint64_t> match_index_;
  std::vector<Message> outbox_;
};

}  // namespace kv
