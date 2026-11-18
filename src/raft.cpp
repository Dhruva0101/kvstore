#include "kvstore/raft.h"

#include <algorithm>

#include "kvstore/coding.h"

namespace kv {

const char* role_name(Role r) {
  switch (r) {
    case Role::kFollower:
      return "follower";
    case Role::kCandidate:
      return "candidate";
    case Role::kLeader:
      return "leader";
  }
  return "?";
}

// ---- wire format ----

std::string encode_message(const Message& m) {
  std::string b;
  b.push_back(static_cast<char>(m.type));
  put_u32(b, static_cast<uint32_t>(m.from));
  put_u32(b, static_cast<uint32_t>(m.to));
  put_u64(b, m.term);
  put_u64(b, m.last_log_index);
  put_u64(b, m.last_log_term);
  b.push_back(m.granted ? 1 : 0);
  put_u64(b, m.prev_index);
  put_u64(b, m.prev_term);
  put_u64(b, m.commit);
  b.push_back(m.success ? 1 : 0);
  put_u64(b, m.match_index);
  put_u32(b, static_cast<uint32_t>(m.entries.size()));
  for (const auto& e : m.entries) {
    put_u64(b, e.term);
    put_u32(b, static_cast<uint32_t>(e.command.size()));
    b += e.command;
  }
  return b;
}

bool decode_message(const std::string& data, Message& m) {
  Reader r(data.data(), data.size());
  uint8_t type, granted, success;
  uint32_t from, to, n;
  if (!r.u8(type) || !r.u32(from) || !r.u32(to) || !r.u64(m.term) || !r.u64(m.last_log_index) ||
      !r.u64(m.last_log_term) || !r.u8(granted) || !r.u64(m.prev_index) || !r.u64(m.prev_term) ||
      !r.u64(m.commit) || !r.u8(success) || !r.u64(m.match_index) || !r.u32(n)) {
    return false;
  }
  if (type < 1 || type > 4) return false;
  m.type = static_cast<MsgType>(type);
  m.from = static_cast<int>(from);
  m.to = static_cast<int>(to);
  m.granted = granted != 0;
  m.success = success != 0;
  m.entries.clear();
  for (uint32_t i = 0; i < n; ++i) {
    LogEntry e;
    uint32_t len;
    if (!r.u64(e.term) || !r.u32(len) || !r.bytes(len, e.command)) return false;
    m.entries.push_back(std::move(e));
  }
  return r.remaining() == 0;
}

// ---- in-memory storage ----

void MemoryRaftStorage::load(uint64_t& term, int& voted_for, std::vector<LogEntry>& log) {
  term = term_;
  voted_for = voted_for_;
  log = log_;
}

void MemoryRaftStorage::save_hard_state(uint64_t term, int voted_for) {
  term_ = term;
  voted_for_ = voted_for;
}

void MemoryRaftStorage::truncate_and_append(uint64_t first_index, const std::vector<LogEntry>& entries) {
  log_.resize(first_index - 1);
  log_.insert(log_.end(), entries.begin(), entries.end());
}

// ---- RaftCore ----

RaftCore::RaftCore(RaftConfig cfg, RaftStorage* storage, ApplyFn apply)
    : cfg_(std::move(cfg)),
      storage_(storage),
      apply_(std::move(apply)),
      rng_(cfg_.seed ? cfg_.seed : static_cast<uint64_t>(cfg_.id) * 0x9e3779b97f4a7c15ull) {
  std::vector<LogEntry> persisted;
  storage_->load(term_, voted_for_, persisted);
  log_.push_back(LogEntry{});  // dummy entry at index 0
  log_.insert(log_.end(), persisted.begin(), persisted.end());
  reset_election_timer();
}

void RaftCore::reset_election_timer() {
  election_elapsed_ = 0;
  std::uniform_int_distribution<int> d(cfg_.election_ticks, 2 * cfg_.election_ticks - 1);
  election_timeout_ = d(rng_);
}

void RaftCore::persist_hard_state() { storage_->save_hard_state(term_, voted_for_); }

void RaftCore::send(Message m) {
  m.from = cfg_.id;
  m.term = term_;
  outbox_.push_back(std::move(m));
}

std::vector<Message> RaftCore::take_messages() {
  std::vector<Message> out;
  out.swap(outbox_);
  return out;
}

void RaftCore::tick() {
  if (role_ == Role::kLeader) {
    if (++heartbeat_elapsed_ >= cfg_.heartbeat_ticks) {
      heartbeat_elapsed_ = 0;
      broadcast_append();
    }
    return;
  }
  if (++election_elapsed_ >= election_timeout_) become_candidate();
}

void RaftCore::become_follower(uint64_t term, int leader) {
  role_ = Role::kFollower;
  if (term > term_) {
    term_ = term;
    voted_for_ = 0;
    persist_hard_state();
  }
  leader_ = leader;
  reset_election_timer();
}

void RaftCore::become_candidate() {
  role_ = Role::kCandidate;
  ++term_;
  voted_for_ = cfg_.id;
  persist_hard_state();
  leader_ = 0;
  votes_ = {cfg_.id};
  reset_election_timer();
  if (votes_.size() >= quorum()) {  // single-node cluster
    become_leader();
    return;
  }
  for (int p : cfg_.peers) {
    Message m;
    m.type = MsgType::kRequestVote;
    m.to = p;
    m.last_log_index = last_index();
    m.last_log_term = term_at(last_index());
    send(std::move(m));
  }
}

void RaftCore::become_leader() {
  role_ = Role::kLeader;
  leader_ = cfg_.id;
  heartbeat_elapsed_ = 0;
  for (int p : cfg_.peers) {
    next_index_[p] = last_index() + 1;
    match_index_[p] = 0;
  }
  // A no-op entry from the new term lets the leader commit entries left over
  // from earlier terms (Raft section 5.4.2 / 8).
  append_local(last_index() + 1, {LogEntry{term_, ""}});
  broadcast_append();
  maybe_advance_commit();
}

void RaftCore::append_local(uint64_t first_index, std::vector<LogEntry> entries) {
  storage_->truncate_and_append(first_index, entries);
  log_.resize(first_index);
  for (auto& e : entries) log_.push_back(std::move(e));
}

std::optional<std::pair<uint64_t, uint64_t>> RaftCore::propose(std::string command) {
  if (role_ != Role::kLeader) return std::nullopt;
  const uint64_t index = last_index() + 1;
  append_local(index, {LogEntry{term_, std::move(command)}});
  // Replicate right away to peers that are caught up; the rest get it with
  // their pending AppendEntries or the next heartbeat.
  for (int p : cfg_.peers) {
    if (next_index_[p] == index) send_append(p);
  }
  maybe_advance_commit();
  return std::make_pair(index, term_);
}

void RaftCore::send_append(int peer) {
  uint64_t& next = next_index_[peer];
  next = std::max<uint64_t>(1, std::min(next, last_index() + 1));
  Message m;
  m.type = MsgType::kAppendEntries;
  m.to = peer;
  m.prev_index = next - 1;
  m.prev_term = term_at(m.prev_index);
  m.commit = commit_;
  const uint64_t end = std::min<uint64_t>(last_index(), next + cfg_.max_entries_per_message - 1);
  for (uint64_t i = next; i <= end; ++i) m.entries.push_back(log_[i]);
  // Optimistic pipelining: assume delivery. A rejection resets next_index.
  next += m.entries.size();
  send(std::move(m));
}

void RaftCore::broadcast_append() {
  for (int p : cfg_.peers) send_append(p);
}

void RaftCore::maybe_advance_commit() {
  // Only entries from the current term are committed by counting replicas
  // (Raft section 5.4.2); earlier entries commit indirectly.
  for (uint64_t n = last_index(); n > commit_; --n) {
    if (term_at(n) != term_) break;
    size_t count = 1;  // self
    for (int p : cfg_.peers) {
      if (match_index_[p] >= n) ++count;
    }
    if (count >= quorum()) {
      commit_ = n;
      break;
    }
  }
}

void RaftCore::apply_committed() {
  while (last_applied_ < commit_) {
    ++last_applied_;
    apply_(last_applied_, log_[last_applied_]);
  }
}

void RaftCore::step(const Message& m) {
  if (m.term > term_) {
    // Any message from a newer term demotes us. Only an AppendEntries tells us
    // who the leader is.
    become_follower(m.term, m.type == MsgType::kAppendEntries ? m.from : 0);
  }
  if (m.term < term_) {
    // Stale sender: tell it about the newer term so it steps down.
    if (m.type == MsgType::kRequestVote) {
      Message r;
      r.type = MsgType::kRequestVoteReply;
      r.to = m.from;
      r.granted = false;
      send(std::move(r));
    } else if (m.type == MsgType::kAppendEntries) {
      Message r;
      r.type = MsgType::kAppendEntriesReply;
      r.to = m.from;
      r.success = false;
      send(std::move(r));
    }
    return;
  }
  switch (m.type) {
    case MsgType::kRequestVote:
      handle_request_vote(m);
      break;
    case MsgType::kRequestVoteReply:
      handle_vote_reply(m);
      break;
    case MsgType::kAppendEntries:
      handle_append(m);
      break;
    case MsgType::kAppendEntriesReply:
      handle_append_reply(m);
      break;
  }
}

void RaftCore::handle_request_vote(const Message& m) {
  const uint64_t my_last_term = term_at(last_index());
  const bool up_to_date = m.last_log_term > my_last_term ||
                          (m.last_log_term == my_last_term && m.last_log_index >= last_index());
  const bool grant = (voted_for_ == 0 || voted_for_ == m.from) && up_to_date;
  if (grant) {
    voted_for_ = m.from;
    persist_hard_state();
    reset_election_timer();
  }
  Message r;
  r.type = MsgType::kRequestVoteReply;
  r.to = m.from;
  r.granted = grant;
  send(std::move(r));
}

void RaftCore::handle_vote_reply(const Message& m) {
  if (role_ != Role::kCandidate || !m.granted) return;
  votes_.insert(m.from);
  if (votes_.size() >= quorum()) become_leader();
}

void RaftCore::handle_append(const Message& m) {
  // m.term == term_: the sender is the legitimate leader for this term.
  if (role_ != Role::kFollower || leader_ != m.from) {
    role_ = Role::kFollower;
    leader_ = m.from;
  }
  reset_election_timer();

  Message r;
  r.type = MsgType::kAppendEntriesReply;
  r.to = m.from;

  if (m.prev_index > last_index()) {
    r.success = false;
    r.match_index = last_index() + 1;  // "my log ends here"
    send(std::move(r));
    return;
  }
  if (term_at(m.prev_index) != m.prev_term) {
    // Skip back over the whole conflicting term in one round trip.
    const uint64_t conflict_term = term_at(m.prev_index);
    uint64_t idx = m.prev_index;
    while (idx > commit_ + 1 && term_at(idx - 1) == conflict_term) --idx;
    r.success = false;
    r.match_index = std::max<uint64_t>(1, idx);
    send(std::move(r));
    return;
  }

  // Append new entries, truncating at the first conflicting one. Entries we
  // already have are left alone (the message may be a stale duplicate).
  for (size_t i = 0; i < m.entries.size(); ++i) {
    const uint64_t idx = m.prev_index + 1 + i;
    if (idx <= last_index() && term_at(idx) == m.entries[i].term) continue;
    append_local(idx, std::vector<LogEntry>(m.entries.begin() + static_cast<std::ptrdiff_t>(i), m.entries.end()));
    break;
  }
  const uint64_t last_new = m.prev_index + m.entries.size();
  if (m.commit > commit_) commit_ = std::min(m.commit, last_new);
  r.success = true;
  r.match_index = last_new;
  send(std::move(r));
}

void RaftCore::handle_append_reply(const Message& m) {
  if (role_ != Role::kLeader) return;
  const int p = m.from;
  if (!m.success) {
    next_index_[p] = std::max(match_index_[p] + 1, m.match_index);
    send_append(p);
    return;
  }
  if (m.match_index > match_index_[p]) {
    match_index_[p] = m.match_index;
    maybe_advance_commit();
  }
  next_index_[p] = std::max(next_index_[p], match_index_[p] + 1);
  if (next_index_[p] <= last_index()) send_append(p);  // more to send
}

}  // namespace kv
