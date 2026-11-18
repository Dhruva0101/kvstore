#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <stdexcept>

#include "kvstore/coding.h"
#include "kvstore/crc32.h"
#include "kvstore/raft.h"

namespace kv {

namespace {

[[noreturn]] void fail(const std::string& what) {
  throw std::runtime_error(what + ": " + std::strerror(errno));
}

void write_all(int fd, const std::string& s) {
  const char* p = s.data();
  size_t n = s.size();
  while (n > 0) {
    const ssize_t w = ::write(fd, p, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      fail("raft log write");
    }
    p += w;
    n -= static_cast<size_t>(w);
  }
}

}  // namespace

FileRaftStorage::FileRaftStorage(std::string dir) : dir_(std::move(dir)) {
  ::mkdir(dir_.c_str(), 0755);
  log_fd_ = ::open((dir_ + "/raft_log").c_str(), O_RDWR | O_CREAT, 0644);
  if (log_fd_ < 0) fail("open raft_log in " + dir_);
}

FileRaftStorage::~FileRaftStorage() {
  if (log_fd_ >= 0) {
    ::fsync(log_fd_);
    ::close(log_fd_);
  }
}

void FileRaftStorage::load(uint64_t& term, int& voted_for, std::vector<LogEntry>& log) {
  term = 0;
  voted_for = 0;
  {
    std::ifstream st(dir_ + "/raft_state");
    st >> term >> voted_for;
  }
  std::ifstream in(dir_ + "/raft_log", std::ios::binary);
  std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  log.clear();
  offsets_.clear();
  size_t pos = 0;
  while (pos + 16 <= data.size()) {
    const uint32_t crc = get_u32(&data[pos]);
    const uint64_t t = get_u64(&data[pos + 4]);
    const uint32_t len = get_u32(&data[pos + 12]);
    if (pos + 16 + len > data.size()) break;  // torn tail
    if (crc32(&data[pos + 4], 12 + len) != crc) break;
    offsets_.push_back(pos);
    log.push_back(LogEntry{t, data.substr(pos + 16, len)});
    pos += 16 + len;
  }
  end_ = pos;
  // Drop a torn or corrupt tail so new appends start on a record boundary.
  if (::ftruncate(log_fd_, static_cast<off_t>(end_)) != 0) fail("truncate raft_log");
}

void FileRaftStorage::save_hard_state(uint64_t term, int voted_for) {
  const std::string tmp = dir_ + "/raft_state.tmp";
  {
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) fail("open raft_state.tmp");
    write_all(fd, std::to_string(term) + " " + std::to_string(voted_for) + "\n");
    ::fsync(fd);
    ::close(fd);
  }
  if (std::rename(tmp.c_str(), (dir_ + "/raft_state").c_str()) != 0) fail("rename raft_state");
  const int dfd = ::open(dir_.c_str(), O_RDONLY);
  if (dfd >= 0) {
    ::fsync(dfd);
    ::close(dfd);
  }
}

void FileRaftStorage::truncate_and_append(uint64_t first_index, const std::vector<LogEntry>& entries) {
  if (first_index - 1 < offsets_.size()) {
    end_ = offsets_[first_index - 1];
    offsets_.resize(first_index - 1);
    if (::ftruncate(log_fd_, static_cast<off_t>(end_)) != 0) fail("truncate raft_log");
  }
  std::string buf;
  for (const auto& e : entries) {
    std::string body;
    put_u64(body, e.term);
    put_u32(body, static_cast<uint32_t>(e.command.size()));
    body += e.command;
    offsets_.push_back(end_ + buf.size());
    put_u32(buf, crc32(body.data(), body.size()));
    buf += body;
  }
  if (::lseek(log_fd_, static_cast<off_t>(end_), SEEK_SET) < 0) fail("seek raft_log");
  write_all(log_fd_, buf);
  end_ += buf.size();
  dirty_ = true;
}

void FileRaftStorage::sync() {
  if (!dirty_) return;
  if (::fsync(log_fd_) != 0) fail("fsync raft_log");
  dirty_ = false;
}

}  // namespace kv
