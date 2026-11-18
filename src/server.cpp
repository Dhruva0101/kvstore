#include "kvstore/server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/epoll.h>
#else
#include <sys/event.h>
#endif

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace kv {

namespace {

constexpr size_t kMaxLine = 1u << 20;  // reject requests longer than 1 MiB

void set_nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

[[noreturn]] void sys_fail(const char* what) {
  throw std::runtime_error(std::string(what) + ": " + std::strerror(errno));
}

// Thin wrapper so the event loop is the same on Linux (epoll) and macOS (kqueue).
class Poller {
 public:
  struct Event {
    int fd;
    bool readable;
    bool writable;
    bool error;
  };

  Poller() {
#if defined(__linux__)
    fd_ = ::epoll_create1(0);
#else
    fd_ = ::kqueue();
#endif
    if (fd_ < 0) sys_fail("poller create");
  }
  ~Poller() { ::close(fd_); }

  void add(int fd) {
#if defined(__linux__)
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    ::epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &ev);
#else
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    ::kevent(fd_, &ev, 1, nullptr, 0, nullptr);
#endif
  }

  // Enable or disable write-readiness notifications for fd.
  void want_write(int fd, bool on) {
#if defined(__linux__)
    epoll_event ev{};
    ev.events = EPOLLIN | (on ? EPOLLOUT : 0);
    ev.data.fd = fd;
    ::epoll_ctl(fd_, EPOLL_CTL_MOD, fd, &ev);
#else
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_WRITE, on ? EV_ADD : EV_DELETE, 0, 0, nullptr);
    ::kevent(fd_, &ev, 1, nullptr, 0, nullptr);
#endif
  }

  void remove(int fd) {
#if defined(__linux__)
    ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr);
#else
    (void)fd;  // kqueue drops filters automatically when the fd is closed
#endif
  }

  // Returns the number of events written to `out`.
  int wait(Event* out, int max, int timeout_ms) {
#if defined(__linux__)
    epoll_event evs[128];
    const int n = ::epoll_wait(fd_, evs, std::min(max, 128), timeout_ms);
    for (int i = 0; i < n; ++i) {
      out[i] = {evs[i].data.fd, (evs[i].events & EPOLLIN) != 0, (evs[i].events & EPOLLOUT) != 0,
                (evs[i].events & (EPOLLERR | EPOLLHUP)) != 0};
    }
#else
    struct kevent evs[128];
    timespec ts{timeout_ms / 1000, (timeout_ms % 1000) * 1000000L};
    const int n = ::kevent(fd_, nullptr, 0, evs, std::min(max, 128), &ts);
    for (int i = 0; i < n; ++i) {
      // EV_EOF on the read filter still has buffered data to read first.
      out[i] = {static_cast<int>(evs[i].ident), evs[i].filter == EVFILT_READ,
                evs[i].filter == EVFILT_WRITE, (evs[i].flags & EV_ERROR) != 0};
    }
#endif
    return n < 0 ? 0 : n;
  }

 private:
  int fd_;
};

}  // namespace

// One event loop thread and the connections it owns.
class Server::Loop {
 public:
  Loop(Server* server, int listen_fd) : server_(server), listen_fd_(listen_fd) {
    if (::pipe(wake_) != 0) sys_fail("pipe");
    set_nonblocking(wake_[0]);
    set_nonblocking(wake_[1]);
    poller_.add(wake_[0]);
    if (listen_fd_ >= 0) poller_.add(listen_fd_);
  }

  ~Loop() {
    stop();
    for (auto& [fd, c] : conns_) ::close(fd);
    for (int fd : new_fds_) ::close(fd);
    ::close(wake_[0]);
    ::close(wake_[1]);
  }

  void start() {
    running_ = true;
    thread_ = std::thread([this] { run(); });
  }

  void stop() {
    if (!running_.exchange(false)) return;
    wake();
    thread_.join();
  }

  // Hands a freshly accepted socket to this loop (called from the acceptor).
  void adopt(int fd) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      new_fds_.push_back(fd);
    }
    wake();
  }

 private:
  struct Connection {
    uint64_t id;
    int fd;
    std::string in;
    std::string out;
    std::deque<std::string> pending;  // complete lines waiting to be handled
    bool in_flight = false;
    bool peer_closed = false;
    bool want_write = false;
  };

  void wake() {
    const char b = 1;
    (void)!::write(wake_[1], &b, 1);  // a full pipe already means "wake up"
  }

  void run() {
    loop_thread_ = std::this_thread::get_id();
    Poller::Event evs[128];
    while (running_.load()) {
      const int n = poller_.wait(evs, 128, 200);
      for (int i = 0; i < n; ++i) {
        const auto& ev = evs[i];
        if (ev.fd == wake_[0]) {
          char buf[256];
          while (::read(wake_[0], buf, sizeof(buf)) > 0) {
          }
          adopt_new();
          deliver_completions();
        } else if (ev.fd == listen_fd_) {
          server_->accept_loop();
        } else {
          auto it = conns_.find(ev.fd);
          if (it == conns_.end()) continue;
          if (ev.error) {
            close_conn(ev.fd);
            continue;
          }
          if (ev.readable) on_readable(it->second);
          auto again = conns_.find(ev.fd);  // on_readable may have closed it
          if (again != conns_.end() && ev.writable) flush_out(again->second);
        }
      }
    }
  }

  void adopt_new() {
    std::vector<int> fds;
    {
      std::lock_guard<std::mutex> lock(mu_);
      fds.swap(new_fds_);
    }
    for (int fd : fds) {
      Connection c;
      c.id = next_id_++;
      c.fd = fd;
      fd_of_[c.id] = fd;
      conns_.emplace(fd, std::move(c));
      poller_.add(fd);
    }
  }

  void on_readable(Connection& c) {
    char buf[16384];
    while (true) {
      const ssize_t n = ::read(c.fd, buf, sizeof(buf));
      if (n > 0) {
        c.in.append(buf, static_cast<size_t>(n));
        continue;
      }
      if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) c.peer_closed = true;
      break;
    }
    size_t start = 0;
    while (true) {
      const size_t nl = c.in.find('\n', start);
      if (nl == std::string::npos) break;
      std::string line = c.in.substr(start, nl - start);
      if (!line.empty() && line.back() == '\r') line.pop_back();
      if (!line.empty()) c.pending.push_back(std::move(line));
      start = nl + 1;
    }
    c.in.erase(0, start);
    if (c.in.size() > kMaxLine) {
      c.out += "ERR line too long\n";
      c.in.clear();
      c.peer_closed = true;
    }
    const int fd = c.fd;
    dispatch(c);
    auto it = conns_.find(fd);
    if (it == conns_.end()) return;
    flush_out(it->second);
    maybe_close(fd);
  }

  // Runs pending requests until one is waiting on an asynchronous reply.
  void dispatch(Connection& c) {
    while (!c.in_flight && !c.pending.empty()) {
      c.in_flight = true;
      const std::string line = std::move(c.pending.front());
      c.pending.pop_front();
      const uint64_t id = c.id;
      Reply reply = [this, id](std::string resp) { on_reply(id, std::move(resp)); };
      try {
        server_->handler_(line, std::move(reply));
      } catch (const std::exception& e) {
        on_reply(id, std::string("ERR ") + e.what());
      }
    }
  }

  void on_reply(uint64_t id, std::string resp) {
    server_->requests_.fetch_add(1, std::memory_order_relaxed);
    if (std::this_thread::get_id() == loop_thread_) {
      // Synchronous reply from inside dispatch(): append directly.
      auto f = fd_of_.find(id);
      if (f == fd_of_.end()) return;
      Connection& c = conns_.at(f->second);
      c.out += resp;
      c.out += '\n';
      c.in_flight = false;
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      done_.emplace_back(id, std::move(resp));
    }
    wake();
  }

  void deliver_completions() {
    std::vector<std::pair<uint64_t, std::string>> batch;
    {
      std::lock_guard<std::mutex> lock(mu_);
      batch.swap(done_);
    }
    for (auto& [id, resp] : batch) {
      auto f = fd_of_.find(id);
      if (f == fd_of_.end()) continue;  // connection already closed
      const int fd = f->second;
      Connection& c = conns_.at(fd);
      c.out += resp;
      c.out += '\n';
      c.in_flight = false;
      dispatch(c);
      auto it = conns_.find(fd);
      if (it == conns_.end()) continue;
      flush_out(it->second);
      maybe_close(fd);
    }
  }

  void maybe_close(int fd) {
    auto it = conns_.find(fd);
    if (it == conns_.end()) return;
    const Connection& c = it->second;
    if (c.peer_closed && !c.in_flight && c.pending.empty()) close_conn(fd);
  }

  void flush_out(Connection& c) {
    while (!c.out.empty()) {
#ifdef MSG_NOSIGNAL
      const ssize_t n = ::send(c.fd, c.out.data(), c.out.size(), MSG_NOSIGNAL);
#else
      const ssize_t n = ::send(c.fd, c.out.data(), c.out.size(), 0);
#endif
      if (n > 0) {
        c.out.erase(0, static_cast<size_t>(n));
        continue;
      }
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (!c.want_write) poller_.want_write(c.fd, true);  // resume when the socket drains
        c.want_write = true;
        return;
      }
      if (n < 0 && errno == EINTR) continue;
      c.out.clear();  // peer is gone
      c.peer_closed = true;
      return;
    }
    if (c.want_write) poller_.want_write(c.fd, false);
    c.want_write = false;
  }

  void close_conn(int fd) {
    auto it = conns_.find(fd);
    if (it == conns_.end()) return;
    fd_of_.erase(it->second.id);
    poller_.remove(fd);
    ::close(fd);
    conns_.erase(it);
  }

  Server* server_;
  int listen_fd_;
  Poller poller_;
  int wake_[2] = {-1, -1};
  std::thread thread_;
  std::thread::id loop_thread_;
  std::atomic<bool> running_{false};

  // Owned by the loop thread.
  uint64_t next_id_ = 1;
  std::unordered_map<int, Connection> conns_;  // by fd
  std::unordered_map<uint64_t, int> fd_of_;    // id -> fd (ids are never reused)

  // Shared with other threads, guarded by mu_.
  std::mutex mu_;
  std::vector<int> new_fds_;
  std::vector<std::pair<uint64_t, std::string>> done_;
};

Server::Server(uint16_t port, size_t io_threads, AsyncHandler handler)
    : handler_(std::move(handler)),
      io_threads_(io_threads ? io_threads : std::max(1u, std::thread::hardware_concurrency())),
      port_(port) {}

Server::Server(uint16_t port, size_t io_threads, Handler handler)
    : Server(port, io_threads,
             AsyncHandler([h = std::move(handler)](const std::string& line, Reply reply) { reply(h(line)); })) {}

Server::~Server() { stop(); }

void Server::start() {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) sys_fail("socket");
  int one = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port_);
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) sys_fail("bind");
  if (::listen(listen_fd_, 1024) != 0) sys_fail("listen");
  socklen_t len = sizeof(addr);
  ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
  port_ = ntohs(addr.sin_port);
  set_nonblocking(listen_fd_);

  for (size_t i = 0; i < io_threads_; ++i) loops_.push_back(std::make_unique<Loop>(this, -1));
  acceptor_ = std::make_unique<Loop>(this, listen_fd_);
  running_ = true;
  for (auto& l : loops_) l->start();
  acceptor_->start();
}

// Runs on the acceptor loop's thread when the listening socket is readable.
void Server::accept_loop() {
  static thread_local size_t next = 0;
  while (true) {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) return;  // EAGAIN: nothing more to accept
    set_nonblocking(fd);
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
    loops_[next++ % loops_.size()]->adopt(fd);
  }
}

void Server::stop() {
  if (!running_.exchange(false)) return;
  acceptor_->stop();
  for (auto& l : loops_) l->stop();
  acceptor_.reset();
  loops_.clear();
  ::close(listen_fd_);
}

uint64_t Server::requests_served() const { return requests_.load(); }

}  // namespace kv
