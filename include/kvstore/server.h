#pragma once

// Multi-reactor TCP server (the memcached / Netty "one event loop per thread"
// model):
//
//   acceptor ─▶ round-robin ─▶ event loop 0 … N-1   (each: epoll or kqueue)
//                                 │ owns its connections; reads lines,
//                                 │ calls handler(line, reply) inline
//                                 ▼
//                  reply(resp) on the loop thread → appended directly
//                  reply(resp) from another thread → completion queue +
//                                 wake-up pipe → loop writes it
//
// Handlers for the local store reply immediately on the loop thread, so a
// request never changes threads. The Raft handler replies later from the Raft
// thread once the command commits, without blocking any event loop.
//
// The protocol is line-based: one request per '\n'-terminated line, one
// response line per request. A connection has at most one request in flight,
// so pipelined requests on one connection are answered in order.

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace kv {

class Server {
 public:
  using Reply = std::function<void(std::string response)>;
  // Called on an event-loop thread with one request line (no newline). Must
  // eventually call reply exactly once, from any thread.
  using AsyncHandler = std::function<void(const std::string& line, Reply reply)>;
  // Convenience: a handler that computes the response synchronously.
  using Handler = std::function<std::string(const std::string& line)>;

  // port 0 picks a free port; see port() after start(). io_threads 0 = one per core.
  Server(uint16_t port, size_t io_threads, AsyncHandler handler);
  Server(uint16_t port, size_t io_threads, Handler handler);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Binds, listens and starts the threads. Throws std::runtime_error on failure.
  void start();
  // Stops accepting, closes all connections and joins all threads.
  void stop();

  uint16_t port() const { return port_; }
  uint64_t requests_served() const;

 private:
  class Loop;
  void accept_loop();

  AsyncHandler handler_;
  size_t io_threads_;
  uint16_t port_;
  int listen_fd_ = -1;
  std::vector<std::unique_ptr<Loop>> loops_;
  std::unique_ptr<Loop> acceptor_;
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> requests_{0};
};

}  // namespace kv
