#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace kv {

// Minimal blocking client for the line protocol (see protocol.h).
class Client {
 public:
  Client() = default;
  ~Client();
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  Client(Client&& other) noexcept;
  Client& operator=(Client&& other) noexcept;

  // Connects with a timeout; returns false on failure.
  bool connect(const std::string& host, uint16_t port, int timeout_ms = 2000);
  bool connected() const { return fd_ >= 0; }
  void close();

  // Sends one request line and waits for one response line. Returns nullopt if
  // the connection failed or timed out (the connection is then closed).
  std::optional<std::string> request(const std::string& line, int timeout_ms = 5000);

  // Lower-level halves of request(), for pipelining many requests at once.
  bool send_line(const std::string& line);
  std::optional<std::string> read_line(int timeout_ms = 5000);

 private:
  int fd_ = -1;
  std::string buf_;
};

// "host:port" -> (host, port). Returns false if malformed.
bool parse_host_port(const std::string& s, std::string& host, uint16_t& port);

}  // namespace kv
