#include "kvstore/client.h"

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace kv {

Client::~Client() { close(); }

Client::Client(Client&& o) noexcept : fd_(o.fd_), buf_(std::move(o.buf_)) { o.fd_ = -1; }

Client& Client::operator=(Client&& o) noexcept {
  if (this != &o) {
    close();
    fd_ = o.fd_;
    buf_ = std::move(o.buf_);
    o.fd_ = -1;
  }
  return *this;
}

void Client::close() {
  if (fd_ >= 0) ::close(fd_);
  fd_ = -1;
  buf_.clear();
}

bool Client::connect(const std::string& host, uint16_t port, int timeout_ms) {
  close();
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0) return false;
  const int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (fd < 0) {
    ::freeaddrinfo(res);
    return false;
  }
  // Non-blocking connect so an unreachable host fails within timeout_ms.
  const int flags = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);
  ::freeaddrinfo(res);
  if (rc != 0 && errno == EINPROGRESS) {
    pollfd p{fd, POLLOUT, 0};
    if (::poll(&p, 1, timeout_ms) == 1) {
      int err = 0;
      socklen_t len = sizeof(err);
      ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
      rc = err == 0 ? 0 : -1;
    }
  }
  if (rc != 0) {
    ::close(fd);
    return false;
  }
  ::fcntl(fd, F_SETFL, flags);  // back to blocking
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
  ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
  fd_ = fd;
  return true;
}

std::optional<std::string> Client::request(const std::string& line, int timeout_ms) {
  if (!send_line(line)) return std::nullopt;
  return read_line(timeout_ms);
}

bool Client::send_line(const std::string& line) {
  if (fd_ < 0) return false;
  std::string msg = line + "\n";
  const char* p = msg.data();
  size_t n = msg.size();
  while (n > 0) {
#ifdef MSG_NOSIGNAL
    const ssize_t w = ::send(fd_, p, n, MSG_NOSIGNAL);
#else
    const ssize_t w = ::send(fd_, p, n, 0);
#endif
    if (w < 0) {
      if (errno == EINTR) continue;
      close();
      return false;
    }
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

std::optional<std::string> Client::read_line(int timeout_ms) {
  if (fd_ < 0) return std::nullopt;
  while (true) {
    const size_t nl = buf_.find('\n');
    if (nl != std::string::npos) {
      std::string resp = buf_.substr(0, nl);
      buf_.erase(0, nl + 1);
      return resp;
    }
    pollfd pfd{fd_, POLLIN, 0};
    const int ready = ::poll(&pfd, 1, timeout_ms);
    if (ready <= 0) {
      if (ready < 0 && errno == EINTR) continue;
      close();
      return std::nullopt;
    }
    char buf[16384];
    const ssize_t r = ::read(fd_, buf, sizeof(buf));
    if (r <= 0) {
      if (r < 0 && errno == EINTR) continue;
      close();
      return std::nullopt;
    }
    buf_.append(buf, static_cast<size_t>(r));
  }
}

bool parse_host_port(const std::string& s, std::string& host, uint16_t& port) {
  const size_t colon = s.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= s.size()) return false;
  host = s.substr(0, colon);
  try {
    const int p = std::stoi(s.substr(colon + 1));
    if (p <= 0 || p > 65535) return false;
    port = static_cast<uint16_t>(p);
  } catch (...) {
    return false;
  }
  return true;
}

}  // namespace kv
