// kvserver: serves one KvStore over TCP.
//   ./kvserver --port 7000 --dir data --threads 8 [--no-sync]
// Try it:  printf 'PUT a 1\nGET a\n' | nc localhost 7000
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include "kvstore/kvstore.h"
#include "kvstore/protocol.h"
#include "kvstore/server.h"

namespace {
volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

void usage() {
  std::cerr << "usage: kvserver [--port N] [--dir PATH] [--threads N] [--no-sync]\n";
}
}  // namespace

int main(int argc, char** argv) {
  uint16_t port = 7000;
  std::string dir = "data";
  size_t threads = 0;  // 0 = one event loop per core
  kv::Options opts;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        usage();
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--port") port = static_cast<uint16_t>(std::stoi(next()));
    else if (a == "--dir") dir = next();
    else if (a == "--threads") threads = std::stoul(next());
    else if (a == "--no-sync") opts.sync_every_write = false;
    else {
      usage();
      return 2;
    }
  }
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  std::signal(SIGPIPE, SIG_IGN);

  kv::KvStore store(dir, opts);
  kv::Server server(port, threads, [&](const std::string& line) { return kv::handle_request(store, line); });
  server.start();
  std::cout << "kvserver listening on :" << server.port() << " (dir=" << dir << ", io_threads=" << (threads ? std::to_string(threads) : "auto")
            << ", fsync=" << (opts.sync_every_write ? "on" : "off") << ")" << std::endl;
  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  server.stop();
  std::cout << "kvserver stopped after " << server.requests_served() << " requests" << std::endl;
  return 0;
}
