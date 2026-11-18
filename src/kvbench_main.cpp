// kvbench: load generator for kvserver (over TCP) or the storage engine (in-process).
//
//   ./kvbench --host 127.0.0.1 --port 7000 --threads 8 --ops 20000 --read-ratio 0.5
//   ./kvbench --embedded --dir /tmp/bench --threads 1 --ops 200000 --no-sync
//
// Each thread runs a closed loop (send one request, wait for the reply) over a
// random key from --keys; reports throughput and p50/p99/p99.9 latency.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "kvstore/client.h"
#include "kvstore/kvstore.h"
#include "kvstore/protocol.h"

namespace {

struct Config {
  std::string host = "127.0.0.1";
  uint16_t port = 7000;
  bool embedded = false;
  std::string dir = "bench-data";
  bool sync = true;
  int threads = 4;
  int ops = 10000;  // per thread
  int keys = 100000;
  size_t value_size = 100;
  double read_ratio = 0.5;
  bool preload = true;
};

void usage() {
  std::cerr << "usage: kvbench [--host H] [--port P] | [--embedded --dir D [--no-sync]]\n"
               "               [--threads N] [--ops N-per-thread] [--keys N] [--value-size B]\n"
               "               [--read-ratio 0..1] [--no-preload]\n";
}

using Clock = std::chrono::steady_clock;

std::string key_name(int k) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "key%08d", k);
  return buf;
}

double pct(const std::vector<double>& sorted, double p) {
  if (sorted.empty()) return 0;
  const size_t idx = std::min(sorted.size() - 1, static_cast<size_t>(p * static_cast<double>(sorted.size())));
  return sorted[idx];
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        usage();
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--host") cfg.host = next();
    else if (a == "--port") cfg.port = static_cast<uint16_t>(std::stoi(next()));
    else if (a == "--embedded") cfg.embedded = true;
    else if (a == "--dir") cfg.dir = next();
    else if (a == "--no-sync") cfg.sync = false;
    else if (a == "--threads") cfg.threads = std::stoi(next());
    else if (a == "--ops") cfg.ops = std::stoi(next());
    else if (a == "--keys") cfg.keys = std::stoi(next());
    else if (a == "--value-size") cfg.value_size = std::stoul(next());
    else if (a == "--read-ratio") cfg.read_ratio = std::stod(next());
    else if (a == "--no-preload") cfg.preload = false;
    else {
      usage();
      return 2;
    }
  }

  std::unique_ptr<kv::KvStore> store;
  if (cfg.embedded) {
    std::filesystem::remove_all(cfg.dir);
    kv::Options o;
    o.sync_every_write = cfg.sync;
    store = std::make_unique<kv::KvStore>(cfg.dir, o);
  }
  // One "connection" per thread: either a TCP client or a direct call.
  auto make_exec = [&]() -> std::function<std::optional<std::string>(const std::string&)> {
    if (cfg.embedded) {
      return [&](const std::string& line) { return std::optional<std::string>(kv::handle_request(*store, line)); };
    }
    auto c = std::make_shared<kv::Client>();
    if (!c->connect(cfg.host, cfg.port)) {
      std::cerr << "cannot connect to " << cfg.host << ":" << cfg.port << "\n";
      std::exit(1);
    }
    return [c](const std::string& line) { return c->request(line); };
  };

  const std::string value(cfg.value_size, 'v');
  if (cfg.preload && cfg.read_ratio > 0) {
    std::cout << "preloading " << cfg.keys << " keys..." << std::flush;
    std::vector<std::thread> loaders;
    const int n = std::max(1, cfg.threads);
    for (int t = 0; t < n; ++t) {
      loaders.emplace_back([&, t] {
        auto exec = make_exec();
        for (int k = t; k < cfg.keys; k += n) exec("PUT " + key_name(k) + " " + value);
      });
    }
    for (auto& th : loaders) th.join();
    std::cout << " done\n";
  }

  std::vector<std::vector<double>> lat(static_cast<size_t>(cfg.threads));
  std::atomic<int> errors{0};
  std::vector<std::thread> workers;
  const auto start = Clock::now();
  for (int t = 0; t < cfg.threads; ++t) {
    workers.emplace_back([&, t] {
      auto exec = make_exec();
      std::mt19937_64 rng(static_cast<uint64_t>(t) * 7919 + 1);
      std::uniform_int_distribution<int> key_dist(0, cfg.keys - 1);
      std::uniform_real_distribution<double> op_dist(0, 1);
      auto& my = lat[static_cast<size_t>(t)];
      my.reserve(static_cast<size_t>(cfg.ops));
      for (int i = 0; i < cfg.ops; ++i) {
        const bool read = op_dist(rng) < cfg.read_ratio;
        const std::string line = (read ? "GET " : "PUT ") + key_name(key_dist(rng)) + (read ? "" : " " + value);
        const auto t0 = Clock::now();
        const auto resp = exec(line);
        const auto t1 = Clock::now();
        if (!resp || resp->rfind("ERR", 0) == 0) ++errors;
        my.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
      }
    });
  }
  for (auto& th : workers) th.join();
  const double secs = std::chrono::duration<double>(Clock::now() - start).count();

  std::vector<double> all;
  for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
  std::sort(all.begin(), all.end());
  const double total = static_cast<double>(all.size());

  std::printf("mode=%s threads=%d ops=%zu value=%zuB read_ratio=%.2f fsync=%s\n",
              cfg.embedded ? "embedded" : "tcp", cfg.threads, all.size(), cfg.value_size, cfg.read_ratio,
              cfg.embedded ? (cfg.sync ? "on" : "off") : "server-side");
  std::printf("throughput: %.0f ops/sec  (%.2f s)\n", total / secs, secs);
  std::printf("latency us: p50=%.1f  p99=%.1f  p99.9=%.1f  max=%.1f\n", pct(all, 0.50), pct(all, 0.99),
              pct(all, 0.999), all.empty() ? 0.0 : all.back());
  std::printf("errors: %d\n", errors.load());
  if (store) {
    const auto s = store->stats();
    std::printf("engine: sstables=%llu flushes=%llu compactions=%llu filter_skips=%llu\n",
                static_cast<unsigned long long>(s.sstables), static_cast<unsigned long long>(s.flushes),
                static_cast<unsigned long long>(s.compactions), static_cast<unsigned long long>(s.filter_skips));
    store.reset();
    std::filesystem::remove_all(cfg.dir);
  }
  return errors.load() == 0 ? 0 : 1;
}
