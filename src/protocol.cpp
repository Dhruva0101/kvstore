#include "kvstore/protocol.h"

#include <cctype>
#include <sstream>

#include "kvstore/kvstore.h"

namespace kv {

bool parse_request(const std::string& line, Request& out) {
  size_t i = 0;
  auto skip_spaces = [&] {
    while (i < line.size() && line[i] == ' ') ++i;
  };
  auto word = [&] {
    skip_spaces();
    const size_t start = i;
    while (i < line.size() && line[i] != ' ') ++i;
    return line.substr(start, i - start);
  };
  out.cmd = word();
  for (auto& ch : out.cmd) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  out.key = word();
  if (i < line.size()) ++i;  // exactly one separator: leading spaces in values are kept
  out.value = i < line.size() ? line.substr(i) : "";
  return !out.cmd.empty();
}

std::string handle_request(KvStore& store, const std::string& line) {
  Request r;
  if (!parse_request(line, r)) return "ERR empty request";
  if (r.cmd == "PING") return "PONG";
  if (r.cmd == "STATS") {
    const Stats s = store.stats();
    std::ostringstream o;
    o << "STATS sstables=" << s.sstables << " sstable_bytes=" << s.sstable_bytes
      << " memtable_bytes=" << s.memtable_bytes << " flushes=" << s.flushes
      << " compactions=" << s.compactions << " filter_skips=" << s.filter_skips;
    return o.str();
  }
  if (r.key.empty()) return "ERR missing key";
  if (r.cmd == "PUT") return store.put(r.key, r.value) ? "OK" : "ERR write failed";
  if (r.cmd == "GET") {
    auto v = store.get(r.key);
    return v ? "VALUE " + *v : "NOT_FOUND";
  }
  if (r.cmd == "DEL") return store.del(r.key) ? "OK" : "NOT_FOUND";
  return "ERR unknown command " + r.cmd;
}

}  // namespace kv
