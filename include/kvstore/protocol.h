#pragma once

// Text protocol, one request and one response per line:
//
//   PING              -> PONG
//   PUT <key> <value> -> OK                (value = rest of the line, may contain spaces)
//   GET <key>         -> VALUE <value> | NOT_FOUND
//   DEL <key>         -> OK | NOT_FOUND
//   STATS             -> STATS key=value ...
//   anything else     -> ERR <message>
//
// Keys cannot contain spaces; neither keys nor values can contain newlines.
// Commands are case-insensitive. Easy to try with `nc localhost 7000`.

#include <string>

namespace kv {

class KvStore;

struct Request {
  std::string cmd;  // upper-cased
  std::string key;
  std::string value;
};

// Splits a line into command, key and value. Returns false if the line is empty.
bool parse_request(const std::string& line, Request& out);

// Executes one request line against a local store.
std::string handle_request(KvStore& store, const std::string& line);

}  // namespace kv
