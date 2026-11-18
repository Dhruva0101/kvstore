// kvcli: tiny REPL for trying the store by hand.
//   ./kvcli data/
//   > put name dhruva
//   > get name
//   > del name
//   > quit
#include <iostream>
#include <sstream>
#include <string>

#include "kvstore/kvstore.h"

int main(int argc, char** argv) {
  const std::string path = argc > 1 ? argv[1] : "data";
  kv::KvStore store(path);
  std::cout << "kvstore: recovered " << store.recovered_records() << " WAL records from " << path
            << " (" << store.size() << " keys)\n";

  std::string line;
  while (std::cout << "> " && std::getline(std::cin, line)) {
    std::istringstream in(line);
    std::string cmd, key, value;
    in >> cmd >> key;
    std::getline(in >> std::ws, value);

    if (cmd == "quit" || cmd == "exit") break;
    if (cmd == "put" && !key.empty()) {
      std::cout << (store.put(key, value) ? "OK" : "ERROR") << "\n";
    } else if (cmd == "get" && !key.empty()) {
      auto v = store.get(key);
      std::cout << (v ? *v : "(nil)") << "\n";
    } else if (cmd == "del" && !key.empty()) {
      std::cout << (store.del(key) ? "OK" : "(nil)") << "\n";
    } else if (!cmd.empty()) {
      std::cout << "commands: put <k> <v> | get <k> | del <k> | quit\n";
    }
  }
  return 0;
}
