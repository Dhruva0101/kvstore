#include "kvstore/kvstore.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <set>
#include <sstream>
#include <stdexcept>

namespace kv {

namespace {

constexpr const char* kManifestHeader = "kvstore-manifest v1";
constexpr size_t kEntryOverhead = 32;  // rough per-entry memtable overhead

void make_dirs(const std::string& dir) {
  std::string partial;
  std::stringstream ss(dir);
  std::string part;
  if (!dir.empty() && dir[0] == '/') partial = "/";
  while (std::getline(ss, part, '/')) {
    if (part.empty()) continue;
    partial += part + "/";
    if (::mkdir(partial.c_str(), 0755) != 0 && errno != EEXIST) {
      throw std::runtime_error("cannot create directory " + partial + ": " + std::strerror(errno));
    }
  }
}

// fsync the directory so a rename() or new file inside it is durable.
void sync_dir(const std::string& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd >= 0) {
    ::fsync(fd);
    ::close(fd);
  }
}

}  // namespace

KvStore::KvStore(std::string dir, Options opts) : dir_(std::move(dir)), opts_(opts) {
  if (dir_.empty()) throw std::invalid_argument("KvStore needs a directory");
  make_dirs(dir_);
  std::lock_guard<std::mutex> lock(mu_);
  load_manifest_locked();
  if (wal_number_ == 0) {
    wal_number_ = new_file_number();
    write_manifest_locked();
  }
  remove_unreferenced_files_locked();

  // Rebuild the memtable from the active WAL before accepting new writes.
  recovered_ = Wal::replay(file_path(wal_number_, "wal"), [this](const WalRecord& rec) {
    Entry e;
    if (rec.op == Op::kPut) {
      e.value = rec.value;
    } else {
      e.type = EntryType::kTombstone;
    }
    memtable_bytes_ += rec.key.size() + e.value.size() + kEntryOverhead;
    memtable_[rec.key] = std::move(e);
  });
  wal_ = std::make_unique<Wal>(file_path(wal_number_, "wal"), opts_.sync_every_write);
}

KvStore::~KvStore() = default;

std::string KvStore::file_path(uint64_t num, const char* ext) const {
  char name[32];
  std::snprintf(name, sizeof(name), "%06llu.%s", static_cast<unsigned long long>(num), ext);
  return dir_ + "/" + name;
}

bool KvStore::put(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mu_);
  Entry e;
  e.value = value;
  return put_locked(key, std::move(e));
}

bool KvStore::del(const std::string& key) {
  std::lock_guard<std::mutex> lock(mu_);
  auto existing = lookup_locked(key);
  if (!existing || existing->deleted()) return false;
  Entry e;
  e.type = EntryType::kTombstone;
  return put_locked(key, std::move(e));
}

bool KvStore::put_locked(const std::string& key, Entry e) {
  // Log first, then apply: if we crash in between, replay redoes the write.
  const Op op = e.deleted() ? Op::kDelete : Op::kPut;
  if (!wal_->append({op, key, e.value})) return false;
  memtable_bytes_ += key.size() + e.value.size() + kEntryOverhead;
  memtable_[key] = std::move(e);
  if (memtable_bytes_ >= opts_.memtable_max_bytes) flush_locked();
  return true;
}

std::optional<std::string> KvStore::get(const std::string& key) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto e = lookup_locked(key);
  if (!e || e->deleted()) return std::nullopt;
  return std::move(e->value);
}

std::optional<Entry> KvStore::lookup_locked(const std::string& key) const {
  auto it = memtable_.find(key);
  if (it != memtable_.end()) return it->second;
  for (auto t = tables_.rbegin(); t != tables_.rend(); ++t) {  // newest first
    auto e = t->sst->get(key);
    if (e) return e;
  }
  return std::nullopt;
}

bool KvStore::flush() {
  std::lock_guard<std::mutex> lock(mu_);
  flush_locked();
  return true;
}

void KvStore::flush_locked() {
  if (memtable_.empty()) return;
  // 1. Write the memtable to a new SSTable (fsync'd inside write_sstable).
  std::vector<KeyEntry> sorted;
  sorted.reserve(memtable_.size());
  for (auto& [k, e] : memtable_) sorted.push_back({k, e});
  const uint64_t sst_num = new_file_number();
  SstWriteOptions wo;
  wo.block_size = opts_.block_size;
  wo.bloom_bits_per_key = opts_.bloom_bits_per_key;
  write_sstable(file_path(sst_num, "sst"), sorted, wo);
  tables_.push_back({sst_num, SsTable::open(file_path(sst_num, "sst"))});

  // 2. Switch to a new WAL and commit both changes in one MANIFEST update.
  const uint64_t old_wal = wal_number_;
  wal_number_ = new_file_number();
  write_manifest_locked();
  wal_ = std::make_unique<Wal>(file_path(wal_number_, "wal"), opts_.sync_every_write);

  // 3. The old WAL's contents now live in the SSTable.
  std::remove(file_path(old_wal, "wal").c_str());
  memtable_.clear();
  memtable_bytes_ = 0;
  ++stats_.flushes;
  maybe_compact_locked();
}

void KvStore::compact_all() {
  std::lock_guard<std::mutex> lock(mu_);
  if (tables_.size() > 1 || (tables_.size() == 1 && tables_[0].sst->entry_count() > 0)) {
    compact_range_locked(0, tables_.size() - 1);
  }
}

// Size-tiered compaction. Walks the SSTables oldest to newest looking for a run
// of adjacent tables with similar sizes. Only *adjacent* tables are merged so
// "newer table wins" stays true for every key without per-entry timestamps.
// Repeats until nothing qualifies, so merged tables can cascade into bigger
// tiers (4 small -> 1 medium; 4 medium -> 1 large).
void KvStore::maybe_compact_locked() {
  const size_t min_tables = opts_.compaction_min_tables;
  if (min_tables < 2) return;
  bool changed = true;
  while (changed) {
    changed = false;
    for (size_t start = 0; start + min_tables <= tables_.size() && !changed; ++start) {
      double total = 0;
      size_t end = start;  // exclusive
      while (end < tables_.size()) {
        const double size = static_cast<double>(tables_[end].sst->file_size());
        const double avg = end == start ? size : total / static_cast<double>(end - start);
        if (end > start &&
            (size < avg * opts_.compaction_bucket_low || size > avg * opts_.compaction_bucket_high)) {
          break;
        }
        total += size;
        ++end;
      }
      if (end - start >= min_tables) {
        compact_range_locked(start, end - 1);
        changed = true;
      }
    }
  }
}

// Merges tables_[first..last] into one new SSTable that replaces them in place.
void KvStore::compact_range_locked(size_t first, size_t last) {
  // A tombstone can be dropped only if no older table could still hold a value
  // it is hiding, i.e. when the run includes the oldest table.
  const bool drop_tombstones = first == 0;
  std::vector<KeyEntry> merged;
  merge_all_locked(
      [&](const KeyEntry& ke) {
        if (!(drop_tombstones && ke.entry.deleted())) merged.push_back(ke);
      },
      first, last, false);

  std::vector<Table> replacement;
  if (!merged.empty()) {
    const uint64_t num = new_file_number();
    SstWriteOptions wo;
    wo.block_size = opts_.block_size;
    wo.bloom_bits_per_key = opts_.bloom_bits_per_key;
    write_sstable(file_path(num, "sst"), merged, wo);
    replacement.push_back({num, SsTable::open(file_path(num, "sst"))});
  }

  std::vector<Table> old(tables_.begin() + static_cast<std::ptrdiff_t>(first),
                         tables_.begin() + static_cast<std::ptrdiff_t>(last) + 1);
  tables_.erase(tables_.begin() + static_cast<std::ptrdiff_t>(first),
                tables_.begin() + static_cast<std::ptrdiff_t>(last) + 1);
  tables_.insert(tables_.begin() + static_cast<std::ptrdiff_t>(first), replacement.begin(),
                 replacement.end());
  write_manifest_locked();  // commit point: the new table is now live
  for (const auto& t : old) std::remove(file_path(t.number, "sst").c_str());
  ++stats_.compactions;
}

void KvStore::write_manifest_locked() {
  const std::string tmp = dir_ + "/MANIFEST.tmp";
  {
    std::ofstream out(tmp, std::ios::trunc);
    out << kManifestHeader << "\n";
    out << "next_file " << next_file_ << "\n";
    out << "wal " << wal_number_ << "\n";
    for (const auto& t : tables_) out << "sst " << t.number << "\n";
    out.flush();
    if (!out) throw std::runtime_error("cannot write " + tmp);
  }
  const int fd = ::open(tmp.c_str(), O_RDONLY);
  if (fd >= 0) {
    ::fsync(fd);
    ::close(fd);
  }
  if (std::rename(tmp.c_str(), (dir_ + "/MANIFEST").c_str()) != 0) {
    throw std::runtime_error("cannot install MANIFEST: " + std::string(std::strerror(errno)));
  }
  sync_dir(dir_);
}

void KvStore::load_manifest_locked() {
  std::ifstream in(dir_ + "/MANIFEST");
  if (!in) return;  // fresh store
  std::string line;
  std::getline(in, line);
  if (line != kManifestHeader) throw std::runtime_error("unrecognized MANIFEST in " + dir_);
  std::string kind;
  uint64_t num;
  while (in >> kind >> num) {
    if (kind == "next_file") {
      next_file_ = num;
    } else if (kind == "wal") {
      wal_number_ = num;
    } else if (kind == "sst") {
      tables_.push_back({num, SsTable::open(file_path(num, "sst"))});
    }
  }
}

// Deletes .sst/.wal files that the MANIFEST does not reference: leftovers from
// a crash in the middle of a flush or compaction.
void KvStore::remove_unreferenced_files_locked() {
  std::set<std::string> live = {file_path(wal_number_, "wal")};
  for (const auto& t : tables_) live.insert(file_path(t.number, "sst"));
  DIR* d = ::opendir(dir_.c_str());
  if (!d) return;
  while (dirent* ent = ::readdir(d)) {
    const std::string name = ent->d_name;
    const bool ours = name.size() > 4 && (name.compare(name.size() - 4, 4, ".sst") == 0 ||
                                          name.compare(name.size() - 4, 4, ".wal") == 0);
    const std::string full = dir_ + "/" + name;
    if (ours && !live.count(full)) std::remove(full.c_str());
  }
  ::closedir(d);
}

// K-way merge over tables_[first..last] (and optionally the memtable), calling
// fn once per key with the newest version of that key. Tombstones are passed
// through; callers decide whether to keep them.
void KvStore::merge_all_locked(const std::function<void(const KeyEntry&)>& fn, size_t first_table,
                               size_t last_table, bool include_memtable) const {
  // Sources ordered newest first so ties resolve to the newest version.
  struct Source {
    std::unique_ptr<SsTable::Iterator> it;
    std::map<std::string, Entry>::const_iterator mem, mem_end;
    bool is_mem = false;
    bool valid() const { return is_mem ? mem != mem_end : it->valid(); }
    const std::string& key() const { return is_mem ? mem->first : it->current().key; }
    const Entry& entry() const { return is_mem ? mem->second : it->current().entry; }
    void next() {
      if (is_mem) {
        ++mem;
      } else {
        it->next();
      }
    }
  };
  std::vector<Source> sources;
  if (include_memtable) {
    Source s;
    s.is_mem = true;
    s.mem = memtable_.begin();
    s.mem_end = memtable_.end();
    sources.push_back(std::move(s));
  }
  if (!tables_.empty()) {
    for (size_t i = last_table + 1; i-- > first_table;) {
      Source s;
      s.it = std::make_unique<SsTable::Iterator>(tables_[i].sst.get());
      sources.push_back(std::move(s));
    }
  }

  while (true) {
    const std::string* min_key = nullptr;
    for (const auto& s : sources) {
      if (s.valid() && (!min_key || s.key() < *min_key)) min_key = &s.key();
    }
    if (!min_key) break;
    const std::string key = *min_key;
    bool emitted = false;
    for (auto& s : sources) {
      if (s.valid() && s.key() == key) {
        if (!emitted) {
          fn({key, s.entry()});  // first source = newest version
          emitted = true;
        }
        s.next();
      }
    }
  }
}

void KvStore::scan(const std::function<void(const std::string&, const std::string&)>& fn) const {
  std::lock_guard<std::mutex> lock(mu_);
  const size_t last = tables_.empty() ? 0 : tables_.size() - 1;
  merge_all_locked(
      [&](const KeyEntry& ke) {
        if (!ke.entry.deleted()) fn(ke.key, ke.entry.value);
      },
      0, last, true);
}

size_t KvStore::size() const {
  size_t n = 0;
  scan([&](const std::string&, const std::string&) { ++n; });
  return n;
}

size_t KvStore::sstable_count() const {
  std::lock_guard<std::mutex> lock(mu_);
  return tables_.size();
}

Stats KvStore::stats() const {
  std::lock_guard<std::mutex> lock(mu_);
  Stats s = stats_;
  s.sstables = tables_.size();
  s.memtable_bytes = memtable_bytes_;
  for (const auto& t : tables_) {
    s.sstable_bytes += t.sst->file_size();
    s.filter_skips += t.sst->filter_skips();
  }
  return s;
}

}  // namespace kv
