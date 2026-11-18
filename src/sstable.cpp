#include "kvstore/sstable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "kvstore/coding.h"
#include "kvstore/crc32.h"

namespace kv {

namespace {

constexpr size_t kFooterSize = 8 + 4 + 8 + 4 + 8 + 8;

[[noreturn]] void fail(const std::string& what, const std::string& path) {
  throw std::runtime_error(what + " (" + path + "): " + std::strerror(errno));
}

void encode_entry(std::string& buf, const KeyEntry& e) {
  buf.push_back(static_cast<char>(e.entry.type));
  put_u32(buf, static_cast<uint32_t>(e.key.size()));
  put_u32(buf, static_cast<uint32_t>(e.entry.value.size()));
  buf += e.key;
  buf += e.entry.value;
}

class FileWriter {
 public:
  explicit FileWriter(const std::string& path) : path_(path) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) fail("cannot create sstable", path);
  }
  ~FileWriter() {
    if (fd_ >= 0) ::close(fd_);
  }
  // Writes the block followed by its CRC; returns the block's offset.
  uint64_t write_block(const std::string& block) {
    const uint64_t off = offset_;
    std::string crc;
    put_u32(crc, crc32(block.data(), block.size()));
    write(block);
    write(crc);
    return off;
  }
  void write(const std::string& s) {
    const char* p = s.data();
    size_t n = s.size();
    while (n > 0) {
      const ssize_t w = ::write(fd_, p, n);
      if (w < 0) {
        if (errno == EINTR) continue;
        fail("write failed", path_);
      }
      p += w;
      n -= static_cast<size_t>(w);
    }
    offset_ += s.size();
  }
  void sync() {
    if (::fsync(fd_) != 0) fail("fsync failed", path_);
  }

 private:
  std::string path_;
  int fd_ = -1;
  uint64_t offset_ = 0;
};

}  // namespace

void write_sstable(const std::string& path, const std::vector<KeyEntry>& sorted,
                   const SstWriteOptions& opts) {
  FileWriter w(path);
  std::string index;
  std::string block;
  const std::string* last_key = nullptr;

  auto finish_block = [&]() {
    if (block.empty()) return;
    const uint64_t off = w.write_block(block);
    put_u32(index, static_cast<uint32_t>(last_key->size()));
    index += *last_key;
    put_u64(index, off);
    put_u32(index, static_cast<uint32_t>(block.size()));
    block.clear();
  };

  for (const auto& e : sorted) {
    encode_entry(block, e);
    last_key = &e.key;
    if (block.size() >= opts.block_size) finish_block();
  }
  finish_block();

  const std::string filter = build_filter(sorted, opts.bloom_bits_per_key);
  const uint64_t filter_off = w.write_block(filter);
  const uint64_t index_off = w.write_block(index);

  std::string footer;
  put_u64(footer, index_off);
  put_u32(footer, static_cast<uint32_t>(index.size()));
  put_u64(footer, filter_off);
  put_u32(footer, static_cast<uint32_t>(filter.size()));
  put_u64(footer, sorted.size());
  put_u64(footer, kSstMagic);
  w.write(footer);
  w.sync();
}

std::shared_ptr<SsTable> SsTable::open(const std::string& path) {
  std::shared_ptr<SsTable> t(new SsTable());
  t->path_ = path;
  t->fd_ = ::open(path.c_str(), O_RDONLY);
  if (t->fd_ < 0) fail("cannot open sstable", path);
  struct stat st {};
  if (::fstat(t->fd_, &st) != 0) fail("stat failed", path);
  t->file_size_ = static_cast<uint64_t>(st.st_size);
  if (t->file_size_ < kFooterSize) throw std::runtime_error("sstable too small: " + path);

  std::string footer(kFooterSize, '\0');
  if (::pread(t->fd_, footer.data(), kFooterSize,
              static_cast<off_t>(t->file_size_ - kFooterSize)) != static_cast<ssize_t>(kFooterSize)) {
    fail("cannot read footer", path);
  }
  Reader r(footer.data(), footer.size());
  uint64_t index_off, filter_off, magic;
  uint32_t index_len, filter_len;
  r.u64(index_off);
  r.u32(index_len);
  r.u64(filter_off);
  r.u32(filter_len);
  r.u64(t->entry_count_);
  r.u64(magic);
  if (magic != kSstMagic) throw std::runtime_error("bad sstable magic: " + path);

  t->filter_ = t->read_checked(filter_off, filter_len);
  const std::string index = t->read_checked(index_off, index_len);
  Reader ir(index.data(), index.size());
  while (ir.remaining() > 0) {
    IndexEntry ie;
    uint32_t klen;
    if (!ir.u32(klen) || !ir.bytes(klen, ie.last_key) || !ir.u64(ie.offset) || !ir.u32(ie.size)) {
      throw std::runtime_error("corrupt sstable index: " + path);
    }
    t->index_.push_back(std::move(ie));
  }
  if (!t->index_.empty()) {
    t->largest_ = t->index_.back().last_key;
    t->smallest_ = t->read_block(0).front().key;
  }
  return t;
}

SsTable::~SsTable() {
  if (fd_ >= 0) ::close(fd_);
}

std::string SsTable::read_checked(uint64_t off, uint32_t len) const {
  std::string buf(static_cast<size_t>(len) + 4, '\0');
  const ssize_t n = ::pread(fd_, buf.data(), buf.size(), static_cast<off_t>(off));
  if (n != static_cast<ssize_t>(buf.size())) {
    throw std::runtime_error("short read in sstable: " + path_);
  }
  const uint32_t stored = get_u32(buf.data() + len);
  buf.resize(len);
  if (crc32(buf.data(), buf.size()) != stored) {
    throw std::runtime_error("checksum mismatch in sstable: " + path_);
  }
  return buf;
}

std::vector<KeyEntry> SsTable::read_block(size_t b) const {
  const std::string data = read_checked(index_[b].offset, index_[b].size);
  std::vector<KeyEntry> out;
  Reader r(data.data(), data.size());
  while (r.remaining() > 0) {
    uint8_t type;
    uint32_t klen, vlen;
    KeyEntry e;
    if (!r.u8(type) || !r.u32(klen) || !r.u32(vlen) || !r.bytes(klen, e.key) ||
        !r.bytes(vlen, e.entry.value)) {
      throw std::runtime_error("corrupt sstable block: " + path_);
    }
    e.entry.type = static_cast<EntryType>(type);
    out.push_back(std::move(e));
  }
  return out;
}

bool SsTable::may_contain(const std::string& key) const {
  return filter_.empty() || filter_may_contain(filter_, key);
}

std::optional<Entry> SsTable::get(const std::string& key) const {
  if (index_.empty() || key < smallest_ || key > largest_) return std::nullopt;
  if (!may_contain(key)) {
    ++filter_skips_;
    return std::nullopt;
  }
  // First block whose last key >= key.
  auto it = std::lower_bound(index_.begin(), index_.end(), key,
                             [](const IndexEntry& ie, const std::string& k) { return ie.last_key < k; });
  if (it == index_.end()) return std::nullopt;
  const auto entries = read_block(static_cast<size_t>(it - index_.begin()));
  auto e = std::lower_bound(entries.begin(), entries.end(), key,
                            [](const KeyEntry& ke, const std::string& k) { return ke.key < k; });
  if (e == entries.end() || e->key != key) return std::nullopt;
  return e->entry;
}

SsTable::Iterator::Iterator(const SsTable* t) : t_(t) {
  if (!t_->index_.empty()) load_block(0);
}

void SsTable::Iterator::load_block(size_t b) {
  block_ = b;
  entries_ = t_->read_block(b);
  pos_ = 0;
  valid_ = !entries_.empty();
  if (valid_) cur_ = entries_[0];
}

void SsTable::Iterator::next() {
  if (!valid_) return;
  if (++pos_ < entries_.size()) {
    cur_ = entries_[pos_];
    return;
  }
  if (block_ + 1 < t_->index_.size()) {
    load_block(block_ + 1);
  } else {
    valid_ = false;
  }
}

}  // namespace kv
