#pragma once

#include "metadata.pb.h"

#include "rocksdb/db.h"
#include "status.h"
#include <cstdint>
#include <vector>

#define EC_K 4
#define EC_M 2

class MetadataStorage {
  public:
    MetadataStorage(const std::string &db_path) : db_path_(db_path) {}
    ~MetadataStorage() = default;

    Status init();

    std::pair<Status, Attributes> getattr(const uint64_t &inode);
    std::pair<Status, std::vector<Dirent>> readdir(const uint64_t &inode);
    std::pair<Status, FileInfo> open(const uint64_t &inode);

    std::pair<Status, Attributes> create_file(const uint64_t &p_inode,
                                              const std::string &name);
    std::pair<Status, Attributes> create_dir(const uint64_t &p_inode,
                                             const std::string &name);

    Status remove_file(const uint64_t &p_inode, const uint64_t &inode,
                       const std::string &name);
    Status remove_dir(const uint64_t &p_inode, const uint64_t &inode,
                      const std::string &name);

    Status rename_file(const uint64_t &old_p_inode, const uint64_t &new_p_inode,
                       const uint64_t &inode, const std::string &new_name);
    Status rename_dir(const uint64_t &old_p_inode, const uint64_t &new_p_inode,
                      const uint64_t &inode, const std::string &new_name);

    Status setattr(const uint64_t &inode, const Attributes &attr);

    std::pair<Status, ChunksLocation> get_chunks(const uint64_t &inode);

  private:
    rocksdb::DB *db_; // RocksDB instance for metadata storage.
    rocksdb::ColumnFamilyHandle *cf_inode_;
    rocksdb::ColumnFamilyHandle *cf_dentry_;
    rocksdb::ColumnFamilyHandle *cf_nodes_;
    std::vector<std::string> storage_nodes_;
    std::string db_path_;

    uint64_t get_and_increment_counter();
    std::pair<std::vector<std::string>, std::vector<std::string>> get_random_nodes();
};
