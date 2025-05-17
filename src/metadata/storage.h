#pragma once

#include "metadata.pb.h"

#include "rocksdb/db.h"
#include "status.h"
#include <cstdint>
#include <vector>

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

  private:
    rocksdb::DB *db_; // RocksDB instance for metadata storage.
    rocksdb::ColumnFamilyHandle *cf_inode_;
    rocksdb::ColumnFamilyHandle *cf_dentry_;
    rocksdb::ColumnFamilyHandle *cf_nodes_;
    std::string db_path_;

    uint64_t get_and_increment_counter();

    // Write/delete helpers for inode CF
    Status put_inode(uint64_t inode, const std::string &value);
    Status delete_inode(uint64_t inode);

    // Write/delete helpers for dentry CF
    Status put_dirent(uint64_t parent_inode, const std::string &name,
                      const std::string &value);
    Status delete_dirent(uint64_t parent_inode, const std::string &name);

    // Read helpers
    Status get_inode(uint64_t inode, std::string &value);
    Status get_dirent(uint64_t parent_inode, const std::string &name,
                      std::string &value);
};