#include "attributes.pb.h"

#include "rocksdb/db.h"
#include "status.h"
#include <cstdint>
#include <unordered_map>
#include <vector>

class MetadataStorage {
  public:
    MetadataStorage();
    ~MetadataStorage() = default;

    Attributes getattr(const uint64_t &inode);
    std::vector<Dirent> readdir(const uint64_t &inode);
    FileInfo open(const uint64_t &inode);

    Status create_file(const uint64_t &p_inode, const std::string &name);
    Status create_dir(const uint64_t &p_inode, const std::string &name);

    Status remove_file(const uint64_t &p_inode, const uint64_t &inode);
    Status remove_dir(const uint64_t &inode);

    Status rename_file(const uint64_t &old_p_inode, const uint64_t &new_p_inode,
                       const uint64_t &inode, const std::string &new_name);
    Status rename_dir(const uint64_t &old_p_inode, const uint64_t &new_p_inode,
                      const uint64_t &inode, const std::string &new_name);

  private:
    uint64_t next_inode_ = 1; // Simple inode allocator.
    rocksdb::DB *db_;         // RocksDB instance for metadata storage.
    rocksdb::ColumnFamilyHandle *cf_inode_;
    rocksdb::ColumnFamilyHandle *cf_dentry_;
    rocksdb::ColumnFamilyHandle *cf_nodes_;
};