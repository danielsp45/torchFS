#ifndef FILE_HANDLE_H
#define FILE_HANDLE_H

#include "attributes.pb.h"
#include "metadata.h"
#include "slice.h"
#include "status.h"
#include "util.h"
#include <atomic>
#include <cstdint>
#include <fcntl.h>
#include <memory>
#include <string>

class FileHandle {
  public:
    // Remove id parameter from constructor
    FileHandle(const uint64_t &p_inode, const uint64_t &inode,
               std::string logic_path, std::string mount_path,
               std::shared_ptr<MetadataStorage> metadata)
        : p_inode_(p_inode), inode_(inode), metadata_(metadata),
          logic_path_(logic_path), mount_path_(mount_path), fd_(-1) {}

    ~FileHandle() {}

    Status init();
    Status destroy();
    Status open(int flags, mode_t mode);
    Status open(int flags);
    Status close();
    Status read(Slice &dst, size_t size, off_t offset);
    Status write(Slice &dst, size_t size, off_t offset);
    Status getattr(struct stat *buf);
    Status sync();

    std::string get_logic_path() { return logic_path_; }
    std::string get_name() { return filename(logic_path_); }
    uint64_t get_inode() { return inode_; }
    void set_logic_path(const std::string &logic_path) {
        logic_path_ = logic_path;
    }
    void set_parent_inode(const uint64_t &p_inode) { p_inode_ = p_inode; }

  private:
    uint64_t p_inode_;       // Parent inode
    uint64_t inode_;         // Unique identifier for the file
    std::string logic_path_; // Logical path of the file
    std::string mount_path_; // Local file path for write operations
    std::shared_ptr<MetadataStorage> metadata_; // Metadata storage
    int fd_;                                    // File descriptor

    Status setattr(Attributes &attr);
};

#endif // FILE_HANDLE_H