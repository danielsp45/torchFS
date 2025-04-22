#ifndef FILE_HANDLE_H
#define FILE_HANDLE_H

#include "slice.h"
#include "status.h"
#include "util.h"
#include <atomic>
#include <fcntl.h>
#include <string>

class FileHandle {
  public:
    inline static std::atomic<uint64_t> id_counter{0};

    // Remove id parameter from constructor
    FileHandle(std::string logic_path, std::string mount_path)
        : logic_path_(logic_path), mount_path_(mount_path), fd_(-1), num_(0),
          id_(id_counter.fetch_add(1, std::memory_order_relaxed)) {}

    ~FileHandle() {}

    Status open(int flags, mode_t mode);
    Status open(int flags);
    Status close();
    Status read(Slice &dst, size_t size, off_t offset);
    Status write(Slice &dst, size_t size, off_t offset);
    Status remove();
    Status sync();

    std::string get_name() { return filename(logic_path_); }
    int get_num() { return num_; }
    uint64_t get_id() { return id_; }
    struct stat *get_meta();

  private:
    std::string logic_path_; // Logical path of the file
    std::string mount_path_; // Local file path for write operations
    int fd_;                 // File descriptor
    int num_;                // Number of open file handles
    uint64_t id_;            // Unique identifier for the file handle
};

#endif // FILE_HANDLE_H