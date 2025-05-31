#ifndef FILE_HANDLE_H
#define FILE_HANDLE_H

#include "metadata.pb.h"
#include "metadata_client.h"
#include "slice.h"
#include "status.h"
#include "storage_client.h"
#include "util.h"
#include <fcntl.h>
#include <memory>
#include <string>
#include <sys/stat.h>

struct FilePointer;

class FileHandle : public std::enable_shared_from_this<FileHandle> {
  public:
    // Remove id parameter from constructor
    FileHandle(const uint64_t &p_inode, const uint64_t &inode,
               std::string logic_path, std::string mount_path,
               std::shared_ptr<MetadataClient> metadata,
               std::shared_ptr<StorageClient> storage)
        : p_inode_(p_inode), inode_(inode), logic_path_(logic_path),
          mount_path_(mount_path), metadata_(metadata), storage_(storage),
          file_pointers_(), unlink_(false), cached_(false), fetched_(false), written_(false) {}

    ~FileHandle() {}

    Status init();
    Status destroy();
    // Status open(int flags);
    Status open(FilePointer **fp, int flags); 
    Status close(FilePointer *fp);
    Status read(FilePointer *fp, Slice &dst, size_t size, off_t offset);
    Status write(FilePointer *fp, Slice &src, size_t count, off_t offset);
    Status getattr(struct stat *buf);
    Status utimens(const struct timespec tv[2]);
    Status sync();

    std::string get_logic_path() { return logic_path_; }
    std::string get_name() { return filename(logic_path_); }
    uint64_t get_inode() { return inode_; }
    void set_logic_path(const std::string &logic_path) {
        logic_path_ = logic_path;
    }
    void set_parent_inode(const uint64_t &p_inode) { p_inode_ = p_inode; }
    bool is_unlinked() const { return unlink_ && file_pointers_.empty(); }

    void unlink() {unlink_ = true;};

    void cache();
    void uncache();

  private:
    uint64_t p_inode_;       // Parent inode
    uint64_t inode_;         // Unique identifier for the file
    std::string logic_path_; // Logical path of the file
    std::string mount_path_; // Local file path for write operations
    std::shared_ptr<MetadataClient> metadata_; // Metadata storage
    std::shared_ptr<StorageClient> storage_;   // Storage client
    std::vector<std::unique_ptr<FilePointer>> file_pointers_; // File pointers
    Attributes attributes_;
    bool unlink_;
    bool cached_;
    bool fetched_;
    bool written_;

    Status setattr(Attributes &attr);
    Status flush();
    Status fetch();
    Status remove_local();


  void stat_to_attr(const struct stat &st, Attributes &a);
  void attr_to_stat(const Attributes &a, struct stat *st);
};

struct FilePointer {
    std::shared_ptr<FileHandle> fh;    
    int fd;          // already-open FD

    // Constructor: opens the local file and hydrates it
    FilePointer(std::shared_ptr<FileHandle> fh)
      : fh(std::move(fh)), fd(-1)
    {}

    ~FilePointer() = default;

    int open(std::string &path, int open_flags) {
      fd = ::open(path.c_str(), open_flags, 0644);
      return fd;
    }
    
    int close() {
      if (fd >= 0) {
        int ret = ::close(fd);
        fd = -1; // Reset fd after closing
        return ret;
      }
      return 0; // Already closed
    }

    ssize_t read(Slice &dst, size_t size, off_t offset) {
      if (fd < 0) {
        return -1; // Invalid file descriptor
      }

      ssize_t bytes_read = ::pread(fd, dst.data(), size, offset);
      if (bytes_read < 0) {
        return -1; // Read error
      }

      return bytes_read;
    }

    ssize_t write(Slice &src, size_t size, off_t offset) {
      if (fd < 0) {
        return -1; // Invalid file descriptor
      }

      ssize_t bytes_written = ::pwrite(fd, src.data(), size, offset);
      if (bytes_written < 0) {
        return -1; // Write error
      }

      return bytes_written;
    }

    int fstat(struct stat *buf) {
      if (fd < 0) {
        return -1; // Invalid file descriptor
      }

      return ::fstat(fd, buf);
    }
};
#endif // FILE_HANDLE_H