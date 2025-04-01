#ifndef FILE_HANDLE_H
#define FILE_HANDLE_H

#include <string>

#include "status.h"

struct FileMetadata {
  std::string path; // File path (for reference)
  mode_t mode;      // File mode (permissions and type)
  uid_t uid;        // Owner user ID
  gid_t gid;        // Owner group ID
  off_t size;       // File size in bytes
  time_t atime;     // Last access time
  time_t mtime;     // Last modification time
  time_t ctime;     // Last status change time

  FileMetadata()
      : mode(0), uid(0), gid(0), size(0), atime(0), mtime(0), ctime(0) {}
};

class FileHandle {
public:
  FileHandle(std::string path) {}
  virtual ~FileHandle() = default;

  virtual Status read(void *buf, size_t count, off_t offset,
                      size_t *bytesRead) = 0;

  virtual Status write(const void *buf, size_t count, off_t offset,
                       size_t *bytesWritten) = 0;

  virtual Status remove() = 0;

  virtual Status rename(const std::string &newPath) = 0;

  virtual Status get_fd() = 0;

private:
  FileMetadata metadata_;
};

#endif // FILE_HANDLE_H