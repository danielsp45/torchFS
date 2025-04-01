#ifndef TORCH_FUSE_H
#define TORCH_FUSE_H

#include "config.h"
#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <string>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include "options.h"
#include "status.h"

#define FUSE_FILL_DIR_PLUS 0

class TorchFuse {
public:
  // Constructor and destructor
  TorchFuse(Options *options);
  ~TorchFuse();

  Status init();

  // Filesystem operation methods corresponding to FUSE callbacks
  int getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi);
  int access(const char *path, int mask);
  int readlink(const char *path, char *buf, size_t size);
  int readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
              struct fuse_file_info *fi);
  int mknod(const char *path, mode_t mode, dev_t rdev);
  int unlink(const char *path);
  int mkdir(const char *path, mode_t mode);
  int rmdir(const char *path);
  int symlink(const char *from, const char *to);
  int rename(const char *from, const char *to, unsigned int flags);
  int link(const char *from, const char *to);
  int chmod(const char *path, mode_t mode, struct fuse_file_info *fi);
  int chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi);
  int truncate(const char *path, off_t size, struct fuse_file_info *fi);
  int create(const char *path, mode_t mode, struct fuse_file_info *fi);
  int open(const char *path, struct fuse_file_info *fi);
  int read(const char *path, char *buf, size_t size, off_t offset,
           struct fuse_file_info *fi);
  int write(const char *path, const char *buf, size_t size, off_t offset,
            struct fuse_file_info *fi);
  int statfs(const char *path, struct statvfs *stbuf);
  int release(const char *path, struct fuse_file_info *fi);
  int fsync(const char *path, int isdatasync, struct fuse_file_info *fi);
  int lseek(const char *path, off_t offset, int whence,
            struct fuse_file_info *fi);
  // int fallocate(const char* path, int mode, off_t offset, off_t length,
  // struct fuse_file_info* fi);

private:
  std::string root_;
};

#endif // TORCH_FUSE_H
