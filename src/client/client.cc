#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "TorchFuse.h"

// Updated wrapper for readdir: now has an extra parameter for flags.
static int torchfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                           off_t offset, struct fuse_file_info *fi,
                           fuse_readdir_flags flags) {
  (void)flags; // If you don't need to use flags, you can ignore it.
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  // Make sure that TorchFuse::readdir is updated to either ignore the flags
  // parameter or you adapt it to accept the extra parameter.
  return fs->readdir(path, buf, filler, offset, fi);
}

// Updated wrapper for lseek: now returns off_t.
static off_t torchfs_lseek(const char *path, off_t offset, int whence,
                           struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->lseek(path, offset, whence, fi);
}

// Other wrappers remain the same
static int torchfs_getattr(const char *path, struct stat *stbuf,
                           struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->getattr(path, stbuf, fi);
}

static int torchfs_access(const char *path, int mask) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->access(path, mask);
}

static int torchfs_readlink(const char *path, char *buf, size_t size) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->readlink(path, buf, size);
}

static int torchfs_mknod(const char *path, mode_t mode, dev_t rdev) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->mknod(path, mode, rdev);
}

static int torchfs_unlink(const char *path) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->unlink(path);
}

static int torchfs_mkdir(const char *path, mode_t mode) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->mkdir(path, mode);
}

static int torchfs_rmdir(const char *path) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->rmdir(path);
}

static int torchfs_symlink(const char *from, const char *to) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->symlink(from, to);
}

static int torchfs_rename(const char *from, const char *to,
                          unsigned int flags) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->rename(from, to, flags);
}

static int torchfs_link(const char *from, const char *to) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->link(from, to);
}

static int torchfs_chmod(const char *path, mode_t mode,
                         struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->chmod(path, mode, fi);
}

static int torchfs_chown(const char *path, uid_t uid, gid_t gid,
                         struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->chown(path, uid, gid, fi);
}

static int torchfs_truncate(const char *path, off_t size,
                            struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->truncate(path, size, fi);
}

static int torchfs_create(const char *path, mode_t mode,
                          struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->create(path, mode, fi);
}

static int torchfs_open(const char *path, struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->open(path, fi);
}

static int torchfs_read(const char *path, char *buf, size_t size, off_t offset,
                        struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->read(path, buf, size, offset, fi);
}

static int torchfs_write(const char *path, const char *buf, size_t size,
                         off_t offset, struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->write(path, buf, size, offset, fi);
}

static int torchfs_statfs(const char *path, struct statvfs *stbuf) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->statfs(path, stbuf);
}

static int torchfs_release(const char *path, struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->release(path, fi);
}

static int torchfs_fsync(const char *path, int isdatasync,
                         struct fuse_file_info *fi) {
  TorchFuse *fs = static_cast<TorchFuse *>(fuse_get_context()->private_data);
  return fs->fsync(path, isdatasync, fi);
}

static const struct fuse_operations torchfs_oper = {
    .getattr = torchfs_getattr,
    .readlink = torchfs_readlink,
    .mknod = torchfs_mknod,
    .mkdir = torchfs_mkdir,
    .unlink = torchfs_unlink,
    .rmdir = torchfs_rmdir,
    .symlink = torchfs_symlink,
    .rename = torchfs_rename,
    .link = torchfs_link,
    .chmod = torchfs_chmod,
    .chown = torchfs_chown,
    .truncate = torchfs_truncate,
    .open = torchfs_open,
    .read = torchfs_read,
    .write = torchfs_write,
    .statfs = torchfs_statfs,
    .flush = NULL,
    .release = torchfs_release,
    .fsync = torchfs_fsync,
    .setxattr = NULL,
    .getxattr = NULL,
    .listxattr = NULL,
    .removexattr = NULL,
    .opendir = NULL,
    .readdir = torchfs_readdir, // Now matches the 6-parameter signature.
    .releasedir = NULL,
    .fsyncdir = NULL,
    .init = NULL,
    .destroy = NULL,
    .access = torchfs_access,
    .create = torchfs_create,
    //.ftruncate = NULL,         // Removed since field no longer exists.
    .lock = NULL,
    .utimens = NULL,
    .bmap = NULL,
    .ioctl = NULL,
    .poll = NULL,
    .write_buf = NULL,
    .read_buf = NULL,
    .flock = NULL,
    .fallocate = NULL,
    .lseek = torchfs_lseek // Now returns off_t.
};

int main(int argc, char *argv[]) {
  enum { MAX_ARGS = 10 };
  int i, new_argc;
  char *new_argv[MAX_ARGS];

  umask(0);
  int fill_dir_plus;
  for (i = 0, new_argc = 0; (i < argc) && (new_argc < MAX_ARGS); i++) {
    if (!strcmp(argv[i], "--plus")) {
      fill_dir_plus = FUSE_FILL_DIR_PLUS;
    } else {
      new_argv[new_argc++] = argv[i];
    }
  }

  // Create your TorchFuse instance with the desired mount directory.
  TorchFuse *fs = new TorchFuse("/mnt/fs");

  // Call fuse_main with the modified arguments and fs as private_data.
  int ret = fuse_main(new_argc, new_argv, &torchfs_oper, fs);

  delete fs;
  return ret;
}
