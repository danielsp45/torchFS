#ifndef TORCH_FUSE_H
#define TORCH_FUSE_H

#include <fuse.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

// Removed static for external linkage.
int torch_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                  off_t offset, struct fuse_file_info *fi,
                  fuse_readdir_flags flags);
int torch_getattr(const char *path, struct stat *stbuf,
                  struct fuse_file_info *fi);
int torch_access(const char *path, int mask);
int torch_unlink(const char *path);
int torch_mkdir(const char *path, mode_t mode);
int torch_rmdir(const char *path);
int torch_rename(const char *from, const char *to, unsigned int flags);
int torch_create(const char *path, mode_t mode, struct fuse_file_info *fi);
int torch_open(const char *path, struct fuse_file_info *fi);
int torch_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi);
int torch_write(const char *path, const char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi);
int torch_release(const char *path, struct fuse_file_info *fi);
int torch_fsync(const char *path, int isdatasync, struct fuse_file_info *fi);
int torch_utimens(const char *path, const struct timespec tv[2],
                  struct fuse_file_info *fi);
off_t torch_lseek(const char *path, off_t offset, int whence,
                  struct fuse_file_info *fi);

static const struct fuse_operations torch_oper = {.getattr = torch_getattr,
                                                  .readlink = NULL,
                                                  .mknod = NULL,
                                                  .mkdir = torch_mkdir,
                                                  .unlink = torch_unlink,
                                                  .rmdir = torch_rmdir,
                                                  .symlink = NULL,
                                                  .rename = torch_rename,
                                                  .link = NULL,
                                                  .chmod = NULL,
                                                  .chown = NULL,
                                                  .truncate = NULL,
                                                  .open = torch_open,
                                                  .read = torch_read,
                                                  .write = torch_write,
                                                  .statfs = NULL,
                                                  .flush = NULL,
                                                  .release = torch_release,
                                                  .fsync = torch_fsync,
                                                  .setxattr = NULL,
                                                  .getxattr = NULL,
                                                  .listxattr = NULL,
                                                  .removexattr = NULL,
                                                  .opendir = NULL,
                                                  .readdir = torch_readdir,
                                                  .releasedir = NULL,
                                                  .fsyncdir = NULL,
                                                  .init = NULL,
                                                  .destroy = NULL,
                                                  .access = torch_access,
                                                  .create = torch_create,
                                                  .lock = NULL,
                                                  .utimens = torch_utimens,
                                                  .bmap = NULL,
                                                  .ioctl = NULL,
                                                  .poll = NULL,
                                                  .write_buf = NULL,
                                                  .read_buf = NULL,
                                                  .flock = NULL,
                                                  .fallocate = NULL,
                                                  .copy_file_range = NULL,
                                                  .lseek = torch_lseek};
#endif // TORCH_FUSE_H