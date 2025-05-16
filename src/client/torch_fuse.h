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
off_t torch_lseek(const char *path, off_t offset, int whence,
                  struct fuse_file_info *fi);
int torch_getattr(const char *path, struct stat *stbuf,
                  struct fuse_file_info *fi);
int torch_access(const char *path, int mask);
int torch_readlink(const char *path, char *buf, size_t size);
int torch_mknod(const char *path, mode_t mode, dev_t rdev);
int torch_unlink(const char *path);
int torch_mkdir(const char *path, mode_t mode);
int torch_rmdir(const char *path);
int torch_symlink(const char *from, const char *to);
int torch_rename(const char *from, const char *to, unsigned int flags);
int torch_link(const char *from, const char *to);
int torch_chmod(const char *path, mode_t mode, struct fuse_file_info *fi);
int torch_chown(const char *path, uid_t uid, gid_t gid,
                struct fuse_file_info *fi);
int torch_truncate(const char *path, off_t size, struct fuse_file_info *fi);
int torch_create(const char *path, mode_t mode, struct fuse_file_info *fi);
int torch_open(const char *path, struct fuse_file_info *fi);
int torch_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi);
int torch_write(const char *path, const char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi);
int torch_statfs(const char *path, struct statvfs *stbuf);
int torch_release(const char *path, struct fuse_file_info *fi);
int torch_fsync(const char *path, int isdatasync, struct fuse_file_info *fi);
int torch_utimens(const char *path, const struct timespec tv[2],
                  struct fuse_file_info *fi);

static const struct fuse_operations torch_oper = {
    .getattr = torch_getattr,
    .readlink = torch_readlink,
    .mknod = torch_mknod,
    .mkdir = torch_mkdir,
    .unlink = torch_unlink,
    .rmdir = torch_rmdir,
    .symlink = torch_symlink,
    .rename = torch_rename,
    .link = torch_link,
    .chmod = torch_chmod,
    .chown = torch_chown,
    .truncate = torch_truncate,
    .open = torch_open,
    .read = torch_read,
    .write = torch_write,
    .statfs = torch_statfs,
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
    //.ftruncate = NULL,
    .lock = NULL,
    .utimens = torch_utimens,
    .bmap = NULL,
    .ioctl = NULL,
    .poll = NULL,
    .write_buf = NULL,
    .read_buf = NULL,
    .flock = NULL,
    .fallocate = NULL,
    .lseek = torch_lseek
};

#endif // TORCH_FUSE_H