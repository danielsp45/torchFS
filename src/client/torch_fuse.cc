#include "torch_fuse.h"
#include "slice.h"
#include "status.h"
#include "storage_engine.h"
#include "spdlog/spdlog.h"

#include <fcntl.h>
#include <iostream>
#include <sys/types.h>

// Updated all logging to use spdlog
int torch_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                  off_t offset, struct fuse_file_info *fi,
                  fuse_readdir_flags flags) {
    spdlog::info("torch_readdir: Reading directory: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->readdir(se->get_logic_path(path), buf, filler, flags);
    if (!s.ok()) {
        spdlog::error("torch_readdir: Error reading directory: {}", s.ToString());
        return -errno;
    }
    return 0;
}

off_t torch_lseek(const char *path, off_t offset, int whence,
                  struct fuse_file_info *fi) {
    return static_cast<off_t>(-ENOSYS);
}

int torch_getattr(const char *path, struct stat *stbuf,
                  struct fuse_file_info *fi) {
    spdlog::info("torch_getattr: Getting attributes for: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->getattr(se->get_logic_path(path), stbuf);

    if (s.is_not_found()) {
        spdlog::warn("torch_getattr: File or Directory not found: {}", path);
        return -ENOENT;
    } else if (s.is_invalid_argument()) {
        spdlog::warn("torch_getattr: Invalid argument: {}", s.ToString());
        return -EINVAL;
    } else if (s.is_io_error()) {
        spdlog::error("torch_getattr: IO error: {}", s.ToString());
        return -EIO;
    } else if (!s.ok()) {
        spdlog::error("torch_getattr: Error getting attributes: {}", s.ToString());
        return -errno;
    }

    return 0;
}

int torch_access(const char *path, int mask) { return -ENOSYS; }

int torch_readlink(const char *path, char *buf, size_t size) { return -ENOSYS; }

int torch_mknod(const char *path, mode_t mode, dev_t rdev) { return -ENOSYS; }

int torch_unlink(const char *path) {
    spdlog::info("torch_unlink: Unlinking file: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->remove(se->get_logic_path(path));
    if (s.is_not_found()) {
        spdlog::warn("torch_unlink: File not found: {}", path);
        return -ENOENT;
    } else if (s.is_invalid_argument()) {
        spdlog::warn("torch_unlink: Invalid argument: {}", s.ToString());
        return -EINVAL;
    } else if (s.is_io_error()) {
        spdlog::error("torch_unlink: IO error: {}", s.ToString());
        return -EIO;
    } else if (!s.ok()) {
        spdlog::error("torch_unlink: Error unlinking file: {}", s.ToString());
        return -errno;
    }
    return 0;
}

int torch_mkdir(const char *path, mode_t mode) {
    spdlog::info("torch_mkdir: Creating directory: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->mkdir(se->get_logic_path(path), mode);
    if (!s.ok()) {
        spdlog::error("torch_mkdir: Error creating directory: {}", s.ToString());
        return -errno;
    }
    return 0;
}

int torch_rmdir(const char *path) {
    spdlog::info("torch_rmdir: Removing directory: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->rmdir(se->get_logic_path(path));
    if (!s.ok()) {
        spdlog::error("torch_rmdir: Error removing directory: {}", s.ToString());
        return -errno;
    }
    return 0;
}

int torch_symlink(const char *from, const char *to) { return -ENOSYS; }

int torch_rename(const char *from, const char *to, unsigned int flags) {
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    std::string logic_from = se->get_logic_path(from);
    std::string logic_to = se->get_logic_path(to);
    Status s = se->rename(logic_from, logic_to);
    if (!s.ok()) {
        spdlog::error("torch_rename: Error renaming file: {}", s.ToString());
        return -errno;
    }
    return 0;
}

int torch_link(const char *from, const char *to) { return -ENOSYS; }

int torch_chmod(const char *path, mode_t mode, struct fuse_file_info *fi) {
    return -ENOSYS;
}

int torch_chown(const char *path, uid_t uid, gid_t gid,
                struct fuse_file_info *fi) {
    return -ENOSYS;
}

int torch_truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    return -ENOSYS;
}

int torch_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    spdlog::info("torch_create: Creating file: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    uint64_t handle;
    Status s = se->create(se->get_logic_path(path), fi->flags, mode, handle);
    if (!s.ok()) {
        spdlog::error("torch_create: Error creating file: {}", s.ToString());
        return -errno;
    }

    fi->fh = handle;
    return 0;
}

int torch_open(const char *path, struct fuse_file_info *fi) {
    spdlog::info("torch_open: Opening file: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    uint64_t handle;
    Status s = se->open(se->get_logic_path(path), fi->flags, handle);
    if (!s.ok()) {
        spdlog::error("torch_open: Error opening file: {}", s.ToString());
        return -errno;
    }

    fi->fh = handle;

    return 0;
}

int torch_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi) {
    spdlog::info("torch_read: Reading file: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    if (fi->fh == 0) {
        spdlog::error("torch_read: Invalid file handle for: {}", path);
        return -EINVAL;
    }

    Slice result(buf, size, false);
    Status s = se->read(fi->fh, result, size, offset);
    if (!s.ok()) {
        spdlog::error("torch_read: Error reading file: {}", s.ToString());
        return -errno;
    }

    return result.size();
}

int torch_write(const char *path, const char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi) {
    spdlog::info("torch_write: Writing to file: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    if (fi->fh == 0) {
        spdlog::error("torch_write: Invalid file handle for: {}", path);
        return -EINVAL;
    }

    Slice data(buf, size, false);
    Status s = se->write(fi->fh, data, size, offset);
    if (!s.ok()) {
        spdlog::error("torch_write: Error writing to file: {}", s.ToString());
        return -errno;
    }

    return size;
}

int torch_statfs(const char *path, struct statvfs *stbuf) { return -ENOSYS; }

int torch_release(const char *path, struct fuse_file_info *fi) {
    spdlog::info("torch_release: Releasing file: {}", path);
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    if (fi->fh == 0) {
        spdlog::error("torch_release: Invalid file handle for: {}", path);
        return -EINVAL;
    }

    se->close(fi->fh);

    fi->fh = 0;
    return 0;
}

int torch_fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
    (void)path;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    if (fi->fh == 0) {
        spdlog::error("torch_fsync: Invalid file handle for: {}", path);
        return -EINVAL;
    }

    Status s = se->sync(se->get_logic_path(path));
    if (!s.ok()) {
        spdlog::error("torch_fsync: Error syncing file: {}", s.ToString());
        return -errno;
    }

    return 0;
}
