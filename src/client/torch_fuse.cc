#include "torch_fuse.h"
#include "slice.h"
#include "status.h"
#include "storage_engine.h"

#include <fcntl.h>
#include <iostream>
#include <sys/types.h>

// Removed static for external linkage.
int torch_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                  off_t offset, struct fuse_file_info *fi,
                  fuse_readdir_flags flags) {
    std::cout << "[LOG] readdir" << path << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->readdir(se->get_logic_path(path), buf, filler, flags);
    if (!s.ok()) {
        std::cerr << "Error reading directory: " << s.ToString() << std::endl;
        return -errno;
    }
    return 0;
}

// Removed static for external linkage.
off_t torch_lseek(const char *path, off_t offset, int whence,
                  struct fuse_file_info *fi) {
    return -ENOSYS;
}

// Removed static for external linkage.
int torch_getattr(const char *path, struct stat *stbuf,
                  struct fuse_file_info *fi) {
    std::cout << "[LOG] getattr " << path << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::cout << se->get_logic_path(path) << std::endl;
    Status s = se->getattr(se->get_logic_path(path), stbuf);

    if (s.is_not_found()) {
        std::cerr << "File or Directory not found: " << path << std::endl;
        return -ENOENT;
    } else if (s.is_invalid_argument()) {
        std::cerr << "Invalid argument: " << s.ToString() << std::endl;
        return -EINVAL;
    } else if (s.is_io_error()) {
        std::cerr << "IO error: " << s.ToString() << std::endl;
        return -EIO;
    } else if (!s.ok()) {
        std::cerr << "Error getting attributes: " << s.ToString() << std::endl;
        return -errno;
    }

    return 0;
}

// Removed static for external linkage.
int torch_access(const char *path, int mask) {
    return 0; // No access check needed
}

// Removed static for external linkage.
int torch_readlink(const char *path, char *buf, size_t size) { return -ENOSYS; }

// Removed static for external linkage.
int torch_mknod(const char *path, mode_t mode, dev_t rdev) { return -ENOSYS; }

// Removed static for external linkage.
int torch_unlink(const char *path) {
    std::cout << "[LOG] unlink: " << path << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->remove(se->get_logic_path(path));
    if (s.is_not_found()) {
        std::cerr << "File not found: " << path << std::endl;
        return -ENOENT;
    } else if (s.is_invalid_argument()) {
        std::cerr << "Invalid argument: " << s.ToString() << std::endl;
        return -EINVAL;
    } else if (s.is_io_error()) {
        std::cerr << "IO error: " << s.ToString() << std::endl;
        return -EIO;
    } else if (!s.ok()) {
        std::cerr << "Error unlinking file: " << s.ToString() << std::endl;
        return -errno;
    }
    return 0;
}

// Removed static for external linkage.
int torch_mkdir(const char *path, mode_t mode) {
    std::cout << "[LOG] mkdir " << path << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->mkdir(se->get_logic_path(path), mode);
    if (!s.ok()) {
        std::cerr << "Error creating directory: " << s.ToString() << std::endl;
        return -errno;
    }
    return 0;
}

// Removed static for external linkage.
int torch_rmdir(const char *path) {
    std::cout << "[LOG] rmdir " << path << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->rmdir(se->get_logic_path(path));
    if (!s.ok()) {
        std::cerr << "Error removing directory: " << s.ToString() << std::endl;
        return -errno;
    }
    return 0;
}

// Removed static for external linkage.
int torch_symlink(const char *from, const char *to) { return -ENOSYS; }

// Removed static for external linkage.
int torch_rename(const char *from, const char *to, unsigned int flags) {
    std::cout << "[LOG] rename" << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    std::string logic_from = se->get_logic_path(from);
    std::string logic_to = se->get_logic_path(to);
    Status s = se->rename(logic_from, logic_to);
    if (!s.ok()) {
        std::cerr << "Error renaming file: " << s.ToString() << std::endl;
        return -errno;
    }
    return 0;
}

// Removed static for external linkage.
int torch_link(const char *from, const char *to) { return -ENOSYS; }

// Removed static for external linkage.
int torch_chmod(const char *path, mode_t mode, struct fuse_file_info *fi) {
    return -ENOSYS;
}

// Removed static for external linkage.
int torch_chown(const char *path, uid_t uid, gid_t gid,
                struct fuse_file_info *fi) {
    return -ENOSYS;
}

// Removed static for external linkage.
int torch_truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    return -ENOSYS;
}

// Removed static for external linkage.
int torch_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    std::cout << "[LOG] create " << path << std::endl;
    (void)fi;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    Status s = se->create(se->get_logic_path(path), fi->flags, mode);
    if (!s.ok()) {
        std::cerr << "Error creating file: " << s.ToString() << std::endl;
        return -errno;
    }

    return 0;
}

int torch_open(const char *path, struct fuse_file_info *fi) {
    std::cout << "[LOG] open" << path << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    Status s = se->open(se->get_logic_path(path), fi->flags);
    if (!s.ok()) {
        std::cerr << "Error opening file: " << s.ToString() << std::endl;
        return -errno;
    }

    return 0;
}

// Removed static for external linkage.
int torch_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi) {
    std::cout << "[LOG] read " << path << std::endl;
    (void)fi;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);

    Slice result(buf, size, false);
    Status s = se->read(logic_path, result, size, offset);
    if (!s.ok()) {
        std::cerr << "Error reading file: " << s.ToString() << std::endl;
        return -errno;
    }

    return result.size();
}

// Removed static for external linkage.
int torch_write(const char *path, const char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi) {
    std::cout << "[LOG] write " << path << std::endl;
    (void)fi;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);

    Slice data(buf, size, false);
    Status s = se->write(logic_path, data, size, offset);
    if (!s.ok()) {
        std::cerr << "Error writing file: " << s.ToString() << std::endl;
        return -errno;
    }

    return size;
}

// Removed static for external linkage.
int torch_statfs(const char *path, struct statvfs *stbuf) { return -ENOSYS; }

// Removed static for external linkage.
int torch_release(const char *path, struct fuse_file_info *fi) {
    std::cout << "[LOG] release" << path << std::endl;
    (void)fi;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);

    // Check if file is open
    Status status = se->close(logic_path);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        return -errno;
    }

    return 0;
}

// Removed static for external linkage.
int torch_fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
    std::cout << "[LOG] fsync " << path << std::endl;
    (void)fi;
    // auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    // std::string logic_path = se->get_logic_path(path);

    // Status s = se->sync(se->get_logic_path(path));
    // if (!s.ok()) {
    //     std::cerr << "Error syncing file: " << s.ToString() << std::endl;
    //     return -errno;
    // }

    return 0;
}

int torch_utimens(const char *path, const struct timespec tv[2],
                  struct fuse_file_info *fi) {
    std::cout << "[LOG] utimens " << path << std::endl;
    (void)fi;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);
    Status s = se->utimens(logic_path, tv);
    if (!s.ok()) {
        std::cerr << "Error updating times: " << s.ToString() << std::endl;
        return -errno;
    }

    return 0;
}