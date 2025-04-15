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
    std::cout << "[LOG] readdir" << std::endl;
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
    (void)path;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    // Return error if no valid file handle is present
    if (fi->fh == 0) {
        std::cerr << "Error: File handle not valid in read." << std::endl;
        return -EINVAL;
    }

    Slice result(buf, size, false);
    Status s = se->read(fi->fh, result, size, offset);
    if (!s.ok()) {
        std::cerr << "Error reading file: " << s.ToString() << std::endl;
        return -errno;
    }

    return result.size();
}

// Removed static for external linkage.
int torch_getattr(const char *path, struct stat *stbuf,
                  struct fuse_file_info *fi) {
    std::cout << "[LOG] getattr" << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
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
int torch_access(const char *path, int mask) { return -ENOSYS; }

// Removed static for external linkage.
int torch_readlink(const char *path, char *buf, size_t size) { return -ENOSYS; }

// Removed static for external linkage.
int torch_mknod(const char *path, mode_t mode, dev_t rdev) { return -ENOSYS; }

// Removed static for external linkage.
int torch_unlink(const char *path) {
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
    std::cout << "[LOG] create" << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    uint64_t handle;
    Status s = se->create(se->get_logic_path(path), fi->flags, mode, handle);
    if (!s.ok()) {
        std::cerr << "Error creating file: " << s.ToString() << std::endl;
        return -errno;
    }

    fi->fh = handle;
    return 0;
}

// Removed static for external linkage.
int torch_open(const char *path, struct fuse_file_info *fi) {
    std::cout << "[LOG] open" << std::endl;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    uint64_t handle;
    Status s = se->open(se->get_logic_path(path), fi->flags, handle);
    if (!s.ok()) {
        std::cerr << "Error opening file: " << s.ToString() << std::endl;
        return -errno;
    }

    fi->fh = handle;

    return 0;
}

// Removed static for external linkage.
int torch_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi) {
    (void)path;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    // Return error if no valid file handle is present
    if (fi->fh == 0) {
        std::cerr << "Error: File handle not valid in read." << std::endl;
        return -EINVAL;
    }

    Slice result(buf, size, false);
    Status s = se->read(fi->fh, result, size, offset);
    if (!s.ok()) {
        std::cerr << "Error reading file: " << s.ToString() << std::endl;
        return -errno;
    }

    return result.size();
}

// Removed static for external linkage.
int torch_write(const char *path, const char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi) {
    (void)path;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    if (fi->fh == 0) {
        // the file is not opened yet
        std::cerr << "Error: File handle not valid in read." << std::endl;
        return -EINVAL;
    }

    Slice data(buf, size, false);
    Status s = se->write(fi->fh, data, size, offset);
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
    std::cout << "[LOG] release" << std::endl;
    (void)path;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    if (fi->fh == 0) {
        std::cerr << "File handle is null" << std::endl;
        return -EINVAL;
    }

    se->close(fi->fh);

    fi->fh = 0; // Reset the file handle to avoid double free
    return 0;
}

// Removed static for external linkage.
int torch_fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
    (void)path;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    if (fi->fh == 0) {
        std::cerr << "File handle is null" << std::endl;
        return -EINVAL;
    }

    Status s = se->sync(se->get_logic_path(path));
    if (!s.ok()) {
        std::cerr << "Error syncing file: " << s.ToString() << std::endl;
        return -errno;
    }

    return 0;
}
