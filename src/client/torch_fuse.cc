#include "torch_fuse.h"
#include "slice.h"
#include "status.h"
#include "storage_engine.h"

#include <fcntl.h>
#include <iostream>
#include <ostream>
#include <sys/types.h>

// Removed static for external linkage.
int torch_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                  off_t /*offset*/, struct fuse_file_info * /*fi*/,
                  fuse_readdir_flags flags) {
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->readdir(se->get_logic_path(path), buf, filler, flags);
    if (!s.ok()) {
        std::cerr << "Error reading directory: " << s.ToString() << std::endl;
        return -errno;
    }
    return 0;
}

// Removed static for external linkage.
int torch_getattr(const char *path, struct stat *stbuf,
                  struct fuse_file_info * /*fi*/) {
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

int torch_access(const char * /*path*/, int /*mask*/) {
    // INFO: We don't have a permission system in place
    // so we allow everyone
    return 0;
}

// Removed static for external linkage.
int torch_unlink(const char *path) {
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->unlink(se->get_logic_path(path));
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
int torch_mkdir(const char *path, mode_t /*mode*/) {
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    Status s = se->mkdir(se->get_logic_path(path));
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
int torch_rename(const char *from, const char *to, unsigned int /*flags*/) {
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
int torch_create(const char *path, mode_t /*mode*/, struct fuse_file_info *fi) {
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);

    FilePointer *fp = nullptr;
    Status s = se->create(se->get_logic_path(path));
    if (!s.ok()) {
        std::cerr << "Error creating file: " << s.ToString() << std::endl;
        return -errno;
    }

    s = se->open(se->get_logic_path(path), fi->flags, &fp);
    if (!s.ok()) {
        std::cerr << "Error opening file: " << s.ToString() << std::endl;
        return -errno;
    }

    fi->fh = reinterpret_cast<uint64_t>(fp);

    return 0;
}

int torch_open(const char *path, struct fuse_file_info *fi) {
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    FilePointer *fp = nullptr;
    Status s = se->open(se->get_logic_path(path), fi->flags, &fp);
    if (!s.ok()) {
        std::cerr << "Error opening file: " << s.ToString() << std::endl;
        return -errno;
    }

    if (fi == NULL) {
        std::cerr << "File info is null for path: " << path << std::endl;
        return -EINVAL; // Invalid argument
    }

    fi->fh = reinterpret_cast<uint64_t>(fp);

    return 0;
}

// Removed static for external linkage.
int torch_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi) {
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);

    FilePointer *fp;
	if(fi == NULL) {
        se->open(logic_path, fi->flags, &fp);
    } else {
        fp = reinterpret_cast<FilePointer *>(fi->fh);
    }

    Slice result(buf, size, false);
    Status s = se->read(fp, result, size, offset);
    if (!s.ok()) {
        std::cerr << "Error reading file: " << s.ToString() << std::endl;
        return -errno;
    }

    if (fi == NULL)
        se->close(fp);

    return result.size();
}

// Removed static for external linkage.
int torch_write(const char *path, const char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi) {

    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);

    FilePointer *fp;
	if(fi == NULL) {
        se->open(logic_path, fi->flags, &fp);
    } else {
        fp = reinterpret_cast<FilePointer *>(fi->fh);
    }

    Slice data(buf, size, false);
    Status s = se->write(fp, data, size, offset);
    if (!s.ok()) {
        std::cerr << "Error writing file: " << s.ToString() << std::endl;
        return -errno;
    }

    if (fi == NULL)
        se->close(fp);

    return size;
}

// Removed static for external linkage.
int torch_release(const char *path, struct fuse_file_info *fi) {

    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);

    FilePointer *fp;
    fp = reinterpret_cast<FilePointer *>(fi->fh);

    // Check if file is open
    Status status = se->close(fp);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        return -errno;
    }

    fi->fh = 0; // Reset file handle

    return 0;
}

// Removed static for external linkage.
int torch_fsync(const char *path, int /*isdatasync*/,
                struct fuse_file_info *fi) {
    (void)fi;
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);
    Status s = se->sync(logic_path);
    if (!s.ok()) {
        std::cerr << "Error syncing file: " << s.ToString() << std::endl;
        return -errno;
    }

    return 0;
}

int torch_utimens(const char *path, const struct timespec tv[2],
                  struct fuse_file_info *fi) {
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

off_t torch_lseek(const char *path, off_t offset, int whence,
                struct fuse_file_info *fi) {
    auto se = static_cast<StorageEngine *>(fuse_get_context()->private_data);
    std::string logic_path = se->get_logic_path(path);

    FilePointer *fp;
	if(fi == NULL) {
        se->open(logic_path, fi->flags, &fp);
    } else {
        fp = reinterpret_cast<FilePointer *>(fi->fh);
    }

    off_t new_offset;
    Status s = se->lseek(fp, offset, whence, &new_offset);

    if (fi == NULL)
        se->close(fp);

    return new_offset;
}