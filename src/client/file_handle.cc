#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "file_handle.h"
#include "slice.h"
#include "util.h"

Status FileHandle::init() {
    if (inode_ == -1) {
        // we need to create the file in the metadata too
        auto [s, attr] =
            metadata_->create_file(p_inode_, filename(logic_path_));
        if (!s.ok()) {
            return s;
        }
        inode_ = attr.inode();
    }

    return Status::OK();
}

Status FileHandle::destroy() {
    // TODO: the file removal should be done only when there's no more open's

    auto s = metadata_->remove_file(p_inode_, inode_);
    if (!s.ok()) {
        return s;
    }

    std::string full_path = join_paths(mount_path_, logic_path_);
    if (::unlink(full_path.c_str()) == -1) {
        return Status::IOError("Failed to remove file: " +
                               std::string(strerror(errno)));
    }
    return Status::OK();
}

Status FileHandle::open(int flags, mode_t mode) {
    if (num_ > 0) {
        num_++;
        return Status::OK();
    } else {
        std::string full_path = join_paths(mount_path_, logic_path_);
        fd_ = ::open(full_path.c_str(), flags, mode);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
        return Status::OK();
    }
}

Status FileHandle::open(int flags) {
    if (num_ > 0) {
        num_++;
        return Status::OK();
    } else {
        std::string full_path = join_paths(mount_path_, logic_path_);
        fd_ = ::open(full_path.c_str(), flags);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
        return Status::OK();
    }
}

Status FileHandle::getattr(struct stat *buf) {
    // Get the file/directory attributes from the metadata service.
    std::cout << "getattr implementation" << std::endl;
    Attributes attr = metadata_->getattr(inode_);
    std::cout << "getattr implementation done" << std::endl;

    // Transfer the data from Attributes to the stat structure.
    // buf->st_mode = attr.mode(); // e.g., file type and permissions
    buf->st_mode = attr.mode();
    buf->st_size = attr.size();               // file size in bytes
    buf->st_atime = attr.access_time();       // last access time
    buf->st_mtime = attr.modification_time(); // last modification time
    buf->st_ctime = attr.creation_time();     // creation or inode change time
    buf->st_ino = attr.inode();               // inode number
    buf->st_uid = attr.user_id();             // user ID of owner
    buf->st_gid = attr.group_id();            // group ID of owner

    return Status::OK();
}

Status FileHandle::close() {
    if (num_ > 1) {
        num_--;
        return Status::OK();
    } else {
        if (fd_ != -1) {
            if (::close(fd_) == -1) {
                return Status::IOError("Failed to close file: " +
                                       std::string(strerror(errno)));
            }
            fd_ = -1;
        }
        return Status::OK();
    }
}

Status FileHandle::read(Slice &dst, size_t size, off_t offset) {
    if (fd_ == -1) {
        return Status::IOError("File not open");
    }
    ssize_t result = ::pread(fd_, dst.data(), size, offset);
    if (result == -1) {
        return Status::IOError("Failed to read file: " +
                               std::string(strerror(errno)));
    }
    return Status::OK();
}

Status FileHandle::write(Slice &src, size_t count, off_t offset) {
    if (fd_ == -1) {
        return Status::IOError("File not open");
    }
    ssize_t result = ::pwrite(fd_, src.data(), count, offset);
    if (result == -1) {
        return Status::IOError("Failed to write file: " +
                               std::string(strerror(errno)));
    }
    return Status::OK();
}

Status FileHandle::sync() {
    if (fd_ == -1) {
        return Status::IOError("File not open");
    }
    if (::fsync(fd_) == -1) {
        return Status::IOError("Failed to flush file: " +
                               std::string(strerror(errno)));
    }
    return Status::OK();
}
