#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "file_handle.h"
#include "slice.h"
#include "util.h"

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

Status FileHandle::remove() {
    // TODO: the file removal should be done only when there's no more open's
    std::string full_path = join_paths(mount_path_, logic_path_);
    if (::unlink(full_path.c_str()) == -1) {
        return Status::IOError("Failed to remove file: " +
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

struct stat *FileHandle::get_meta() {
    // TODO: this method should either read from the cached metadata or
    // request the metadata server for the file.
    // Example:
    // metadata_service_->get_file(inode_);
    struct stat *buf = new struct stat();
    std::string full_path = join_paths(mount_path_, logic_path_);
    if (::stat(full_path.c_str(), buf) == -1) {
        delete buf;
        return nullptr;
    }
    return buf;
}
