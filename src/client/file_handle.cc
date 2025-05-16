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

    auto s = metadata_->remove_file(p_inode_, inode_, filename(logic_path_));
    if (!s.ok()) {
        return s;
    }

    return storage_->remove_file(std::to_string(inode_));
}

Status FileHandle::open(int flags, mode_t mode) {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);

    // if the file doesn't exist, create it
    if (::access(path.c_str(), F_OK) == -1) {
        // File does not exist, create it
        fd_ = ::open(path.c_str(), flags | O_CREAT, 0644);
        if (fd_ == -1) {
            return Status::IOError("Failed to create file: " +
                                   std::string(strerror(errno)));
        }
    } else {
        // File exists, open it
        fd_ = ::open(path.c_str(), flags);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
    }

    return Status::OK();
}

Status FileHandle::open(int flags) {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    // if the file doesn't exist, create it
    if (::access(path.c_str(), F_OK) == -1) {
        // File does not exist, create it
        fd_ = ::open(path.c_str(), flags | O_CREAT, 0644);
        if (fd_ == -1) {
            return Status::IOError("Failed to create file: " +
                                   std::string(strerror(errno)));
        }
    } else {
        // File exists, open it
        fd_ = ::open(path.c_str(), flags);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
    }
    return Status::OK();
}

Status FileHandle::getattr(struct stat *buf) {
    // Get the file/directory attributes from the metadata service.
    auto [s, attr] = metadata_->getattr(inode_);
    if (!s.ok()) {
        return s;
    }

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

Status FileHandle::utimens(const struct timespec tv[2]) {
    // Update the file access and modification times.
    Attributes attr = metadata_->getattr(inode_).second;
    attr.set_access_time(tv[0].tv_sec);
    attr.set_modification_time(tv[1].tv_sec);

    Status s = setattr(attr);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

Status FileHandle::close() {
    if (fd_ != -1) {
        if (::close(fd_) == -1) {
            return Status::IOError("Failed to close file: " +
                                   std::string(strerror(errno)));
        }
        fd_ = -1;
    }

    return Status::OK();
}

Status FileHandle::read(Slice &dst, size_t size, off_t offset) {
    if (fd_ == -1) {
        std::string inode_str = std::to_string(inode_);
        std::string path = join_paths(mount_path_, inode_str);
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " + std::string(strerror(errno)));
        }
    }

    std::string file_id = std::to_string(inode_);
    auto [st, data] = storage_->read(file_id, offset, size);
    if (!st.ok()) {
        return st;
    }

    // Assuming it's impossible to return more bytes than size argument
    memcpy(dst.data(), data.payload().c_str(), data.len());
    return Status::OK();
}

Status FileHandle::write(Slice &src, size_t count, off_t offset) {
    if (fd_ == -1) {
        std::string inode_str = std::to_string(inode_);
        std::string path = join_paths(mount_path_, inode_str);
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " + std::string(strerror(errno)));
        }
    }
    
    std::string file_id = std::to_string(inode_);
    Data data;
    data.set_payload(src.data());
    data.set_len(count);

    auto [st, bytes_written] = storage_->write(file_id, data, offset);
    if (!st.ok()) {
        return st;
    }
    
    // Update metadata
    Attributes attr = metadata_->getattr(inode_).second;
    struct stat stat;
    if (::fstat(fd_, &stat) == -1) {
        return Status::IOError("Failed to get file size: " + std::string(strerror(errno)));
    }
    uint64_t new_size = offset + bytes_written;
    uint64_t updated_size = std::max(attr.size(), new_size);
    attr.set_size(updated_size);
    
    Status s = setattr(attr);
    if (!s.ok()) {
        return s;
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

Status FileHandle::setattr(Attributes &attr) {
    // Update the file attributes in the metadata service.
    auto s = metadata_->setattr(inode_, attr);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}
