#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "attributes.pb.h"
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

    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    if (::access(path.c_str(), F_OK) == -1) {
        // if the file doesn't exist, we don't need to remove it from the disk
        std::cout << "File does not exist, no need to remove it from disk."
                  << std::endl;
        return Status::OK();
    }

    if (::unlink(path.c_str()) == -1) {
        return Status::IOError("Failed to remove file: " +
                               std::string(strerror(errno)));
    }
    std::cout << "File removed from disk: inode -> " << inode_ << std::endl;

    return Status::OK();
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
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
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
        std::string inode_str = std::to_string(inode_);
        std::string path = join_paths(mount_path_, inode_str);
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
    }
    ssize_t result = ::pwrite(fd_, src.data(), count, offset);
    if (result == -1) {
        return Status::IOError("Failed to write file: " +
                               std::string(strerror(errno)));
    }

    Attributes attr = metadata_->getattr(inode_).second;
    // update the file size and modification time
    attr.set_size(attr.size() + count);
    attr.set_modification_time(time(nullptr));
    attr.set_access_time(time(nullptr));

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