#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "file_handle.h"
#include "metadata.pb.h"
#include "slice.h"
#include "util.h"

Status FileHandle::init() {
    if (inode_ == static_cast<uint64_t>(-1)) {
        // we need to create the file in the metadata too
        auto [s, attr] =
            metadata_->create_file(p_inode_, filename(logic_path_));
        if (!s.ok()) {
            return s;
        }
        inode_ = attr.inode();
    
        open(O_RDWR | O_CREAT);
        close();
    }
    return Status::OK();
}

Status FileHandle::destroy() {
    // TODO: the file removal should be done only when there's no more open's

    auto s = metadata_->remove_file(p_inode_, inode_, filename(logic_path_));
    if (!s.ok()) {
        return s;
    }

    // std::vector<std::string> chunk_nodes{
    //     "node1",
    //     "node2",
    //     "node3",
    //     "node4",
    // };

    // std::vector<std::string> parity_nodes{
    //     "node5",
    //     "node6",
    // };

    // storage_->remove_file(chunk_nodes, parity_nodes, std::to_string(inode_));

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

// Status FileHandle::open(int flags) {
//     std::string inode_str = std::to_string(inode_);
//     std::string path = join_paths(mount_path_, inode_str);

//     fd_ = ::open(path.c_str(), flags | O_CREAT, 0666);

//     // // Retrieve the file from remote storage
//     // std::vector<std::string> chunk_nodes{
//     //     "node1",
//     //     "node2",
//     //     "node3",
//     //     "node4",
//     // };

//     // std::vector<std::string> parity_nodes{
//     //     "node5",
//     //     "node6",
//     // };

//     // auto [st_attr, attr] = metadata_->getattr(inode_);
//     // if (!st_attr.ok()) {
//     //     return st_attr;
//     // }
//     // if (attr.size() > 0) {
//     //     std::string file_id = std::to_string(inode_);
//     //     auto [st, data] = storage_->read(chunk_nodes, parity_nodes,
//     file_id);
//     //     if (!st.ok()) {
//     //         return st;
//     //     }
//     //     ssize_t written = ::write(fd_, data.payload().c_str(),
//     data.len());
//     //     if (written < 0 || static_cast<size_t>(written) != data.len()) {
//     //         return Status::IOError(
//     //             "Failed to write remote data to local file: " +
//     //             std::string(strerror(errno)));
//     //     }
//     // }

//     return Status::OK();
// }

Status FileHandle::getattr(struct stat *buf) {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);

    if (::stat(path.c_str(), buf) == -1) {
        return Status::IOError("Failed to get attributes from local filesystem: " +
                               std::string(strerror(errno)));
    }
    return Status::OK();
}
// Status FileHandle::getattr(struct stat *buf) {
//     // Get the file/directory attributes from the metadata service.
//     auto [s, attr] = metadata_->getattr(inode_);
//     if (!s.ok()) {
//         return s;
//     }

//     buf->st_mode = attr.mode();
//     buf->st_size = attr.size();               // file size in bytes
//     buf->st_atime = attr.access_time();       // last access time
//     buf->st_mtime = attr.modification_time(); // last modification time
//     buf->st_ctime = attr.creation_time();     // creation or inode change time
//     buf->st_ino = attr.inode();               // inode number
//     buf->st_uid = attr.user_id();             // user ID of owner
//     buf->st_gid = attr.group_id();            // group ID of owner

//     return Status::OK();
// }

Status FileHandle::utimens(const struct timespec tv[2]) {
    // Update the file access and modification times in metadata.
    Attributes attr = metadata_->getattr(inode_).second;
    attr.set_access_time(tv[0].tv_sec);
    attr.set_modification_time(tv[1].tv_sec);

    Status s = setattr(attr);
    if (!s.ok()) {
        return s;
    }

    // Also update the local file on disk.
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    if (::utimensat(AT_FDCWD, path.c_str(), tv, 0) == -1) {
        return Status::IOError("Failed to update file timestamps: " +
                               std::string(strerror(errno)));
    }

    return Status::OK();
}

Status FileHandle::close() {
    if (fd_ != -1) {
        // 1) Ensure all data is on disk
        if (::fsync(fd_) < 0) {
            return Status::IOError("fsync failed: " + std::string(strerror(errno)));
        }

        // 2) Get the true file size
        struct stat st;
        if (::fstat(fd_, &st) < 0) {
            return Status::IOError("fstat failed: " + std::string(strerror(errno)));
        }

        // 3) Truncate any old tail beyond st.st_size
        if (::ftruncate(fd_, st.st_size) < 0) {
            return Status::IOError("ftruncate failed: " + std::string(strerror(errno)));
        }

        // 4) Rewind so that future reads start at byte 0
        if (::lseek(fd_, 0, SEEK_SET) < 0) {
            return Status::IOError("lseek failed: " + std::string(strerror(errno)));
        }

        // 5) Close the descriptor
        ::close(fd_);
        fd_ = -1;
    }
    return Status::OK();
}


Status FileHandle::read(Slice &dst, size_t size, off_t offset) {
    std::cout << "[INFO] read " << logic_path_ << " offset: " << offset
              << " size: " << size << std::endl;

    if (fd_ == -1) {
        std::string inode_str = std::to_string(inode_);
        std::string path = join_paths(mount_path_, inode_str);
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
    }

    // Seek to the specified offset
    if (::lseek(fd_, offset, SEEK_SET) == -1) {
        return Status::IOError("Failed to seek file: " +
                               std::string(strerror(errno)));
    }

    // Read up to 'size' bytes from the local file
    ssize_t bytes_read = ::read(fd_, dst.data(), size);
    if (bytes_read < 0) {
        return Status::IOError("Failed to read file: " +
                               std::string(strerror(errno)));
    }

    return Status::OK();
}

Status FileHandle::write(Slice &src, size_t count, off_t offset) {
    if (fd_ == -1) {
        std::string inode_str = std::to_string(inode_);
        std::string path = join_paths(mount_path_, inode_str);
        // must be readable later in close()
        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
    }

    // Seek to the specified offset
    if (::lseek(fd_, offset, SEEK_SET) == -1) {
        return Status::IOError("Failed to seek file: " +
                               std::string(strerror(errno)));
    }

    // Write the data to the local file
    ssize_t written = ::write(fd_, src.data(), count);
    if (written < 0 || static_cast<size_t>(written) != count) {
        return Status::IOError("Failed to write file: " +
                               std::string(strerror(errno)));
    }

    // Update metadata attributes based on local filesystem.
    // Get the file size.
    struct stat st;
    if (::fstat(fd_, &st) == -1) {
        return Status::IOError("Failed to stat file: " +
                               std::string(strerror(errno)));
    }
    Attributes attr;
    attr.set_mode(st.st_mode);
    attr.set_path(filename(logic_path_));
    attr.set_size(st.st_size);
    attr.set_access_time(st.st_atime);
    attr.set_modification_time(st.st_mtime);
    attr.set_creation_time(st.st_ctime);
    attr.set_inode(inode_);
    attr.set_user_id(st.st_uid);
    attr.set_group_id(st.st_gid);

    Status meta_status = setattr(attr);
    if (!meta_status.ok()) {
        return meta_status;
    }

    return Status::OK();
}

Status FileHandle::sync() {
    if (fd_ == -1) {
        std::string inode_str = std::to_string(inode_);
        std::string path = join_paths(mount_path_, inode_str);
        fd_ = ::open(path.c_str(), O_RDWR);
        if (fd_ == -1) {
            return Status::IOError("Failed to open file: " +
                                   std::string(strerror(errno)));
        }
    }

    // Rewind the file to the beginning.
    if (::lseek(fd_, 0, SEEK_SET) == -1) {
        return Status::IOError("Failed to seek file: " +
                               std::string(strerror(errno)));
    }

    // Get the file size.
    struct stat st;
    if (::fstat(fd_, &st) == -1) {
        return Status::IOError("Failed to stat file: " +
                               std::string(strerror(errno)));
    }
    size_t filesize = st.st_size;

    // Allocate a buffer and read the file content.
    std::string buffer;
    buffer.resize(filesize);
    ssize_t n = ::read(fd_, &buffer[0], filesize);
    if (n < 0 || static_cast<size_t>(n) != filesize) {
        return Status::IOError("Failed to read whole file: " +
                               std::string(strerror(errno)));
    }

    // Write the file data to the remote storage.
    std::vector<std::string> chunk_nodes{
        "node1",
        "node2",
        "node3",
        "node4",
    };
    std::vector<std::string> parity_nodes{
        "node5",
        "node6",
    };

    std::string file_id = std::to_string(inode_);
    Data data;
    data.set_payload(buffer);
    data.set_len(filesize);

    auto [s, bytes_written] =
        storage_->write(chunk_nodes, parity_nodes, file_id, data);
    if (!s.ok()) {
        return s;
    }

    // Update metadata attributes based on local filesystem.
    Attributes attr;
    attr.set_mode(st.st_mode);
    attr.set_path(filename(logic_path_));
    attr.set_size(st.st_size);
    attr.set_access_time(st.st_atime);
    attr.set_modification_time(st.st_mtime);
    attr.set_creation_time(st.st_ctime);
    attr.set_inode(inode_);
    attr.set_user_id(st.st_uid);
    attr.set_group_id(st.st_gid);

    Status meta_status = setattr(attr);
    if (!meta_status.ok()) {
        return meta_status;
    }

    return Status::OK();
}

Status FileHandle::setattr(Attributes &attr) {
    // Update the file attributes in the metadata service.
    auto s = metadata_->setattr(attr);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}
