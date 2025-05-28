#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "file_handle.h"
#include "metadata.pb.h"
#include "slice.h"
#include "status.h"
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
    }
    return Status::OK();
}

Status FileHandle::destroy() {
    // delete the file from the local storage
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    if (::unlink(path.c_str()) == -1) {
        return Status::IOError("destroy failed: " + std::string(strerror(errno)));
    }
    auto s = metadata_->remove_file(p_inode_, inode_, filename(logic_path_));
    if (!s.ok()) {
        return s;
    }

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

    storage_->remove_file(chunk_nodes, parity_nodes, std::to_string(inode_));

    return Status::OK();
}

Status FileHandle::open(FilePointer **out_fp, int flags) {
    std::string inode_str = std::to_string(inode_);
    std::string path      = join_paths(mount_path_, inode_str);

    // 1) wrap the fd in a unique_ptr<FilePointer>
    auto self = shared_from_this();                
    auto fp = std::make_unique<FilePointer>(self);
    fp->open(path, flags);
    *out_fp = fp.get();
    file_pointers_.push_back(std::move(fp));

    return Status::OK();
}

Status FileHandle::close(FilePointer *fp) {
    // first find the FilePointer in the vector
    auto it = std::find_if(file_pointers_.begin(), file_pointers_.end(),
                           [fp](const std::unique_ptr<FilePointer> &p) {
                               return p.get() == fp;
                           });
    if (it == file_pointers_.end()) {
        return Status::NotFound("FilePointer not found in the vector");
    }

    fp->close();
    file_pointers_.erase(it);

    return Status::OK();
}

Status FileHandle::unlink() {
    unlink_ = true;
    return Status::OK();
}

Status FileHandle::read(FilePointer *fp, Slice &dst, size_t size, off_t offset) {
    ssize_t bytes_read = fp->read(dst, size, offset);
    if (bytes_read < 0) {
        return Status::IOError("Failed to read file: " +
                               std::string(strerror(errno)));
    }

    return Status::OK();
}

Status FileHandle::write(FilePointer *fp, Slice &src, size_t count, off_t offset) {
    // Write the data to the local file
    ssize_t written = fp->write(src, count, offset);

    if (written < 0 || static_cast<size_t>(written) != count) {
        return Status::IOError("Failed to write file: " +
                               std::string(strerror(errno)));
    }

    // Update the file attributes in the metadata service
    // read stats from local file
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    struct stat st;
    if (::fstat(fp->fd, &st) == -1) {
        return Status::IOError("fstat failed: " + std::string(strerror(errno)));
    }
    
    return Status::OK();
}

Status FileHandle::getattr(struct stat *buf) {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);

    if (::lstat(path.c_str(), buf) == -1) {
        return Status::IOError("lstat failed: " + std::string(strerror(errno)));
    }

    return Status::OK();
}

Status FileHandle::utimens(const struct timespec tv[2]) {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);

    if (::utimensat(AT_FDCWD, path.c_str(), tv, 0) == -1) {
        return Status::IOError("utimensat failed: " + std::string(strerror(errno)));
    }

    return Status::OK();
}

Status FileHandle::sync() {
    // Sync the file to the local storage
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);

    if (::fsync(::open(path.c_str(), O_RDWR)) == -1) {
        return Status::IOError("fsync failed: " + std::string(strerror(errno)));
    }

    return Status::OK();
}

Status FileHandle::flush() {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd == -1) {
        return Status::IOError("Failed to open file: " +
                                std::string(strerror(errno)));
    }

    // Get the file size.
    struct stat st;
    if (::fstat(fd, &st) == -1) {
        return Status::IOError("Failed to stat file: " +
                               std::string(strerror(errno)));
    }
    size_t filesize = st.st_size;

    // Allocate a buffer and read the file content.
    std::string buffer;
    buffer.resize(filesize);
    ssize_t n = ::read(fd, &buffer[0], filesize);
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

Status FileHandle::cache() {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd == -1) {
        return Status::IOError("Failed to open file: " +
                                std::string(strerror(errno)));
    }

    // Retrieve the file from remote storage
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
    auto [st, data] = storage_->read(chunk_nodes, parity_nodes, file_id);

    ssize_t written = ::write(fd, data.payload().c_str(), data.len());
    if (written < 0 || static_cast<size_t>(written) != data.len()) {
        return Status::IOError(
            "Failed to write remote data to local file: " +
            std::string(strerror(errno)));
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
