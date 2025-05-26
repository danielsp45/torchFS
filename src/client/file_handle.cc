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
    }
    return Status::OK();
}

Status FileHandle::destroy() {
    // TODO: the file removal should be done only when there's no more open's

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

Status FileHandle::open(int flags) {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    // ensure we can write remote data into the file even if FUSE asked for
    // O_RDONLY
    int open_flags = (flags & ~O_ACCMODE) | O_CREAT;
    if ((open_flags & O_ACCMODE) == O_RDONLY) {
        open_flags = (open_flags & ~O_ACCMODE) | O_RDWR;
    }
    fd_ = ::open(path.c_str(), open_flags, 0644);
    if (fd_ == -1) {
        return Status::IOError("Failed to open local file: " +
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

    auto [st_attr, attr] = metadata_->getattr(inode_);
    if (!st_attr.ok()) {
        return st_attr;
    }
    if (attr.size() > 0) {
        std::string file_id = std::to_string(inode_);
        auto [st, data] = storage_->read(chunk_nodes, parity_nodes, file_id);
        if (!st.ok()) {
            return st;
        }
        ssize_t written = ::write(fd_, data.payload().c_str(), data.len());
        if (written < 0 || static_cast<size_t>(written) != data.len()) {
            return Status::IOError(
                "Failed to write remote data to local file: " +
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
    std::cout << "CLOSING FILE" << std::endl;
    if (fd_ == -1) {
        return Status::IOError("File not open");
    }

    // Rewind the file to the beginning.
    if (::lseek(fd_, 0, SEEK_SET) == -1) {
        return Status::IOError("Failed to seek file: " +
                               std::string(strerror(errno)));
    }
    std::cout << "LSEEK SUCCEEDED" << std::endl;

    // Get the file size.
    struct stat st;
    if (::fstat(fd_, &st) == -1) {
        return Status::IOError("Failed to stat file: " +
                               std::string(strerror(errno)));
    }
    size_t filesize = st.st_size;

    std::cout << "FILE READ BEFORE: " << filesize << std::endl;

    // Allocate the string so &buffer[0] is valid
    std::string buffer;
    buffer.resize(filesize);

    // Read the file content.
    ssize_t n = ::read(fd_, &buffer[0], filesize);
    std::cout << "FILE READ AFTER: " << n << std::endl;
    if (n < 0 || static_cast<size_t>(n) != filesize) {
        return Status::IOError("Failed to read whole file: " +
                               std::string(strerror(errno)));
    }
    std::cout << "READ SUCCEEDED" << std::endl;

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
    std::cout << "WROTE FILE TO STORAGE, BYTES WRITTEN: " << bytes_written
              << std::endl;

    // Update metadata with the new file size.
    Attributes attr = metadata_->getattr(inode_).second;
    attr.set_size(filesize);
    s = setattr(attr);
    if (!s.ok()) {
        return s;
    }

    // Close the local file.
    if (::close(fd_) == -1) {
        return Status::IOError("Failed to close file: " +
                               std::string(strerror(errno)));
    }
    fd_ = -1;

    // Delete the local file.
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    if (::unlink(path.c_str()) == -1) {
        return Status::IOError("Failed to delete local file: " +
                               std::string(strerror(errno)));
    }

    return Status::OK();
}

Status FileHandle::read(Slice &dst, size_t size, off_t offset) {
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

    // Update metadata: refresh file size based on current local file state.
    struct stat st;
    if (::fstat(fd_, &st) == -1) {
        return Status::IOError("Failed to stat file: " +
                               std::string(strerror(errno)));
    }
    Attributes attr = metadata_->getattr(inode_).second;
    attr.set_size(static_cast<size_t>(st.st_size));

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
    auto s = metadata_->setattr(attr);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}
