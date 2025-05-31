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
    if (fetched_) {
        Status s = remove_local();
        if (!s.ok()) {
            return s;
        }
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
    if (!fetched_) {
        Status s = fetch();
        if (!s.ok()) {
            return s;
        }
    }

    std::string inode_str = std::to_string(inode_);
    std::string path      = join_paths(mount_path_, inode_str);

    auto self = shared_from_this();                
    auto fp = std::make_unique<FilePointer>(self);
    fp->open(path, flags);
    *out_fp = fp.get();
    file_pointers_.push_back(std::move(fp));

    return Status::OK();
}

Status FileHandle::close(FilePointer *fp) {
    auto it = std::find_if(file_pointers_.begin(), file_pointers_.end(),
                           [fp](const std::unique_ptr<FilePointer> &p) {
                               return p.get() == fp;
                           });
    if (it == file_pointers_.end()) {
        return Status::NotFound("FilePointer not found in the vector");
    }

    fp->close();
    file_pointers_.erase(it);

    if (written_ && fetched_ && file_pointers_.empty()) {
        Status s = flush();
        if (!s.ok()) {
            return s;
        }
    }

    if (!cached_ && fetched_ && file_pointers_.empty()) {
        Status s = remove_local();
        if (!s.ok()) {
            return s;
        }
    }

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

    attributes_.set_size(st.st_size);

    written_ = true;
    
    return Status::OK();
}

Status FileHandle::getattr(struct stat *buf) {
    if (cached_) {
        attr_to_stat(attributes_, buf);
    } else {
        auto [s, attr] = metadata_->getattr(inode_);
        if (!s.ok()) {
            return s;
        }
        attr_to_stat(attr, buf);
    }

    return Status::OK();
}

Status FileHandle::utimens(const struct timespec tv[2]) {
    if (cached_) {
        attributes_.set_access_time(tv[0].tv_sec);
        attributes_.set_modification_time(tv[1].tv_sec);
    } else {
        // Update the file attributes in the metadata service
        auto [s, attr] = metadata_->getattr(inode_);
        attr.set_access_time(tv[0].tv_sec);
        attr.set_modification_time(tv[1].tv_sec);

        s = setattr(attr);
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

Status FileHandle::setattr(Attributes &attr) {
    if (cached_) {
        // Update the local attributes
        attributes_.CopyFrom(attr);
        return Status::OK();
    } else {
        // Update the file attributes in the metadata service.
        auto s = metadata_->setattr(attr);
        if (!s.ok()) {
            return s;
        }
    }
    return Status::OK();
}

void FileHandle::cache() {
    if (cached_) {
        // If the file is already cached, no need to cache again
        return;
    }

    if (!fetched_) {
        // If the file is not fetched, we need to fetch it first
        Status s = fetch();
        if (!s.ok()) {
            return;
        }
    }

    cached_ = true;
};

void FileHandle::uncache() {
    if (!cached_) {
        return; // Already uncached
    }

    if (fetched_ && file_pointers_.empty()) {
        // If the file is fetched and there are no open file pointers, remove it
        Status s = remove_local();
        if (!s.ok()) {
        }
    }

    cached_ = false;
};


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
    stat_to_attr(st, attr);

    Status meta_status = setattr(attr);
    if (!meta_status.ok()) {
        return meta_status;
    }

    ::close(fd);

    written_ = false;

    return Status::OK();
}

Status FileHandle::fetch() {
    // get the cache attributes from the metadata service
    auto [s, attr] = metadata_->getattr(inode_);
    if (!s.ok()) {
        return s;
    }
    attributes_ = attr;

    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT);
    if (fd == -1) {
        return Status::IOError("Failed to open file: " +
                                std::string(strerror(errno)));
    }

    if (attributes_.size() > 0) {
        // Only fetch the file if it has a size greater than 0

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
    }

    ::close(fd); // Close the file descriptor after writing


    fetched_ = true;

    return Status::OK();
}

Status FileHandle::remove_local() {
    std::string inode_str = std::to_string(inode_);
    std::string path = join_paths(mount_path_, inode_str);
    if (::unlink(path.c_str()) == -1) {
        return Status::IOError("Failed to unlink file: " +
                               std::string(strerror(errno)));
    }
    fetched_ = false;
    return Status::OK();
}

void FileHandle::stat_to_attr(const struct stat &st, Attributes &a) {
    a.set_inode(inode_);
    a.set_path(logic_path_);
    a.set_mode(st.st_mode);
    a.set_size(st.st_size);
    a.set_access_time(st.st_atime);
    a.set_modification_time(st.st_mtime);
    a.set_creation_time(st.st_ctime);
    a.set_user_id(st.st_uid);
    a.set_group_id(st.st_gid);
}

// Convert our Attributes proto → POSIX stat buffer
void FileHandle::attr_to_stat(const Attributes &a, struct stat *st) {
    st->st_mode = a.mode();
    st->st_size = a.size();
    st->st_atime = a.access_time();
    st->st_mtime = a.modification_time();
    st->st_ctime = a.creation_time();
    st->st_uid  = a.user_id();
    st->st_gid  = a.group_id();
}