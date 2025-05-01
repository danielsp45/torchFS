#include "storage_engine.h"
#include "directory.h"
#include "file_handle.h"
#include "fuse.h"
#include "status.h"

#include <cstdint>
#include <dirent.h>
#include <fcntl.h> // For O_CREAT and file mode flags
#include <iostream>
#include <memory>

Status StorageEngine::init() {
    // Initialize the root directory
    return Status::OK();
}

Status StorageEngine::open(const std::string &path, int flags) {

    auto [s, fh] = namespace_->find_file(path);
    if (!fh) {
        return Status::NotFound("File not found");
    }
    register_fh(fh);
    s = fh->open(flags);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}

Status StorageEngine::is_open(const std::string &path) {
    auto it = open_files_.find(path);
    if (it != open_files_.end()) {
        return Status::OK();
    }
    return Status::NotFound("File not found");
}

Status StorageEngine::create(const std::string &path, int flags, mode_t mode) {
    auto [s, fh] = namespace_->create_file(path);
    return s;
}

Status StorageEngine::close(std::string &path) {
    // Retrieve the file handle from the open_files_ map.
    std::shared_ptr<FileHandle> fh = lookup_fh(path);
    if (!fh) {
        return Status::NotFound("Invalid file handle");
    }

    Status s = fh->close();
    if (!s.ok()) {
        return s;
    }

    open_files_.erase(path);

    return Status::OK();
}

Status StorageEngine::remove(const std::string path) {
    auto [s, fh] = namespace_->find_file(path);
    if (!s.ok()) {
        return s;
    }

    return fh->destroy();
}

Status StorageEngine::read(std::string &path, Slice result, size_t size,
                           off_t offset) {
    std::shared_ptr<FileHandle> fh = lookup_fh(path);
    if (!fh) {
        return Status::NotFound("Invalid file handle");
    }

    Status s = fh->read(result, size, offset);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

Status StorageEngine::write(std::string &path, Slice data, size_t size,
                            off_t offset) {
    std::shared_ptr<FileHandle> fh = lookup_fh(path);
    if (!fh) {
        return Status::NotFound("Invalid file handle");
    }

    Status s = fh->write(data, size, offset);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

Status StorageEngine::sync(std::string path) {
    auto [s, fh] = namespace_->find_file(path);
    if (!s.ok()) {
        return s;
    }
    return fh->sync();
}

Status StorageEngine::rename(const std::string &oldpath,
                             const std::string &newpath) {
    if (namespace_->is_file(oldpath)) {
        return namespace_->rename_file(oldpath, newpath);
    } else if (namespace_->is_dir(oldpath)) {
        return namespace_->rename_dir(oldpath, newpath);
    } else {
        return Status::NotFound("File or directory not found");
    }
}

Status StorageEngine::mkdir(const std::string &path, mode_t mode) {
    auto [s, dir] = namespace_->create_dir(path);
    if (!s.ok()) {
        return s;
    }
    return s;
}

Status StorageEngine::rmdir(const std::string &path) {
    auto [s, dir] = namespace_->remove_dir(path);
    if (!s.ok()) {
        return s;
    }
    dir->destroy();
    return s;
}

std::string StorageEngine::register_fh(std::shared_ptr<FileHandle> fh) {
    std::string path = fh->get_logic_path();
    open_files_[path] = fh;
    return path;
}

std::shared_ptr<FileHandle> StorageEngine::lookup_fh(std::string &path) {
    auto it = open_files_.find(path);
    if (it != open_files_.end()) {
        return it->second;
    }
    return nullptr;
}

Status StorageEngine::getattr(const std::string &path, struct stat *stbuf) {
    struct stat *buf = new struct stat();
    if (namespace_->is_file(path)) {
        auto [s, fh] = namespace_->find_file(path);
        if (!s.ok()) {
            return s;
        }
        fh->getattr(buf);
    } else if (namespace_->is_dir(path)) {
        auto [s, dir] = namespace_->find_dir(path);
        if (!s.ok()) {
            return s;
        }
        dir->getattr(buf);
    } else {
        return Status::NotFound("File or directory not found");
    }

    if (!buf) {
        return Status::IOError("Failed to get directory metadata");
    }

    // print the data inside the buffer
    memcpy(stbuf, buf, sizeof(struct stat));
    delete buf;
    return Status::OK();
}

Status StorageEngine::readdir(const std::string &path, void *buf,
                              fuse_fill_dir_t filler,
                              fuse_readdir_flags flags) {
    auto [s, dir] = namespace_->find_dir(path);
    if (!s.ok()) {
        return Status::NotFound("Directory not found");
    }

    return dir->readdir(buf, filler, flags);
}

std::string StorageEngine::get_logic_path(const std::string &path) {
    if (path == mount_path_) {
        return "/";
    }

    return path.substr(mount_path_.size());
}
