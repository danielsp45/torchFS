#include "storage_engine.h"
#include "directory.h"
#include "file_handle.h"
#include "fuse.h"
#include "status.h"

#include <cstdint>
#include <dirent.h>
#include <fcntl.h> // For O_CREAT and file mode flags
#include <memory>

Status StorageEngine::init() {
    // Initialize the root directory
    return Status::OK();
}

Status StorageEngine::open(const std::string &path, int flags,
                           uint64_t &handle) {

    auto [s, fh] = namespace_->find_file(path);
    if (!fh) {
        return Status::NotFound("File not found");
    }
    handle = register_fh(fh);
    s = fh->open(flags);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}

Status StorageEngine::create(const std::string &path, int flags, mode_t mode,
                             uint64_t &handle) {
    auto [s, fh] = namespace_->create_file(path);
    if (!s.ok()) {
        return s;
    }
    // now open the file with CREATE
    auto s1 = fh->open(flags, mode);
    if (!s1.ok()) {
        return s1;
    }
    // since the file is created we can close it
    auto f2 = fh->close();
    if (!f2.ok()) {
        return f2;
    }

    return Status::OK();
}

Status StorageEngine::close(uint64_t &handle) {
    // Retrieve the file handle from the open_files_ map.
    std::shared_ptr<FileHandle> fh = lookup_fh(handle);
    if (!fh) {
        return Status::NotFound("Invalid file handle");
    }

    Status s = fh->close();
    if (!s.ok()) {
        return s;
    }

    open_files_.erase(handle);

    handle = 0;
    return Status::OK();
}

Status StorageEngine::remove(const std::string path) {
    auto [s, fh] = namespace_->find_file(path);
    if (!s.ok()) {
        return s;
    }

    return fh->remove();
}

Status StorageEngine::read(uint64_t &handle, Slice result, size_t size,
                           off_t offset) {
    std::shared_ptr<FileHandle> fh = lookup_fh(handle);
    if (!fh) {
        return Status::NotFound("Invalid file handle");
    }

    Status s = fh->read(result, size, offset);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

Status StorageEngine::write(uint64_t &handle, Slice data, size_t size,
                            off_t offset) {
    std::shared_ptr<FileHandle> fh = lookup_fh(handle);
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

uint64_t StorageEngine::register_fh(std::shared_ptr<FileHandle> fh) {
    uint64_t id = fh->get_id();
    open_files_[id] = fh;
    return id;
}

std::shared_ptr<FileHandle> StorageEngine::lookup_fh(uint64_t id) {
    auto it = open_files_.find(id);
    if (it != open_files_.end()) {
        return it->second;
    }
    return nullptr;
}

Status StorageEngine::getattr(const std::string &path, struct stat *stbuf) {
    struct stat *buf;
    if (namespace_->is_file(path)) {
        auto [s, fh] = namespace_->find_file(path);
        if (!s.ok()) {
            return s;
        }
        buf = fh->get_meta();
    } else if (namespace_->is_dir(path)) {
        auto [s, dir] = namespace_->find_dir(path);
        if (!s.ok()) {
            return s;
        }
        buf = dir->get_meta();
    } else {
        return Status::NotFound("File or directory not found");
    }

    if (!buf) {
        return Status::IOError("Failed to get directory metadata");
    }

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
