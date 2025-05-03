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
    s = fh->open(flags);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}

Status StorageEngine::create(const std::string &path, int flags, mode_t mode) {
    auto [s, fh] = namespace_->create_file(path);
    return s;
}

Status StorageEngine::close(std::string &path) {
    auto [s, fh] = namespace_->find_file(path);
    if (!s.ok()) {
        return s;
    }

    Status s1 = fh->close();
    if (!s1.ok()) {
        return s;
    }

    return Status::OK();
}

Status StorageEngine::remove(const std::string path) {
    auto [dir_name, file_name] = split_path_from_target(path);
    auto [s1, dir] = namespace_->find_dir(dir_name);
    if (!s1.ok()) {
        return s1;
    }

    auto [s2, fh] = dir->remove_file(file_name);
    if (!s2.ok()) {
        return s2;
    }

    return fh->destroy();
}

Status StorageEngine::read(std::string &path, Slice result, size_t size,
                           off_t offset) {
    auto [s, fh] = namespace_->find_file(path);
    if (!s.ok()) {
        return s;
    }

    s = fh->read(result, size, offset);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

Status StorageEngine::write(std::string &path, Slice data, size_t size,
                            off_t offset) {
    auto [s, fh] = namespace_->find_file(path);
    if (!s.ok()) {
        return s;
    }

    s = fh->write(data, size, offset);
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
