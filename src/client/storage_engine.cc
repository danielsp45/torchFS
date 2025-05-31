#include "storage_engine.h"
#include "directory.h"
#include "file_handle.h"
#include "fuse.h"
#include "status.h"

#include <dirent.h>
#include <fcntl.h> // For O_CREAT and file mode flags
#include <memory>

Status StorageEngine::init() {
    // Initialize the root directory
    Status s = root_->init();
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

Status StorageEngine::open(const std::string &path, int flags, 
                           FilePointer **out_fp) {

    // std::shared_ptr<FileHandle> cached_fh = cache_.lookup(path);
    
    // if (cached_fh) {
    //     std::cout << "Cached" << std::endl;
    //     Status s = cached_fh->open(out_fp, flags);
    //     if (!s.ok()) {
    //         return s;
    //     }
    //     return Status::OK();
    // }

    // // File is not cached
    // std::cout << "Not Cached" << std::endl;
    auto [s, fh] = find_file(path);
    if (!fh) {
        return Status::NotFound("File not found");
    }
    s = fh->open(out_fp, flags);
    if (!s.ok()) {
        return s;
    }
    cache_.insert(path, fh);
    return Status::OK();
}

Status StorageEngine::create(const std::string &path) {
    auto [dir_name, file_name] = split_path_from_target(path);
    if (file_name.empty()) {
        return Status::InvalidArgument("Invalid path");
    }

    auto [s, parent_dir] = find_dir(dir_name);
    if (!s.ok()) {
        return s;
    }

    if (parent_dir->get_file(file_name)) {
        return Status::AlreadyExists("File already exists");
    }

    auto [s1, fh] = parent_dir->create_file(file_name);

    return s1;
}

Status StorageEngine::close(FilePointer *fp) {
    auto fh = fp->fh;
    Status s1 = fh->close(fp);
    if (!s1.ok()) {
        return s1;
    }

    if (fh->is_unlinked()) {
        // If the file is unlinked, we can remove it from the parent directory
        std::string path = fh->get_logic_path();
        auto [dir_name, file_name] = split_path_from_target(path);
        auto [dir_status, directory] = find_dir(dir_name);
        if (!dir_status.ok()) {
            return dir_status;
        }

        // Remove the file from the directory
        auto [s2, fh] = directory->remove_file(file_name);
        if (!s2.ok()) {
            return s2;
        }
    }

    return Status::OK();
}

Status StorageEngine::unlink(const std::string path) {
    auto [dir_name, file_name] = split_path_from_target(path);

    auto [dir_status, directory] = find_dir(dir_name);
    if (!dir_status.ok()) {
        return dir_status;
    }

    auto fh = directory->get_file(file_name);
    fh->unlink();

    if (fh->is_unlinked()) {
        // If the file is unlinked, we can remove it from the parent directory
        auto [s, _fh] = directory->remove_file(file_name);
        if (!s.ok()) {
            return s;
        }
    }

    return Status::OK();
}

Status StorageEngine::read(FilePointer *fp, Slice result, size_t size,
                           off_t offset) {
    std::shared_ptr<FileHandle> fh = fp->fh;
    Status s = fh->read(fp, result, size, offset);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

Status StorageEngine::write(FilePointer *fp, Slice data, size_t size,
                            off_t offset) {
    std::shared_ptr<FileHandle> fh = fp->fh;
    Status s = fh->write(fp, data, size, offset);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}

Status StorageEngine::sync(std::string path) {
    auto [s, fh] = find_file(path);
    if (!s.ok()) {
        return s;
    }
    return fh->sync();
}

Status StorageEngine::rename(const std::string &src_path,
                             const std::string &dst_path) {
    auto [src_parent_path, src_name] = split_path_from_target(src_path);
    auto [dst_parent_path, dst_name] = split_path_from_target(dst_path);

    if (is_file(src_path)) {
        if (src_name.empty() || dst_name.empty()) {
            return Status::InvalidArgument("Invalid path");
        }

        // Get the parent directories
        auto [src_dir_status, src_parent_dir] = find_dir(src_parent_path);
        if (!src_dir_status.ok()) {
            return src_dir_status;
        }

        auto [dst_dir_status, dst_parent_dir] = find_dir(dst_parent_path);
        if (!dst_dir_status.ok()) {
            return dst_dir_status;
        }

        std::shared_ptr<FileHandle> file_handle =
            src_parent_dir->get_file(src_name);
        if (!file_handle) {
            return Status::NotFound("File not found");
        }

        Status rename_status =
            dst_parent_dir->move_file(src_parent_dir, file_handle, dst_name);
        if (!rename_status.ok()) {
            return rename_status;
        }

        return Status::OK();
    } else if (is_dir(src_path)) {
        if (src_name.empty() || dst_name.empty()) {
            return Status::InvalidArgument("Invalid path");
        }

        auto [src_parent_status, src_parent_directory] =
            find_dir(src_parent_path);
        if (!src_parent_status.ok()) {
            return src_parent_status;
        }

        auto [dst_parent_status, dst_parent_directory] =
            find_dir(dst_parent_path);
        if (!dst_parent_status.ok()) {
            return dst_parent_status;
        }

        auto [remove_dir_status, directory_to_move] =
            src_parent_directory->remove_dir(src_name, false);
        if (!remove_dir_status.ok()) {
            return remove_dir_status;
        }

        Status rename_dir_status = dst_parent_directory->move_dir(
            src_parent_directory, std::move(directory_to_move), dst_name);
        if (!rename_dir_status.ok()) {
            return rename_dir_status;
        }

        return Status::OK();
    }

    return Status::NotFound("File or directory not found");
}

Status StorageEngine::mkdir(const std::string &path) {
    auto [dir_name, new_dir_name] = split_path_from_target(path);
    if (new_dir_name.empty()) {
        return Status::InvalidArgument("Invalid path");
    }

    // Get the parent directory
    auto [s, parent_dir] = find_dir(dir_name);
    if (!s.ok()) {
        return s;
    }

    // Check if the directory already exists
    auto dir = parent_dir->get_dir(new_dir_name);
    if (dir) {
        return Status::AlreadyExists("Directory already exists");
    }

    // Create the new directory
    auto [s1, _dir] = parent_dir->create_subdirectory(new_dir_name);
    return s1;
}

Status StorageEngine::rmdir(const std::string &path) {
    auto [dir_name, target] = split_path_from_target(path);
    auto [s, parent_dir] = find_dir(dir_name);
    if (target.empty() || !s.ok()) {
        return Status::InvalidArgument("Invalid path");
    }

    // Check if the directory exists
    auto subdir = parent_dir->get_dir(target);
    if (!subdir) {
        return Status::NotFound("Directory not found");
    }

    auto [s1, dir] = parent_dir->remove_dir(target);
    return s1;
}

Status StorageEngine::getattr(const std::string &path, struct stat *stbuf) {
    struct stat *buf = new struct stat();
    if (is_file(path)) {
        auto [s, fh] = find_file(path);
        if (!s.ok()) {
            return s;
        }
        fh->getattr(buf);
    } else if (is_dir(path)) {
        auto [s, dir] = find_dir(path);
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

    memcpy(stbuf, buf, sizeof(struct stat));
    delete buf;
    return Status::OK();
}

Status StorageEngine::readdir(const std::string &path, void *buf,
                              fuse_fill_dir_t filler,
                              fuse_readdir_flags flags) {
    auto [s, dir] = find_dir(path);
    if (!s.ok()) {
        return Status::NotFound("Directory not found");
    }

    return dir->readdir(buf, filler, flags);
}

Status StorageEngine::utimens(const std::string &path,
                              const struct timespec tv[2]) {
    // check if is file or directory
    if (is_file(path)) {
        auto [s, fh] = find_file(path);
        if (!s.ok()) {
            return s;
        }
        return fh->utimens(tv);
    } else if (is_dir(path)) {
        auto [s, dir] = find_dir(path);
        if (!s.ok()) {
            return s;
        }
        return dir->utimens(tv);
    }

    return Status::NotFound("File or directory not found");
}

std::string StorageEngine::get_logic_path(const std::string &path) {
    if (path == mount_path_) {
        return "/";
    }

    return path.substr(mount_path_.size());
}

bool StorageEngine::is_file(const std::string &path) {
    if (path == "/") {
        return false;
    }
    auto [dir_name, target] = split_path_from_target(path);
    auto [s, parent_dir] = find_dir(dir_name);
    if (target.empty() || !s.ok()) {
        return false;
    }

    auto fh = parent_dir->get_file(target);
    if (!fh) {
        return false;
    }

    return true;
}

bool StorageEngine::is_dir(const std::string &path) {
    if (path == "/") {
        return true;
    }
    auto [dir_name, target] = split_path_from_target(path);
    auto [s, parent_dir] = find_dir(dir_name);
    if (target.empty() || !s.ok()) {
        return false;
    }
    // Check if the target is a directory
    auto subdir = parent_dir->get_dir(target);
    if (!subdir) {
        return false;
    }

    return true;
}

std::pair<Status, std::shared_ptr<FileHandle>>
StorageEngine::find_file(const std::string &path) {
    if (path == "/") {
        return {Status::InvalidArgument("Invalid path"), nullptr};
    }
    auto [dir_name, file_name] = split_path_from_target(path);
    auto [s, parent_dir] = find_dir(dir_name);
    if (!s.ok()) {
        return {s, nullptr};
    }
    auto fh = parent_dir->get_file(file_name);
    if (!fh) {
        return {Status::NotFound("File not found"), nullptr};
    }
    return {Status::OK(), fh};
}

std::pair<Status, Directory *>
StorageEngine::find_dir(const std::string &path) {
    if (path == "/") {
        return {Status::OK(), root_.get()};
    }
    std::vector<std::string> path_parts = split_path(path);
    if (path_parts.empty()) {
        // it means this is the root directory
        return {Status::OK(), root_.get()};
    } else {
        Directory *current_dir = root_.get();
        for (const auto &part : path_parts) {
            auto next_dir = current_dir->get_dir(part);
            if (!next_dir) {
                return {Status::NotFound("Directory not found"), nullptr};
            }
            current_dir = next_dir;
        }
        return {Status::OK(), current_dir};
    }

    return {Status::OK(), nullptr};
}
