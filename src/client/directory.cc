#include "directory.h"
#include "file_handle.h"
#include "status.h"
#include "util.h"

#include <cstddef>
#include <cstring>
#include <dirent.h>
#include <errno.h>
#include <memory>
#include <string>
#include <sys/stat.h> // Added for stat()
#include <utility>

// Look up an existing file in the directory.
std::shared_ptr<FileHandle> Directory::get_file(const std::string name) {
    auto it = files_.find(name);
    if (it != files_.end()) {
        return it->second;
    }
    return nullptr;
}

// Look up a subdirectory by name.
Directory *Directory::get_dir(const std::string &name) {
    auto it = subdirs_.find(name);
    if (it != subdirs_.end()) {
        return it->second.get();
    }
    return nullptr;
}

std::pair<Status, std::shared_ptr<FileHandle>>
Directory::create_file(const std::string &name) {
    // TODO: this operation needs to go to the metadata service to create a new file
    // Example:
    // metadata_service_->create_file(path);
    if (files_.find(name) != files_.end()) {
        return {Status::AlreadyExists("File already exists"), nullptr};
    }
    std::string file_logic_path = join_paths(logic_path_, name);
    auto fh = std::make_shared<FileHandle>(file_logic_path, mount_path_);
    files_[name] = fh;
    // Note: The Directory only registers the file.
    // The Storage Engine is responsible for creating the physical file in
    // local_path.
    return {Status::OK(), fh};
}

std::pair<Status, Directory *>
Directory::create_subdirectory(const std::string &name) {
    // TODO: this operation needs to go to the metadata service
    // EXAMPLE:
    // metadata_service_->create_directory(path);
    if (subdirs_.find(name) != subdirs_.end()) {
        return {Status::AlreadyExists("Directory already exists"), nullptr};
    }
    std::string new_path = join_paths(logic_path_, name);
    auto new_dir = std::make_unique<Directory>(new_path, mount_path_, this);
    Directory *new_dir_ptr = new_dir.get();
    subdirs_[name] = std::move(new_dir);

    // TODO: the mode should be passed from the caller
    int r = ::mkdir(new_path.c_str(), 0755); // Use appropriate permissions
    if (r == -1) {
        return {Status::IOError("Failed to create directory: " +
                                std::string(strerror(errno))),
                nullptr};
    }

    return {Status::OK(), new_dir_ptr};
}

std::pair<Status, std::shared_ptr<FileHandle>>
Directory::remove_file(const std::string name) {
    // TODO: this operation needs to go to the metadata service
    // Example:
    // metadata_service_->remove_file(path);
    auto it = files_.find(name);
    if (it == files_.end()) {
        return {Status::NotFound("File not found"), nullptr};
    }
    auto fh = it->second;
    files_.erase(it);
    return {Status::OK(), fh};
}

std::pair<Status, std::unique_ptr<Directory>>
Directory::remove_dir(const std::string name) {
    // TODO: this operation needs to go to the metadata service
    // Example:
    // metadata_service_->remove_directory(path);
    auto it = subdirs_.find(name);
    if (it == subdirs_.end()) {
        return {Status::NotFound("Directory not found"), nullptr};
    }
    // If you want to ensure the directory is empty, add a check here.
    if (!it->second->files_.empty() || !it->second->subdirs_.empty()) {
        return {Status::InvalidArgument("Directory not empty"), nullptr};
    }
    auto dir_ptr = std::move(it->second);
    subdirs_.erase(it);
    return {Status::OK(), std::move(dir_ptr)};
}

Status Directory::move_file(std::shared_ptr<FileHandle> fh) {
    // TODO: this operation needs to go to the metadata service
    // Example:
    // metadata_service_->move_file(old_path, new_path);
    auto it = files_.find(fh->get_name());
    if (it == files_.end()) {
        return Status::NotFound("File not found");
    }
    // Move the file to the new directory.
    files_[fh->get_name()] = fh;
    return Status::OK();
}

Status Directory::move_dir(std::unique_ptr<Directory> dir) {
    // TODO: this operation needs to go to the metadata service
    // Example:
    // metadata_service_->move_directory(old_path, new_path);
    auto it = subdirs_.find(dir->get_name());
    if (it == subdirs_.end()) {
        return Status::NotFound("Directory not found");
    }
    // Move the directory to the new location.
    subdirs_[dir->get_name()] = std::move(dir);
    return Status::OK();
}

struct stat *Directory::get_meta() {
    // TODO: this operation needs to go to the metadata service
    // Example:
    // metadata_service_->readdir(logic_path_);
    struct stat *buf = new struct stat();
    if (logic_path_ == "/") {
        // If the logic path is root, we need to get the metadata of the
        // mount path.
        std::string path = mount_path_;
        if (::stat(path.c_str(), buf) == -1) {
            delete buf;
            return nullptr;
        }
        return buf;
    } else {
        // For other directories, we need to get the metadata of the logic path.
        std::string path = mount_path_ + "/" + logic_path_;
        if (::stat(path.c_str(), buf) == -1) {
            delete buf;
            return nullptr;
        }
        return buf;
    }
    return nullptr;
}

Status Directory::readdir(void *buf, fuse_fill_dir_t filler,
                          fuse_readdir_flags /*flags*/) {
    // TODO: this operation needs to go to the metadata service
    // Example:
    // metadata_service_->readdir(logic_path_);
    std::string path = join_paths(mount_path_, logic_path_);
    DIR *dp = ::opendir(path.c_str());
    if (dp == NULL) {
        return Status::IOError("Failed to open directory: " +
                               std::string(strerror(errno)));
    }

    struct dirent *de;
    // Iterate over each directory entry.
    while ((de = ::readdir(dp)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        // The mode is derived by shifting the d_type field.
        st.st_mode = de->d_type << 12;
        // Call the filler function; using offset 0 and no extra flags (0).
        if (filler(buf, de->d_name, &st, 0,
                   static_cast<fuse_fill_dir_flags>(0))) {
            break;
        }
    }

    closedir(dp);
    return Status::OK();
}

Status Directory::destroy() {
    // TODO: this operation needs to go to the metadata service
    // Example:
    // metadata_service_->rmdir(logic_path_);
    std::string path = join_paths(mount_path_, logic_path_);
    if (::rmdir(path.c_str()) == -1) {
        return Status::IOError("Failed to remove directory: " +
                               std::string(strerror(errno)));
    }

    return Status::OK();
}

std::vector<std::shared_ptr<FileHandle>> Directory::list_files() {
    // TODO: this operation needs to go to the metadata service
    // Example:
    // metadata_service_->readdir(logic_path_);
    std::vector<std::shared_ptr<FileHandle>> files;
    for (auto &entry : files_) {
        files.push_back(entry.second);
    }
    return files;
}

std::vector<Directory *> Directory::list_dirs() {
    std::vector<Directory *> dirs;
    for (auto &entry : subdirs_) {
        dirs.push_back(entry.second.get());
    }
    return dirs;
}