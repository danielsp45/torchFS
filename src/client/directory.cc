#include "directory.h"
#include "file_handle.h"
#include "status.h"
#include "util.h"

#include <cstddef>
#include <cstring>
#include <dirent.h>
#include <errno.h>
#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h> // Added for stat()
#include <utility>
#include <vector>

Status Directory::init() {
    if (inode_ == -1) {
        // we need to create the file in the metadata too
        auto [s, attr] = metadata_->create_dir(p_inode_, filename(logic_path_));
        if (!s.ok()) {
            return s;
        }
        inode_ = attr.inode();
    } else {
        // we need to read the content of this directory
        auto [s, dirent_list] = metadata_->readdir(inode_);
        if (!s.ok()) {
            return s;
        }
        // for each inode, we need to create a file/directory
        for (const auto &dirent : dirent_list) {
            this->create_inode(dirent.inode(), dirent.name());
        }
    }

    return Status::OK();
}

Status Directory::destroy() {
    // destroy all of the files in this directory
    for (auto &entry : files_) {
        auto [s1, fh] = remove_file(entry.first);
        if (!s1.ok()) {
            return s1;
        }
        fh->destroy();
    }

    // destroy all of the subdirectories in this directory
    for (auto &entry : subdirs_) {
        auto [s1, dir] = remove_dir(entry.first);
        if (!s1.ok()) {
            return s1;
        }
        dir->destroy();
    }

    // Destroy the directory by removing it from the metadata service.
    return metadata_->remove_dir(p_inode_, inode_, filename(logic_path_));
}

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
    if (files_.find(name) != files_.end()) {
        return {Status::AlreadyExists("File already exists"), nullptr};
    }

    std::string new_path = join_paths(logic_path_, name);
    auto fh = std::make_shared<FileHandle>(inode_, -1, new_path, mount_path_,
                                           metadata_);
    auto s = fh->init();
    if (!s.ok()) {
        // NOTE: if the file creation fails, since it's a shared_ptr, the
        // the destructor will be called automatically
        return {s, nullptr};
    }

    files_[name] = fh;
    return {Status::OK(), fh};
}

std::pair<Status, Directory *>
Directory::create_subdirectory(const std::string &name) {
    if (subdirs_.count(name)) {
        return {Status::AlreadyExists("Directory already exists"), nullptr};
    }

    std::string new_path = join_paths(logic_path_, name);
    auto new_dir = std::make_unique<Directory>(inode_, -1, new_path,
                                               mount_path_, metadata_);
    auto s = new_dir->init();
    if (!s.ok()) {
        return {s, nullptr};
    }

    // take raw pointer before moving
    Directory *dir_ptr = new_dir.get();
    subdirs_[name] = std::move(new_dir);

    return {Status::OK(), dir_ptr};
}

std::pair<Status, std::shared_ptr<FileHandle>>
Directory::remove_file(const std::string name) {
    auto it = files_.find(name);
    if (it == files_.end()) {
        return {Status::NotFound("File not found"), nullptr};
    }
    auto fh = it->second;

    files_.erase(it);

    return {fh->destroy(), fh};
}

std::pair<Status, std::unique_ptr<Directory>>
Directory::remove_dir(const std::string name) {
    auto it = subdirs_.find(name);
    if (it == subdirs_.end()) {
        return {Status::NotFound("Directory not found"), nullptr};
    }
    // If you want to ensure the directory is empty, add a check here.
    if (!it->second->files_.empty() || !it->second->subdirs_.empty()) {
        return {Status::InvalidArgument("Directory not empty"), nullptr};
    }
    auto dir_ptr = std::move(it->second);

    // Remove the directory from the metadata service
    auto s = metadata_->remove_dir(inode_, dir_ptr->get_inode(), name);
    if (!s.ok()) {
        return {s, nullptr};
    }

    subdirs_.erase(it);
    return {Status::OK(), std::move(dir_ptr)};
}

Status Directory::move_file(Directory *parent_dir,
                            std::shared_ptr<FileHandle> fh,
                            const std::string &new_name) {
    auto it = files_.find(fh->get_name());
    if (it != files_.end()) {
        return Status::AlreadyExists("File already exists");
    }

    auto s = metadata_->rename_file(parent_dir->get_inode(), inode_,
                                    fh->get_inode(), new_name);
    if (!s.ok()) {
        return s;
    }
    // Remove the file from the old directory.
    auto [s1, old_file_handle] = remove_file(fh->get_name());
    if (!s1.ok()) {
        return Status::NotFound("File not found in parent directory");
    }
    // rename the file
    fh->set_logic_path(join_paths(logic_path_, new_name));
    fh->set_parent_inode(inode_);
    files_[fh->get_name()] = fh;
    return Status::OK();
}

Status Directory::move_dir(Directory *parent_dir,
                           std::unique_ptr<Directory> dir,
                           const std::string &new_name) {
    auto it = subdirs_.find(dir->get_name());
    if (it != subdirs_.end()) {
        return Status::AlreadyExists("Directory already exists");
    }

    auto s = metadata_->rename_dir(parent_dir->get_inode(), inode_,
                                   dir->get_inode(), new_name);
    if (!s.ok()) {
        return s;
    }

    // Remove the directory from the old parent directory.
    auto [s1, old_dir] = parent_dir->remove_dir(dir->get_name());
    if (!s1.ok()) {
        return Status::NotFound("Directory not found in parent directory");
    }
    // Rename the directory
    dir->set_logic_path(join_paths(logic_path_, new_name));
    dir->set_parent_inode(inode_);

    // Move the directory to the new location.
    subdirs_[dir->get_name()] = std::move(dir);
    return Status::OK();
}

Status Directory::getattr(struct stat *buf) {
    // Get the file/directory attributes from the metadata service.
    auto [s, attr] = metadata_->getattr(inode_);
    if (!s.ok()) {
        return s;
    }

    // Transfer the data from Attributes to the stat structure.
    // buf->st_mode = attr.mode(); // e.g., file type and permissions
    buf->st_mode = attr.mode();               // e.g., file type and permissions
    buf->st_size = attr.size();               // file size in bytes
    buf->st_atime = attr.access_time();       // last access time
    buf->st_mtime = attr.modification_time(); // last modification time
    buf->st_ctime = attr.creation_time();     // creation or inode change time
    buf->st_ino = attr.inode();               // inode number
    buf->st_uid = attr.user_id();             // user ID of owner
    buf->st_gid = attr.group_id();            // group ID of owner

    return Status::OK();
}

Status Directory::readdir(void *buf, fuse_fill_dir_t filler,
                          fuse_readdir_flags /*flags*/) {
    auto [s, list] = metadata_->readdir(inode_);
    if (!s.ok()) {
        return s;
    }

    for (const auto &entry : list) {
        if (files_.find(entry.name()) == files_.end() &&
            subdirs_.find(entry.name()) == subdirs_.end()) {
            // create a new file or directory
            create_inode(entry.inode(), entry.name());
        }

        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = entry.inode();
        st.st_mode = S_IFREG | 0755; // Regular file with read/write/execute
        if (filler(buf, entry.name().c_str(), &st, 0,
                   static_cast<fuse_fill_dir_flags>(0))) {
            break;
        }
    }

    return Status::OK();
}

Status Directory::utimens(const struct timespec tv[2]) {
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

std::vector<std::shared_ptr<FileHandle>> Directory::list_files() {
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

Status Directory::create_inode(const uint64_t &inode, const std::string &name) {
    // first we need to check if it's a file or a directory
    auto it = files_.find(name);
    if (it != files_.end()) {
        return Status::AlreadyExists("File already exists");
    }
    auto it2 = subdirs_.find(name);
    if (it2 != subdirs_.end()) {
        return Status::AlreadyExists("Directory already exists");
    }
    // check if the inode is a file or a directory
    auto [s, attr] = metadata_->getattr(inode);
    if (!s.ok()) {
        return s;
    }

    if (attr.mode() & S_IFDIR) {
        // it's a directory
        auto new_dir = std::make_unique<Directory>(inode_, inode, name,
                                                   mount_path_, metadata_);
        auto s = new_dir->init();
        if (!s.ok()) {
            return s;
        }
        subdirs_[name] = std::move(new_dir);
    } else {
        // it's a file
        auto new_file = std::make_shared<FileHandle>(inode_, inode, name,
                                                     mount_path_, metadata_);
        auto s = new_file->init();
        if (!s.ok()) {
            return s;
        }
        files_[name] = new_file;
    }

    return Status::OK();
}

Status Directory::setattr(Attributes &attr) {
    // Update the file attributes in the metadata service.
    auto s = metadata_->setattr(inode_, attr);
    if (!s.ok()) {
        return s;
    }
    return Status::OK();
}