// #include "directory.h"
// #include "file_handle.h"
// #include "status.h"
// #include "util.h"

// #include <cstring>
// #include <dirent.h>
// #include <memory>
// #include <shared_mutex>
// #include <string>
// #include <sys/stat.h> // Added for stat()

// Status Directory::init() {
//     if (inode_ == static_cast<uint64_t>(-1)) {
//         // we need to create the file in the metadata too
//         auto [s, attr] = metadata_->create_dir(p_inode_, filename(logic_path_));
//         if (!s.ok()) {
//             return s;
//         }
//         inode_ = attr.inode();
//     } else {
//         // we need to read the content of this directory
//         auto [s, dirent_list] = metadata_->readdir(inode_);
//         if (!s.ok()) {
//             return s;
//         }
//         // for each inode, we need to create a file/directory
//         for (const auto &dirent : dirent_list) {
//             this->create_inode(dirent.inode(), dirent.name());
//         }
//     }

//     return Status::OK();
// }

// Status Directory::destroy() {
//     std::unique_lock lk(mu_);

//     // destroy all of the files in this directory
//     for (auto &entry : files_) {
//         auto [s1, fh] = remove_file(entry.first);
//         if (!s1.ok()) {
//             return s1;
//         }
//         fh->destroy();
//     }

//     // destroy all of the subdirectories in this directory
//     for (auto &entry : subdirs_) {
//         auto [s1, dir] = remove_dir(entry.first);
//         if (!s1.ok()) {
//             return s1;
//         }
//         dir->destroy();
//     }

//     // Destroy the directory by removing it from the metadata service.
//     return metadata_->remove_dir(p_inode_, inode_, filename(logic_path_));
// }

// // Look up an existing file in the directory.
// std::shared_ptr<FileHandle> Directory::get_file(const std::string name) {
//     std::shared_lock lk(mu_);

//     auto it = files_.find(name);
//     if (it != files_.end()) {
//         return it->second;
//     }
//     return nullptr;
// }

// // Look up a subdirectory by name.
// Directory *Directory::get_dir(const std::string &name) {
//     std::shared_lock lk(mu_);

//     auto it = subdirs_.find(name);
//     if (it != subdirs_.end()) {
//         return it->second.get();
//     }
//     return nullptr;
// }

// std::pair<Status, std::shared_ptr<FileHandle>>
// Directory::create_file(const std::string &name) {
//     std::unique_lock lk(mu_);

//     if (files_.find(name) != files_.end()) {
//         return {Status::AlreadyExists("File already exists"), nullptr};
//     }

//     std::string new_path = join_paths(logic_path_, name);
//     auto fh = std::make_shared<FileHandle>(inode_, -1, new_path, mount_path_,
//                                            metadata_, storage_);
//     auto s = fh->init();
//     if (!s.ok()) {
//         // NOTE: if the file creation fails, since it's a shared_ptr, the
//         // the destructor will be called automatically
//         return {s, nullptr};
//     }

//     files_[name] = fh;
//     return {Status::OK(), fh};
// }

// std::pair<Status, Directory *>
// Directory::create_subdirectory(const std::string &name) {
//     std::unique_lock lk(mu_);

//     if (subdirs_.count(name)) {
//         return {Status::AlreadyExists("Directory already exists"), nullptr};
//     }

//     std::string new_path = join_paths(logic_path_, name);
//     auto new_dir = std::make_unique<Directory>(
//         inode_, -1, new_path, mount_path_, metadata_, storage_);
//     auto s = new_dir->init();
//     if (!s.ok()) {
//         return {s, nullptr};
//     }

//     // take raw pointer before moving
//     Directory *dir_ptr = new_dir.get();
//     subdirs_[name] = std::move(new_dir);

//     return {Status::OK(), dir_ptr};
// }

// std::pair<Status, std::shared_ptr<FileHandle>>
// Directory::remove_file(const std::string name, bool delete_fh) {
//     std::unique_lock lk(mu_);

//     auto it = files_.find(name);
//     if (it == files_.end()) {
//         return {Status::NotFound("File not found"), nullptr};
//     }
//     auto fh = it->second;

//     files_.erase(it);

//     if (delete_fh) {
//         // Remove the file from the metadata service
//         return {fh->destroy(), fh};
//     } else {
//         // If not removing from metadata, just return the file handle
//         return {Status::OK(), fh};
//     }
// }

// std::pair<Status, std::unique_ptr<Directory>>
// Directory::remove_dir(const std::string name, bool delete_dir) {
//     std::unique_lock lk(mu_);

//     auto it = subdirs_.find(name);
//     if (it == subdirs_.end()) {
//         return {Status::NotFound("Directory not found"), nullptr};
//     }

//     auto dir_ptr = std::move(it->second);
//     subdirs_.erase(it);

//     if (delete_dir) {
//         auto s = metadata_->remove_dir(inode_, dir_ptr->get_inode(), name);
//         return {s, std::move(dir_ptr)};
//     }
//     return {Status::OK(), std::move(dir_ptr)};
// }

// Status Directory::move_file(Directory *parent_dir,
//                             std::shared_ptr<FileHandle> fh,
//                             const std::string &new_name) {
//     std::unique_lock lk(mu_);

//     // Ensure the file doesn't already exist in the target directory.
//     if (files_.find(new_name) != files_.end()) {
//         return Status::AlreadyExists(
//             "Target file already exists in target directory");
//     }

//     // Rename the file in the metadata.
//     auto s = metadata_->rename_file(parent_dir->get_inode(), inode_,
//                                     fh->get_inode(), new_name);
//     if (!s.ok()) {
//         return s;
//     }

//     // Remove the file from the parent directory.
//     auto [s1, removed_file] = parent_dir->remove_file(fh->get_name(), false);
//     if (!s1.ok()) {
//         return Status::NotFound("File not found in parent directory");
//     }

//     // Update file's internal state for its new location and name.
//     removed_file->set_logic_path(join_paths(logic_path_, new_name));
//     removed_file->set_parent_inode(inode_);

//     // Add the removed file to this directory.
//     files_[new_name] = removed_file;
//     return Status::OK();
// }

// Status Directory::move_dir(Directory *parent_dir,
//                            std::unique_ptr<Directory> dir,
//                            const std::string &new_name) {
//     std::unique_lock lk(mu_);

//     auto it = subdirs_.find(dir->get_name());
//     if (it != subdirs_.end()) {
//         return Status::AlreadyExists("Directory already exists");
//     }

//     auto s = metadata_->rename_dir(parent_dir->get_inode(), inode_,
//                                    dir->get_inode(), new_name);
//     if (!s.ok()) {
//         return s;
//     }

//     // Rename the directory
//     dir->set_logic_path(join_paths(logic_path_, new_name));
//     dir->set_parent_inode(inode_);

//     // Move the directory to the new location.
//     subdirs_[dir->get_name()] = std::move(dir);
//     return Status::OK();
// }

// Status Directory::getattr(struct stat *buf) {
//     std::shared_lock lk(mu_);

//     // Get the file/directory attributes from the metadata service.
//     auto [s, attr] = metadata_->getattr(inode_);
//     if (!s.ok()) {
//         return s;
//     }

//     // Transfer the data from Attributes to the stat structure.
//     // buf->st_mode = attr.mode(); // e.g., file type and permissions
//     buf->st_mode = attr.mode();               // e.g., file type and permissions
//     buf->st_size = attr.size();               // file size in bytes
//     buf->st_atime = attr.access_time();       // last access time
//     buf->st_mtime = attr.modification_time(); // last modification time
//     buf->st_ctime = attr.creation_time();     // creation or inode change time
//     buf->st_ino = attr.inode();               // inode number
//     buf->st_uid = attr.user_id();             // user ID of owner
//     buf->st_gid = attr.group_id();            // group ID of owner

//     return Status::OK();
// }

// Status Directory::readdir(void *buf, fuse_fill_dir_t filler,
//                           fuse_readdir_flags /*flags*/) {
//     std::shared_lock lk(mu_);

//     auto [s, list] = metadata_->readdir(inode_);
//     if (!s.ok()) {
//         return s;
//     }

//     for (const auto &entry : list) {
//         if (files_.find(entry.name()) == files_.end() &&
//             subdirs_.find(entry.name()) == subdirs_.end()) {
//             // create a new file or directory
//             create_inode(entry.inode(), entry.name());
//         }

//         struct stat st;
//         memset(&st, 0, sizeof(st));
//         st.st_ino = entry.inode();
//         st.st_mode = S_IFREG | 0755; // Regular file with read/write/execute
//         if (filler(buf, entry.name().c_str(), &st, 0,
//                    static_cast<fuse_fill_dir_flags>(0))) {
//             break;
//         }
//     }

//     return Status::OK();
// }

// Status Directory::utimens(const struct timespec tv[2]) {
//     std::unique_lock lk(mu_);

//     // Update the file access and modification times.
//     Attributes attr = metadata_->getattr(inode_).second;
//     attr.set_access_time(tv[0].tv_sec);
//     attr.set_modification_time(tv[1].tv_sec);

//     Status s = setattr(attr);
//     if (!s.ok()) {
//         return s;
//     }

//     return Status::OK();
// }

// std::vector<std::shared_ptr<FileHandle>> Directory::list_files() {
//     std::shared_lock lk(mu_);

//     std::vector<std::shared_ptr<FileHandle>> files;
//     for (auto &entry : files_) {
//         files.push_back(entry.second);
//     }
//     return files;
// }

// std::vector<Directory *> Directory::list_dirs() {
//     std::shared_lock lk(mu_);

//     std::vector<Directory *> dirs;
//     for (auto &entry : subdirs_) {
//         dirs.push_back(entry.second.get());
//     }
//     return dirs;
// }

// Status Directory::create_inode(const uint64_t &inode, const std::string &name) {
//     std::unique_lock lk(mu_);

//     // first we need to check if it's a file or a directory
//     auto it = files_.find(name);
//     if (it != files_.end()) {
//         return Status::AlreadyExists("File already exists");
//     }
//     auto it2 = subdirs_.find(name);
//     if (it2 != subdirs_.end()) {
//         return Status::AlreadyExists("Directory already exists");
//     }
//     // check if the inode is a file or a directory
//     auto [s, attr] = metadata_->getattr(inode);
//     if (!s.ok()) {
//         return s;
//     }

//     if (attr.mode() & S_IFDIR) {
//         // it's a directory
//         auto new_dir = std::make_unique<Directory>(
//             inode_, inode, name, mount_path_, metadata_, storage_);
//         auto s = new_dir->init();
//         if (!s.ok()) {
//             return s;
//         }
//         subdirs_[name] = std::move(new_dir);
//     } else {
//         // it's a file
//         auto new_file = std::make_shared<FileHandle>(
//             inode_, inode, name, mount_path_, metadata_, storage_);
//         auto s = new_file->init();
//         if (!s.ok()) {
//             return s;
//         }
//         files_[name] = new_file;
//     }

//     return Status::OK();
// }

// Status Directory::setattr(Attributes &attr) {
//     std::unique_lock lk(mu_);

//     // Update the file attributes in the metadata service.
//     auto s = metadata_->setattr(attr);
//     if (!s.ok()) {
//         return s;
//     }
//     return Status::OK();
// }

#include "directory.h"
#include "file_handle.h"
#include "status.h"
#include "util.h"

#include <cstring>      // for memset
#include <dirent.h>     // for FUSE
#include <fcntl.h>
#include <memory>
#include <shared_mutex>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

//
// === init() / destroy() ===
//

Status Directory::init() {
    // If inode_ is “unassigned” (-1), create a brand-new directory via metadata.
    if (inode_ == static_cast<uint64_t>(-1)) {
        auto [s, attr] = metadata_->create_dir(p_inode_, filename(logic_path_));
        if (!s.ok()) {
            return s;
        }
        inode_ = attr.inode();
    } else {
        // Otherwise, fetch existing entries from metadata:
        auto [s, dirent_list] = metadata_->readdir(inode_);
        if (!s.ok()) {
            return s;
        }

        // For each entry returned, insert into files_ or subdirs_ as appropriate.
        for (const auto &dirent : dirent_list) {
            // We need to acquire mu_ exclusively before mutating files_/subdirs_.
            std::unique_lock lk(mu_);
            Status s2 = _create_inode_unlocked(dirent.inode(), dirent.name());
            if (!s2.ok()) {
                return s2;
            }
        }
    }
    return Status::OK();
}

Status Directory::destroy() {
    // 1) First, take an exclusive lock to snapshot all children.
    std::unique_lock lk(mu_);

    // Collect all FileHandle pointers so that we can call destroy() on each outside the lock.
    std::vector<std::shared_ptr<FileHandle>> files_to_destroy;
    files_to_destroy.reserve(files_.size());
    for (auto & kv : files_) {
        files_to_destroy.push_back(kv.second);
    }
    files_.clear();

    // Collect all subdirectory pointers so we can call destroy() on each outside the lock.
    std::vector<Directory *> dirs_to_destroy;
    dirs_to_destroy.reserve(subdirs_.size());
    for (auto & kv : subdirs_) {
        dirs_to_destroy.push_back(kv.second.get());
    }
    subdirs_.clear();

    // Destroy all files (each FileHandle::destroy() will handle its own locking internally)
    for (auto &fh : files_to_destroy) {
        fh->destroy();
    }

    // Destroy all subdirectories (each Directory::destroy() will handle its own locking)
    for (auto *d : dirs_to_destroy) {
        d->destroy();
    }

    // 3) Finally, remove this directory entry from its parent via metadata RPC:
    return metadata_->remove_dir(p_inode_, inode_, filename(logic_path_));
}

//
// === get_file() / get_dir() ===
//    (readers only, hold shared lock on mu_)
//

std::shared_ptr<FileHandle> Directory::get_file(const std::string &name) {
    std::shared_lock lk(mu_);
    auto it = files_.find(name);
    if (it != files_.end()) {
        return it->second;
    }
    return nullptr;
}

Directory *Directory::get_dir(const std::string &name) {
    std::shared_lock lk(mu_);
    auto it = subdirs_.find(name);
    if (it != subdirs_.end()) {
        return it->second.get();
    }
    return nullptr;
}

//
// === create_file / create_subdirectory ===
//    (writers, hold unique lock on mu_)
//

std::pair<Status, std::shared_ptr<FileHandle>>
Directory::create_file(const std::string &name) {
    std::unique_lock lk(mu_);
    return _create_file_unlocked(name);
}

std::pair<Status, Directory *>
Directory::create_subdirectory(const std::string &name) {
    std::unique_lock lk(mu_);
    return _create_subdir_unlocked(name);
}

std::pair<Status, std::shared_ptr<FileHandle>>
Directory::_create_file_unlocked(const std::string &name) {
    // (caller holds mu_ already)

    // Check for name collisions
    if (files_.count(name)) {
        return { Status::AlreadyExists("File already exists"), nullptr };
    }
    if (subdirs_.count(name)) {
        return { Status::AlreadyExists("A directory with that name already exists"), nullptr };
    }

    // Build the new-file path
    std::string new_path = join_paths(logic_path_, name);
    auto fh = std::make_shared<FileHandle>(
        /*p_inode=*/ inode_,
        /*inode=*/ static_cast<uint64_t>(-1), // assigned inside init()
        new_path,
        mount_path_,
        metadata_,
        storage_);

    // We must release mu_ while calling fh->init(), because fh->init() has its own locking.
    mu_.unlock();
    Status s = fh->init();
    mu_.lock();
    if (!s.ok()) {
        return { s, nullptr };
    }

    files_.emplace(name, fh);
    return { Status::OK(), fh };
}

std::pair<Status, Directory *>
Directory::_create_subdir_unlocked(const std::string &name) {
    // (caller holds mu_ already)

    // Check for name collisions
    if (subdirs_.count(name)) {
        return { Status::AlreadyExists("Directory already exists"), nullptr };
    }
    if (files_.count(name)) {
        return { Status::AlreadyExists("A file with that name already exists"), nullptr };
    }

    // Build the new-dir path
    std::string new_path = join_paths(logic_path_, name);
    auto new_dir = std::make_unique<Directory>(
        /*p_inode=*/ inode_,
        /*inode=*/ static_cast<uint64_t>(-1), // assigned in init()
        new_path,
        mount_path_,
        metadata_,
        storage_);

    // We must release mu_ while calling new_dir->init(), because new_dir->init() will lock new_dir->mu_.
    Status s = new_dir->init();
    if (!s.ok()) {
        return { s, nullptr };
    }

    Directory *dir_ptr = new_dir.get();
    subdirs_.emplace(name, std::move(new_dir));
    return { Status::OK(), dir_ptr };
}

//
// === remove_file / remove_dir ===
//    (writers, hold unique lock on mu_)
//

std::pair<Status, std::shared_ptr<FileHandle>>
Directory::remove_file(const std::string &name, bool delete_fh) {
    std::unique_lock lk(mu_);
    return _remove_file_unlocked(name, delete_fh);
}

std::pair<Status, std::unique_ptr<Directory>>
Directory::remove_dir(const std::string &name, bool delete_dir) {
    std::unique_lock lk(mu_);
    return _remove_dir_unlocked(name, delete_dir);
}

std::pair<Status, std::shared_ptr<FileHandle>>
Directory::_remove_file_unlocked(const std::string &name, bool delete_fh) {
    // (caller holds mu_ already)

    auto it = files_.find(name);
    if (it == files_.end()) {
        return { Status::NotFound("File not found"), nullptr };
    }
    auto fh = it->second;
    files_.erase(it);

    if (delete_fh) {
        Status s = fh->destroy();
        return { s, fh };
    } else {
        return { Status::OK(), fh };
    }
}

std::pair<Status, std::unique_ptr<Directory>>
Directory::_remove_dir_unlocked(const std::string &name, bool delete_dir) {
    // (caller holds mu_ already)

    auto it = subdirs_.find(name);
    if (it == subdirs_.end()) {
        return { Status::NotFound("Directory not found"), nullptr };
    }
    auto dir_ptr = std::move(it->second);
    subdirs_.erase(it);

    if (delete_dir) {
        Status s = metadata_->remove_dir(inode_, dir_ptr->get_inode(), name);
        return { s, std::move(dir_ptr) };
    } else {
        return { Status::OK(), std::move(dir_ptr) };
    }
}

//
// === move_file ===
//

Status Directory::move_file(Directory *parent_dir,
                            std::shared_ptr<FileHandle> fh,
                            const std::string &new_name)
{
    // If parent_dir == this, do a “rename within same directory”
    if (parent_dir == this) {
        std::unique_lock lk(mu_);
        return _move_file_within_same_dir_unlocked(fh, new_name);
    }

    // Otherwise, lock both directories’ mu_ in a fixed order (pointer comparison).
    Directory *first  = (this < parent_dir ? this : parent_dir);
    Directory *second = (this < parent_dir ? parent_dir : this);

    // Acquire both locks without risk of deadlock:
    std::unique_lock lk1(first->mu_, std::defer_lock);
    std::unique_lock lk2(second->mu_, std::defer_lock);
    std::lock(lk1, lk2);
    // Now we hold both this->mu_ and parent_dir->mu_ simultaneously.

    // 1) Check collision in “this” directory:
    if (files_.count(new_name)) {
        return Status::AlreadyExists("Target file already exists in this directory");
    }

    // 2) Rename in metadata (RPC):
    Status s = metadata_->rename_file(
        parent_dir->get_inode(),
        inode_,
        fh->get_inode(),
        new_name);
    if (!s.ok()) {
        return s;
    }

    // 3) Remove from parent_dir’s in-memory map (unlocked helper):
    auto [s2, removed_fh] = parent_dir->_remove_file_unlocked(fh->get_name(), /*delete_fh=*/false);
    if (!s2.ok()) {
        return s2;
    }

    // 4) Update FileHandle’s in-memory state
    removed_fh->set_logic_path(join_paths(logic_path_, new_name));
    removed_fh->set_parent_inode(inode_);

    // 5) Insert into this directory’s map
    files_.emplace(new_name, std::move(removed_fh));
    return Status::OK();
}

Status Directory::move_dir(Directory *parent_dir,
                           std::unique_ptr<Directory> dir,
                           const std::string &new_name)
{
    // If parent_dir == this, “rename within the same directory” logic:
    if (parent_dir == this) {
        std::unique_lock lk(mu_);
        return _move_dir_within_same_dir_unlocked(std::move(dir), new_name);
    }

    // Otherwise, lock both directories’ mu_ in a fixed order:
    Directory *first  = (this < parent_dir ? this : parent_dir);
    Directory *second = (this < parent_dir ? parent_dir : this);

    std::unique_lock lk1(first->mu_, std::defer_lock);
    std::unique_lock lk2(second->mu_, std::defer_lock);
    std::lock(lk1, lk2);

    // 1) Check name collision in “this”
    if (subdirs_.count(new_name)) {
        return Status::AlreadyExists("Target directory already exists");
    }

    // 2) Rename in metadata (RPC)
    Status s = metadata_->rename_dir(
        parent_dir->get_inode(),
        inode_,
        dir->get_inode(),
        new_name);
    if (!s.ok()) return s;

    // 3) Remove from parent_dir’s map via unlocked helper
    auto [s2, removed_dir] = parent_dir->_remove_dir_unlocked(dir->get_name(), /*delete_dir=*/false);
    if (!s2.ok()) {
        return s2;
    }

    // 4) Update in-memory state for that Directory
    removed_dir->logic_path_ = join_paths(logic_path_, new_name);
    removed_dir->p_inode_    = inode_;

    // 5) Insert into this directory’s subdirs_
    subdirs_.emplace(new_name, std::move(removed_dir));
    return Status::OK();
}

//
// === getattr / readdir / utimens / list_… ===
//

Status Directory::getattr(struct stat *buf) {
    std::shared_lock lk(mu_);
    auto [s, attr] = metadata_->getattr(inode_);
    if (!s.ok()) {
        return s;
    }
    buf->st_mode = attr.mode();
    buf->st_size = attr.size();
    buf->st_atime = attr.access_time();
    buf->st_mtime = attr.modification_time();
    buf->st_ctime = attr.creation_time();
    buf->st_ino  = attr.inode();
    buf->st_uid  = attr.user_id();
    buf->st_gid  = attr.group_id();
    return Status::OK();
}

Status Directory::readdir(void *buf, fuse_fill_dir_t filler, fuse_readdir_flags /*flags*/) {
    std::unique_lock lk(mu_);
    auto [s, list] = metadata_->readdir(inode_);
    if (!s.ok()) {
        return s;
    }

    for (const auto &entry : list) {
        if (!files_.count(entry.name()) && !subdirs_.count(entry.name())) {
            // We need to insert an in-memory entry first via create_inode:
            Status s2 = _create_inode_unlocked(entry.inode(), entry.name());
            if (!s2.ok()) {
                return s2;
            }
        }

        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino  = entry.inode();
        st.st_mode = S_IFREG | 0755;
        if (filler(buf, entry.name().c_str(), &st, 0,
                   static_cast<fuse_fill_dir_flags>(0))) {
            break;
        }
    }
    return Status::OK();
}

Status Directory::utimens(const struct timespec tv[2]) {
    std::unique_lock lk(mu_);
    auto [s_attr, attr] = metadata_->getattr(inode_);
    if (!s_attr.ok()) {
        return s_attr;
    }
    attr.set_access_time(tv[0].tv_sec);
    attr.set_modification_time(tv[1].tv_sec);
    return metadata_->setattr(attr);
}

std::vector<std::shared_ptr<FileHandle>> Directory::list_files() {
    std::shared_lock lk(mu_);
    std::vector<std::shared_ptr<FileHandle>> result;
    result.reserve(files_.size());
    for (auto & kv : files_) {
        result.push_back(kv.second);
    }
    return result;
}

std::vector<Directory *> Directory::list_dirs() {
    std::shared_lock lk(mu_);
    std::vector<Directory *> result;
    result.reserve(subdirs_.size());
    for (auto & kv : subdirs_) {
        result.push_back(kv.second.get());
    }
    return result;
}

//
// === _create_inode_unlocked ===
//

Status Directory::_create_inode_unlocked(const uint64_t &inode, const std::string &name) {
    if (files_.count(name)) {
        return Status::AlreadyExists("File already exists");
    }
    if (subdirs_.count(name)) {
        return Status::AlreadyExists("Directory already exists");
    }

    auto [s_attr, attr] = metadata_->getattr(inode);
    if (!s_attr.ok()) {
        return s_attr;
    }

    if (attr.mode() & S_IFDIR) {
        // Subdirectory case
        auto new_dir = std::make_unique<Directory>(
            /*p_inode=*/ inode,
            /*inode=*/ inode,
            join_paths(logic_path_, name),
            mount_path_,
            metadata_,
            storage_);

        // Release lock before calling init on the child
        Status s2 = new_dir->init();
        if (!s2.ok()) {
            return s2;
        }

        subdirs_.emplace(name, std::move(new_dir));
    } else {
        // File case
        auto new_file = std::make_shared<FileHandle>(
            /*p_inode=*/ inode,
            /*inode=*/ inode,
            join_paths(logic_path_, name),
            mount_path_,
            metadata_,
            storage_);

        Status s2 = new_file->init();
        if (!s2.ok()) {
            return s2;
        }

        files_.emplace(name, std::move(new_file));
    }
    return Status::OK();
}

//
// === _move within same directory (unlocked) ===
//

Status Directory::_move_file_within_same_dir_unlocked(std::shared_ptr<FileHandle> fh,
                                                      const std::string &new_name)
{
    // (caller holds mu_)

    const std::string old_name = fh->get_name();
    if (!files_.count(old_name)) {
        return Status::NotFound("File not found in this directory");
    }
    if (files_.count(new_name)) {
        return Status::AlreadyExists("Target file name already exists");
    }

    // Metadata RPC
    Status s = metadata_->rename_file(
        /*old_parent=*/ inode_,
        /*new_parent=*/ inode_,
        /*inode=*/ fh->get_inode(),
        /*new_name=*/ new_name);
    if (!s.ok()) return s;

    // Update in-memory
    files_.erase(old_name);
    fh->set_logic_path(join_paths(logic_path_, new_name));
    files_.emplace(new_name, std::move(fh));
    return Status::OK();
}

Status Directory::_move_dir_within_same_dir_unlocked(std::unique_ptr<Directory> dir,
                                                     const std::string &new_name)
{
    // (caller holds mu_)

    const std::string old_name = dir->get_name();
    if (!subdirs_.count(old_name)) {
        return Status::NotFound("Directory not found in this directory");
    }
    if (subdirs_.count(new_name)) {
        return Status::AlreadyExists("Target directory name already exists");
    }

    // Metadata RPC
    Status s = metadata_->rename_dir(
        /*old_parent=*/ inode_,
        /*new_parent=*/ inode_,
        /*inode=*/ dir->get_inode(),
        /*new_name=*/ new_name);
    if (!s.ok()) return s;

    // Update in-memory
    subdirs_.erase(old_name);
    dir->logic_path_ = join_paths(logic_path_, new_name);
    dir->p_inode_    = inode_;
    subdirs_.emplace(new_name, std::move(dir));
    return Status::OK();
}

//
// === Helpers to convert between stat ↔ Attributes (not strictly used above) ===
//

void Directory::stat_to_attr(const struct stat &st, Attributes &a) {
    a.set_mode(st.st_mode);
    a.set_size(st.st_size);
    a.set_access_time(st.st_atime);
    a.set_modification_time(st.st_mtime);
    a.set_creation_time(st.st_ctime);
    a.set_user_id(st.st_uid);
    a.set_group_id(st.st_gid);
    a.set_inode(inode_);
    a.set_path(logic_path_);
}

void Directory::attr_to_stat(const Attributes &a, struct stat *st) {
    st->st_mode = a.mode();
    st->st_size = a.size();
    st->st_atime = a.access_time();
    st->st_mtime = a.modification_time();
    st->st_ctime = a.creation_time();
    st->st_uid  = a.user_id();
    st->st_gid  = a.group_id();
    st->st_ino  = a.inode();
}
