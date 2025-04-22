#include "namespace.h"
#include "directory.h"
#include "status.h"

#include <iostream>

std::pair<Status, Directory *>
Namespace::create_dir(const std::string &path) {
    auto [dir_name, new_dir_name] = split_path_from_target(path);
    if (new_dir_name.empty()) {
        return {Status::InvalidArgument("Invalid path"), nullptr};
    }

    // Get the parent directory
    auto [s, parent_dir] = find_dir(dir_name);
    if (!s.ok()) {
        return {s, nullptr};
    }

    // Check if the directory already exists
    auto dir = parent_dir->get_dir(new_dir_name);
    if (dir) {
        return {Status::AlreadyExists("Directory already exists"), nullptr};
    }

    // Create the new directory
    return parent_dir->create_subdirectory(new_dir_name);
}

std::pair<Status, std::shared_ptr<FileHandle>>
Namespace::create_file(const std::string &path) {
    auto [dir_name, file_name] = split_path_from_target(path);
    if (file_name.empty()) {
        return {Status::InvalidArgument("Invalid path"), nullptr};
    }

    auto [s, parent_dir] = find_dir(dir_name);
    if (!s.ok()) {
        return {s, nullptr};
    }

    if (parent_dir->get_file(file_name)) {
        return {Status::AlreadyExists("File already exists"), nullptr};
    }

    return parent_dir->create_file(file_name);
}

std::pair<Status, Directory *>
Namespace::find_dir(const std::string &path) {
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

std::pair<Status, std::shared_ptr<FileHandle>>
Namespace::find_file(const std::string &path) {
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

bool Namespace::is_file(const std::string &path) {
    if (path == "/") {
        return false;
    }
    std::cout << path << std::endl;
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

bool Namespace::is_dir(const std::string &path) {
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

bool Namespace::exists(const std::string &path) {
    auto [dir_name, target] = split_path_from_target(path);
    auto [s, parent_dir] = find_dir(dir_name);
    if (target.empty() || !s.ok()) {
        return false;
    }

    // Check if the target is a file or directory
    auto fh = parent_dir->get_file(target);
    auto subdir = parent_dir->get_dir(target);

    if (fh || subdir) {
        return true;
    }
    return false;
}

std::pair<Status, std::unique_ptr<Directory>>
Namespace::remove_dir(const std::string &path) {
    auto [dir_name, target] = split_path_from_target(path);
    auto [s, parent_dir] = find_dir(dir_name);
    if (target.empty() || !s.ok()) {
        return {Status::InvalidArgument("Invalid path"), nullptr};
    }

    // Check if the directory exists
    auto subdir = parent_dir->get_dir(target);
    if (!subdir) {
        return {Status::NotFound("Directory not found"), nullptr};
    }

    return parent_dir->remove_dir(target);
}

std::pair<Status, std::shared_ptr<FileHandle>>
Namespace::remove_file(const std::string &path) {
    auto [dir_name, target] = split_path_from_target(path);
    auto [s, parent_dir] = find_dir(dir_name);
    if (target.empty() || !s.ok()) {
        return {Status::InvalidArgument("Invalid path"), nullptr};
    }

    auto fh = parent_dir->get_file(target);
    if (!fh) {
        return {Status::NotFound("File not found"), nullptr};
    }

    // Attempt to remove the file
    return parent_dir->remove_file(target);
}

std::pair<Status, std::vector<std::shared_ptr<FileHandle>>>
Namespace::list_files(const std::string &path) {
    auto [dir_name, target_name] = split_path_from_target(path);
    auto [s, dir] = find_dir(dir_name);
    if (target_name.empty() || !s.ok()) {
        return {Status::InvalidArgument("Invalid path"), {}};
    }

    // List files in the directory
    std::vector<std::shared_ptr<FileHandle>> files = dir->list_files();
    return {Status::OK(), files};
}

std::pair<Status, std::vector<Directory *>>
Namespace::list_dirs(const std::string &path) {
    auto [dir_name, target_name] = split_path_from_target(path);
    auto [s, dir] = find_dir(dir_name);
    if (target_name.empty() || !s.ok()) {
        return {Status::InvalidArgument("Invalid path"), {}};
    }
    // List directories in the directory
    std::vector<Directory *> dirs = dir->list_dirs();
    return {Status::OK(), dirs};
}

Status Namespace::rename_file(const std::string &oldpath,
                                     const std::string &newpath) {
    auto [old_dir_name, old_filename] = split_path_from_target(oldpath);
    auto [new_dir_name, new_filename] = split_path_from_target(newpath);

    if (old_filename.empty() || new_filename.empty()) {
        return Status::InvalidArgument("Invalid path");
    }

    // Get the parent directories
    auto [s, old_dir_ptr] = find_dir(old_dir_name);
    if (!s.ok()) {
        return s;
    }
    Directory *old_dir = old_dir_ptr;

    auto [s2, new_dir_ptr] = find_dir(new_dir_name);
    if (!s2.ok()) {
        return s2;
    }
    Directory *new_dir = new_dir_ptr;

    auto [s1, file_handle] = old_dir->remove_file(old_filename);
    if (!s1.ok()) {
        return s1;
    }
    // Move the file to the new directory
    Status move_status = new_dir->move_file(file_handle);
    if (!move_status.ok()) {
        return move_status;
    }

    return Status::OK();
}

Status Namespace::rename_dir(const std::string &oldpath,
                                    const std::string &newpath) {
    auto [old_dir_name, old_dirname] = split_path_from_target(oldpath);
    auto [new_dir_name, new_dirname] = split_path_from_target(newpath);

    if (old_dirname.empty() || new_dirname.empty()) {
        return Status::InvalidArgument("Invalid path");
    }

    // Get the parent directories
    auto [s, old_dir_ptr] = find_dir(old_dir_name);
    if (!s.ok()) {
        return s;
    }
    Directory *old_dir = old_dir_ptr;

    auto [s2, new_dir_ptr] = find_dir(new_dir_name);
    if (!s2.ok()) {
        return s2;
    }
    Directory *new_dir = new_dir_ptr;

    // Check if the directory exists in the old directory
    auto [s1, subdir] = old_dir->remove_dir(old_dirname);
    if (!s1.ok()) {
        return s1;
    }

    // Move the directory to the new directory
    Status move_status = new_dir->move_dir(std::move(subdir));
    if (!move_status.ok()) {
        return move_status;
    }

    return Status::OK();
}
