#ifndef DIRECTORY_MANAGER_H
#define DIRECTORY_MANAGER_H

#include "directory.h"
#include "file_handle.h"
#include "status.h"

#include <memory>
#include <vector>

class DirectoryManager {
  public:
    DirectoryManager(const std::string &mount_path)
        : root_(std::make_unique<Directory>("/", mount_path, nullptr)),
          mount_path_(mount_path) {}
    ~DirectoryManager() {}

    // Create a new directory
    std::pair<Status, Directory *> create_dir(const std::string &path);

    std::pair<Status, std::shared_ptr<FileHandle>>
    create_file(const std::string &path);

    std::pair<Status, Directory *> find_dir(const std::string &path);

    std::pair<Status, std::shared_ptr<FileHandle>>
    find_file(const std::string &path);

    bool is_dir(const std::string &path);
    bool is_file(const std::string &path);
    bool exists(const std::string &path);

    // Remove a directory
    std::pair<Status, std::unique_ptr<Directory>>
    remove_dir(const std::string &path);

    std::pair<Status, std::shared_ptr<FileHandle>>
    remove_file(const std::string &path);

    // List contents of a directory
    std::pair<Status, std::vector<std::shared_ptr<FileHandle>>>
    list_files(const std::string &path);

    std::pair<Status, std::vector<Directory *>>
    list_dirs(const std::string &path);

    Status rename_file(const std::string &oldpath, const std::string &newpath);
    Status rename_dir(const std::string &oldpath, const std::string &newpath);

  private:
    std::unique_ptr<Directory> root_; // Root directory
    std::string mount_path_;          // Mount path for the directory
};

#endif // DIRECTORY_MANAGER_H