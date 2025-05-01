#ifndef NAMESPACE_H
#define NAMESPACE_H

#include "directory.h"
#include "file_handle.h"
#include "metadata.h"
#include "status.h"

#include <memory>
#include <vector>

class Namespace {
  public:
    Namespace(const std::string &mount_path)
        : root_(std::make_unique<Directory>(
              0, 1, "/", mount_path, std::make_shared<MetadataStorage>())),
          mount_path_(mount_path) {
        // Initialize the root directory
        auto status = root_->init();
        if (!status.ok()) {
            throw std::runtime_error("Failed to initialize root directory");
        }
    }

    ~Namespace() {}

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

#endif // NAMESPACE_H
