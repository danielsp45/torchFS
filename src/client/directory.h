#ifndef DIRECTORY_H
#define DIRECTORY_H

#include "file_handle.h"
#include "fuse.h"
#include "status.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

class Directory {
  public:
    Directory(const std::string &logic_path, const std::string &mount_path)
        : name_(filename(logic_path)), logic_path_(logic_path),
          mount_path_(mount_path), subdirs_(), files_() {}

    ~Directory() = default;

    std::shared_ptr<FileHandle> get_file(const std::string name);

    Directory *get_dir(const std::string &name);

    std::pair<Status, std::shared_ptr<FileHandle>>
    create_file(const std::string &name);

    std::pair<Status, Directory *> create_subdirectory(const std::string &name);

    std::pair<Status, std::shared_ptr<FileHandle>>
    remove_file(const std::string name);

    std::pair<Status, std::unique_ptr<Directory>>
    remove_dir(const std::string name);

    Status move_file(std::shared_ptr<FileHandle> fh);
    Status move_dir(std::unique_ptr<Directory> dir);

    bool exists(const std::string &name);

    struct stat *get_meta();

    std::string get_name() const { return name_; }

    Status readdir(void *buf, fuse_fill_dir_t filler, fuse_readdir_flags flags);

    Status destroy();

    std::vector<std::shared_ptr<FileHandle>> list_files();
    std::vector<Directory *> list_dirs();

  private:
    std::string name_;       // Directory name
    std::string mount_path_; // Local absolute path of the directory
    std::string logic_path_;
    std::map<std::string, std::unique_ptr<Directory>>
        subdirs_; // Subdirectories in the directory
    std::map<std::string, std::shared_ptr<FileHandle>>
        files_; // File handles in the directory
};

#endif // DIRECTORY_H