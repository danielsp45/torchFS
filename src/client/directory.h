#ifndef DIRECTORY_H
#define DIRECTORY_H

#include "file_handle.h"
#include "fuse.h"
#include "metadata_client.h"
#include "status.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

class Directory {
  public:
    Directory(const uint64_t &p_inode, const uint64_t &inode,
              const std::string &logic_path, const std::string &mount_path,
              std::shared_ptr<MetadataClient> metadata_client,
              std::shared_ptr<StorageClient> storage_client)
        : p_inode_(p_inode), inode_(inode), mount_path_(mount_path),
          logic_path_(logic_path), subdirs_(), files_(),
          metadata_(metadata_client), storage_(storage_client) {}

    ~Directory() = default;

    Status init();

    Status destroy();

    std::shared_ptr<FileHandle> get_file(const std::string name);

    Directory *get_dir(const std::string &name);

    std::pair<Status, std::shared_ptr<FileHandle>>
    create_file(const std::string &name);

    std::pair<Status, Directory *> create_subdirectory(const std::string &name);

    std::pair<Status, std::shared_ptr<FileHandle>>
    remove_file(const std::string name, bool delete_fh = true);

    std::pair<Status, std::unique_ptr<Directory>>
    remove_dir(const std::string name);

    Status move_file(Directory *old_dir, std::shared_ptr<FileHandle> fh,
                     const std::string &new_name);
    Status move_dir(Directory *old_dir, std::unique_ptr<Directory> dir,
                    const std::string &new_name);

    bool exists(const std::string &name);

    Status getattr(struct stat *buf);

    uint64_t get_inode() const { return inode_; }

    std::string get_name() const { return filename(logic_path_); }

    void set_logic_path(const std::string &logic_path) {
        logic_path_ = logic_path;
    }

    void set_parent_inode(const uint64_t &p_inode) { p_inode_ = p_inode; }

    Status readdir(void *buf, fuse_fill_dir_t filler, fuse_readdir_flags flags);

    Status utimens(const struct timespec tv[2]);

    std::vector<std::shared_ptr<FileHandle>> list_files();
    std::vector<Directory *> list_dirs();

  private:
    uint64_t p_inode_;       // Unique identifier for the directory
    uint64_t inode_;         // Unique identifier for the directory
    std::string mount_path_; // Local absolute path of the directory
    std::string logic_path_;
    std::map<std::string, std::unique_ptr<Directory>>
        subdirs_; // Subdirectories in the directory
    std::map<std::string, std::shared_ptr<FileHandle>>
        files_; // File handles in the directory
    std::shared_ptr<MetadataClient> metadata_;
    std::shared_ptr<StorageClient> storage_;

    Status create_inode(const uint64_t &inode, const std::string &name);

    Status setattr(Attributes &attr);
};

#endif // DIRECTORY_H