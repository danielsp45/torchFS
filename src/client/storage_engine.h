#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include "directory.h"
#include "file_handle.h"
#include "fuse.h"
#include "status.h"
#include "cache.h"
#include "cache_policies/filo_policy.h"

#include <memory>

class StorageEngine {
  public:
    StorageEngine(const std::string &mount_path)
        : mount_path_(mount_path),
          root_(std::make_unique<Directory>(
              0, 1, "/", mount_path,
              std::make_shared<MetadataClient>(),
              std::make_shared<StorageClient>())),
          cache_(std::make_unique<FILOEvictionPolicy>()) {}

    ~StorageEngine() {}

    Status init();

    Status open(const std::string &path, int flagse, FilePointer **out_fp);
    Status create(const std::string &path);
    Status close(FilePointer *fp);
    Status unlink(const std::string path);
    Status read(FilePointer *fp, Slice result, size_t size, off_t offset);
    Status write(FilePointer *fp, Slice data, size_t size, off_t offset);
    Status sync(std::string path);
    Status rename(const std::string &oldpath, const std::string &newpath);
    Status getattr(const std::string &path, struct stat *stbuf);
    Status readdir(const std::string &path, void *buf, fuse_fill_dir_t filler,
                   fuse_readdir_flags flags);
    Status mkdir(const std::string &path);
    Status rmdir(const std::string &path);
    Status utimens(const std::string &path, const struct timespec tv[2]);
    Status lseek(FilePointer *fp, off_t offset, int whence, off_t *new_offset);

    std::string get_logic_path(const std::string &path);

  private:
    std::string mount_path_;          // Directory for local storage
    std::unique_ptr<Directory> root_; // Root directory
    Cache cache_;

    std::string register_fh(std::shared_ptr<FileHandle> fh);
    std::shared_ptr<FileHandle> lookup_fh(std::string &logic_path);
    std::shared_ptr<FileHandle> get_file_handle(std::string file_path);

    std::pair<Status, std::shared_ptr<FileHandle>>
    find_file(const std::string &path);
    std::pair<Status, Directory *> find_dir(const std::string &path);
    bool is_file(const std::string &path);
    bool is_dir(const std::string &path);
};

#endif // STORAGE_ENGINE_H
