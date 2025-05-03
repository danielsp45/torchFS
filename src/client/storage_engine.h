#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include "file_handle.h"
#include "fuse.h"
#include "namespace.h"
#include "status.h"

#include <cstdint>
#include <map>
#include <memory>

class StorageEngine {
  public:
    StorageEngine(const std::string &mount_path)
        : namespace_(std::make_unique<Namespace>(mount_path)),
          mount_path_(mount_path) {}

    ~StorageEngine() {}

    Status init();

    Status open(const std::string &path, int flagse);
    Status create(const std::string &path, int flags, mode_t mode);
    Status close(std::string &path);
    Status remove(const std::string path);
    Status read(std::string &path, Slice result, size_t size, off_t offset);
    Status write(std::string &path, Slice data, size_t size, off_t offset);
    Status sync(std::string path);
    Status rename(const std::string &oldpath, const std::string &newpath);
    Status getattr(const std::string &path, struct stat *stbuf);
    Status readdir(const std::string &path, void *buf, fuse_fill_dir_t filler,
                   fuse_readdir_flags flags);
    Status mkdir(const std::string &path, mode_t mode);
    Status rmdir(const std::string &path);

    std::string get_logic_path(const std::string &path);

  private:
    std::unique_ptr<Namespace> namespace_;
    std::string mount_path_; // Directory for local storage

    std::string register_fh(std::shared_ptr<FileHandle> fh);
    std::shared_ptr<FileHandle> lookup_fh(std::string &logic_path);

    std::shared_ptr<FileHandle> get_file_handle(std::string file_path);
};

#endif // STORAGE_ENGINE_H
