#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>

#include "directory.h"
#include "directory_manager.h"
#include "file_handle.h"
#include "fuse.h"
#include "status.h"

// class StorageEngine {
//   public:
//     StorageEngine() {}
//     virtual ~StorageEngine() {}

//     virtual Status init() = 0;

//     virtual Status open(const std::string &path, int flags,
//                         uint64_t &handle) = 0;
//     virtual Status create(const std::string &path, int flags, mode_t mode,
//                           uint64_t &handle) = 0;
//     virtual Status close(uint64_t &handle) = 0;
//     virtual Status remove(const std::string path) = 0;
//     virtual Status read(uint64_t &handle, Slice result, size_t size,
//                         off_t offset) = 0;
//     virtual Status write(uint64_t &handle, Slice data, size_t size,
//                          off_t offset) = 0;
//     virtual Status sync(std::string path) = 0;
//     virtual Status rename(const std::string &oldpath,
//                           const std::string &newpath) = 0;
//     virtual Status getattr(const std::string &path, struct stat *stbuf) = 0;
//     virtual Status readdir(const std::string &path, void *buf,
//                            fuse_fill_dir_t filler,
//                            fuse_readdir_flags flags) = 0;
//     virtual Status mkdir(const std::string &path, mode_t mode) = 0;
//     virtual Status rmdir(const std::string &path) = 0;

//     virtual std::string get_logic_path(const std::string &path) = 0;
// };

class StorageEngine {
  public:
    StorageEngine(const std::string &mount_path)
        : dm_(std::make_unique<DirectoryManager>(mount_path)),
          mount_path_(mount_path) {}

    ~StorageEngine() {}

    Status init();

    Status open(const std::string &path, int flags, uint64_t &handle);
    Status create(const std::string &path, int flags, mode_t mode,
                  uint64_t &handle);
    Status close(uint64_t &handle);
    Status remove(const std::string path);
    Status read(uint64_t &handle, Slice result, size_t size, off_t offset);
    Status write(uint64_t &handle, Slice data, size_t size, off_t offset);
    Status sync(std::string path);
    Status rename(const std::string &oldpath, const std::string &newpath);
    Status getattr(const std::string &path, struct stat *stbuf);
    Status readdir(const std::string &path, void *buf, fuse_fill_dir_t filler,
                   fuse_readdir_flags flags);
    Status mkdir(const std::string &path, mode_t mode);
    Status rmdir(const std::string &path);

    std::string get_logic_path(const std::string &path);

  private:
    std::unique_ptr<DirectoryManager> dm_;
    std::map<std::uint64_t, std::shared_ptr<FileHandle>> open_files_;
    std::string mount_path_; // Directory for local storage

    uint64_t register_fh(std::shared_ptr<FileHandle> fh);
    std::shared_ptr<FileHandle> lookup_fh(uint64_t handle);

    std::shared_ptr<FileHandle> get_file_handle(std::string file_path);
};

#endif // STORAGE_ENGINE_H