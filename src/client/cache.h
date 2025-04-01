#ifndef CACHE_H
#define CACHE_H

#include <memory>

#include "directory.h"
#include "file_handle.h"
#include "status.h"

class Cache {
public:
  Cache() {}
  ~Cache() {}

  virtual Status init() = 0;

  virtual Status get(const std::string &path,
                     std::unique_ptr<FileHandle> &out_file_handle) = 0;
  virtual Status put(const std::string &path,
                     std::unique_ptr<FileHandle> file_handle) = 0;
  virtual Status remove(const std::string &path,
                        std::unique_ptr<FileHandle> &out_file_handle) = 0;
};

class DiskCache : public Cache {
public:
  DiskCache() {}
  ~DiskCache() {}

  Status init() override;

  Status get(const std::string &path,
             std::unique_ptr<FileHandle> &out_file_handle) override;
  Status put(const std::string &path,
             std::unique_ptr<FileHandle> file_handle) override;
  Status remove(const std::string &path,
                std::unique_ptr<FileHandle> &out_file_handle) override;

private:
  std::unique_ptr<Directory> root_dir_;
};

#endif // CACHE_H