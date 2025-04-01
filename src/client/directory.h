#ifndef DIRECTORY_H
#define DIRECTORY_H

#include <memory>
#include <string>
#include <vector>

#include "file_handle.h"
#include "status.h"

class Directory {
public:
  Directory(const std::string &path) : path_(path) {}
  ~Directory() = default;

  Status list(std::vector<std::string> &out_files);
  Status insert(std::unique_ptr<FileHandle> file_handle);

private:
  std::string path_; // Directory path
  std::vector<std::unique_ptr<FileHandle>>
      files_; // File handles in the directory
};

#endif // DIRECTORY_H