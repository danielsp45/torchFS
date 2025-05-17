#pragma once

#include "storage.pb.h"
#include "status.h"
#include <cstdint>

class StorageBackend {
public:
    StorageBackend(const std::string &mount_path);
    ~StorageBackend() = default;

    std::pair<Status, Data> read(const std::string &file_id, off_t offset, size_t size);
    std::pair<Status, uint64_t> write(const std::string &file_id, const Data &data, off_t offset);
    Status remove_file(const std::string &file_id);

private:
    std::string mount_path_;

    std::string get_file_path(const std::string& file_id) const;
};
