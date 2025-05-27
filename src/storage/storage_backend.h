#pragma once

#include "storage.pb.h"
#include "status.h"
#include <cstdint>

class StorageBackend {
public:
    StorageBackend(const std::string &mount_path);
    ~StorageBackend() = default;

    std::pair<Status, Data> read_chunk(const std::string &chunk_id);
    std::pair<Status, uint64_t> write_chunk(const std::string &chunk_id, const Data &data);
    Status remove_chunk(const std::string &chunk_id);

private:
    std::string mount_path_;

    std::string get_chunk_path(const std::string& chunk_id) const;
};
