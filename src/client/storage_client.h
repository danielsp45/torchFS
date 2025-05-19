#pragma once

#include "storage.pb.h"
#include "status.h"
#include <brpc/channel.h>
#include <cstdint>
#include <string>
#include <unordered_map>

class StorageClient {
  public:
    StorageClient();
    ~StorageClient() = default;

    std::pair<Status, Data> read(const std::string &server, const std::string &file_id, off_t offset, size_t size);
    std::pair<Status, uint64_t> write(const std::string &server, const std::string &file_id, const Data &data, off_t offset);
    Status remove_file(const std::string &server, const std::string &file_id);

  private:
    struct StorageNode {
        brpc::Channel channel;
        std::unique_ptr<StorageService_Stub> stub;
    };
    std::unordered_map<std::string, std::unique_ptr<StorageNode>> nodes_;
};
