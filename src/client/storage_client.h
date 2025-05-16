#pragma once

#include "storage.pb.h"
#include "status.h"
#include <brpc/channel.h>
#include <cstdint>

class StorageClient {
  public:
    StorageClient(const std::string &server_address = "127.0.0.1:9000");
    ~StorageClient() = default;

    std::pair<Status, Data> read(const std::string &file_id, off_t offset, size_t size);
    std::pair<Status, uint64_t> write(const std::string &file_id, const Data &data, off_t offset);
    Status remove_file(const std::string &file_id);

  private:
    brpc::Channel channel_;
    StorageService_Stub *stub_;
};
