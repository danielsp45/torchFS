#pragma once

#include "storage.pb.h"
#include "status.h"
#include <brpc/channel.h>
#include <cstdint>
#include <string>
#include <sys/types.h>
#include <unordered_map>

#define EC_K 4
#define EC_M 2

class StorageClient {
  public:
    StorageClient();
    ~StorageClient();

    std::pair<Status, Data> read(
      const std::vector<std::string> &data_nodes,
      const std::vector<std::string> &parity_nodes, 
      const std::string &file_id
    );
    std::pair<Status, uint64_t> write(
      const std::vector<std::string> &data_nodes,
      const std::vector<std::string> &parity_nodes,
      const std::string &file_id,
      const Data &data
    );
    Status remove_file(
      const std::vector<std::string> &data_nodes,
      const std::vector<std::string> &parity_nodes,
      const std::string &file_id
    );

  private:
    struct StorageNode {
      brpc::Channel channel;
      std::unique_ptr<StorageService_Stub> stub;
    };
    std::unordered_map<std::string, std::unique_ptr<StorageNode>> nodes_;
    int ec_descriptor_;
};
