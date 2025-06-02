#pragma once

#include "storage.pb.h"
#include "status.h"
#include <brpc/channel.h>
#include <cstdint>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <thread>
#include <mutex>
#include <future>

#define EC_K 4
#define EC_M 2

struct StorageNode {
    brpc::Channel channel;
    std::unique_ptr<StorageService_Stub> stub;
};

struct WriteJob {
    std::vector<std::string> data_nodes;
    std::vector<std::string> parity_nodes;
    std::string              file_id;
    Data                     data_payload; // contains both .payload() and .len()
};

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
    std::unordered_map<std::string, std::unique_ptr<StorageNode>> nodes_;
    int ec_descriptor_;

    void process_write(const WriteJob &job);
};
