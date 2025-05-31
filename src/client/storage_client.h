#pragma once

#include "storage.pb.h"
#include "status.h"
#include <brpc/channel.h>
#include <condition_variable>
#include <queue>
#include <cstdint>
#include <string>
#include <sys/types.h>
#include <unordered_map>

//
// StorageNode just wraps a brpc::Channel + stub for one "nodeX".
// You already have this in your code.
//
struct StorageNode {
    brpc::Channel channel;
    std::unique_ptr<StorageService_Stub> stub;
};

//
// A simple struct that holds everything needed for a single write job.
//
struct WriteJob {
    std::vector<std::string> data_nodes;
    std::vector<std::string> parity_nodes;
    std::string               file_id;
    Data                      data_payload; // contains both .payload() and .len()
};

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

    void worker_loop();
    void process_write(const WriteJob &job);
    std::mutex                           queue_mutex_;
    std::condition_variable              queue_cv_;
    std::queue<WriteJob>                 write_queue_;
    bool                                 stop_worker_{false};
    std::thread                          worker_thread_;

};
