#include "metadata.pb.h"
#include "server.h"
#include "state_machine.h"
#include "status.h"
#include "storage.h"

#include <arpa/inet.h>
#include <braft/protobuf_file.h>
#include <braft/raft.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/at_exit.h>
#include <string>
#include <unistd.h>

// Define port as a constant.
const int kPort = 8000;

int main() {
    // Remove gflags initialization.
    butil::AtExitManager exit_manager;

    brpc::Server server;
    KVStore kv_store;

    MetadataServiceImpl metadata_service(&kv_store);

    // Add the metadata service into the RPC server.
    if (server.AddService(&metadata_service, brpc::SERVER_OWNS_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Add the raft service. Use the port constant.
    if (braft::add_service(&server, kPort) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // Start the KVStore to print initialization logs.
    if (kv_store.start() != 0) {
        LOG(ERROR) << "Failed to start KVStore";
        return -1;
    }

    // Bind to all interfaces so the service is reachable externally.
    std::string server_address = "0.0.0.0:" + std::to_string(kPort);
    brpc::ServerOptions options;
    if (server.Start(server_address.c_str(), &options) != 0) {
        LOG(ERROR) << "Failed to start RPC server";
        return -1;
    }

    LOG(INFO) << "Metadata service is running on " << server.listen_address();

    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "Metadata service is going to quit";
    kv_store.shutdown();
    server.Stop(0);
    kv_store.join();
    server.Join();
    return 0;
}