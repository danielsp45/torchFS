#include "server.h"
#include "state_machine.h"

#include <arpa/inet.h>
#include <braft/protobuf_file.h>
#include <braft/raft.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/at_exit.h>
#include <gflags/gflags.h>
#include <string>
#include <unistd.h>

// Define port as a constant.
DEFINE_int32(port, 8000, "Listen port of this peer");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(path, "", "Path to metadata storage");
DEFINE_string(host, "127.0.1.1", "Host address for the metadata service");

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Remove gflags initialization.
    butil::AtExitManager exit_manager;

    brpc::Server server;
    MetadataStateMachine state_machine;

    MetadataServiceImpl metadata_service(&state_machine);

    // Add the metadata service into the RPC server.
    if (server.AddService(&metadata_service, brpc::SERVER_OWNS_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Add the raft service. Use the port constant.
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // Start the KVStore to print initialization logs.
    if (state_machine.start(FLAGS_port, FLAGS_conf, FLAGS_path) != 0) {
        LOG(ERROR) << "Failed to start MetadataStateMachine";
        return -1;
    }

    // Bind to all interfaces so the service is reachable externally.
    std::string server_address = FLAGS_host + ":" + std::to_string(FLAGS_port);
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
    state_machine.shutdown();
    server.Stop(0);
    state_machine.join();
    server.Join();
    return 0;
}