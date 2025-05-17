#include "storage.pb.h"
#include "server.h"
#include <brpc/server.h>
#include <google/protobuf/empty.pb.h>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <mount_path>\n";
        return 1;
    }
    brpc::Server server;
    std::string mount_path = argv[1];
    StorageServiceImpl storage_service(mount_path);

    if (server.AddService(&storage_service, brpc::SERVER_DOESNT_OWN_SERVICE) !=
        0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    butil::EndPoint point;
    point = butil::EndPoint(butil::IP_ANY, 9000);
    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = 10;
    if (server.Start(point, &options) != 0) {
        LOG(ERROR) << "Fail to start storage node";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
