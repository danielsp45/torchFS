#include "storage.pb.h"
#include "server.h"
#include <gflags/gflags.h>
#include <brpc/server.h>
#include <google/protobuf/empty.pb.h>

DEFINE_int32(port, 9000, "Listening port of the storage server");
DEFINE_string(mount_path, "/home/vagrant/storage", "Directory where data is stored");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;
    StorageServiceImpl storage_service(FLAGS_mount_path);

    if (server.AddService(&storage_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    butil::EndPoint point;
    point = butil::EndPoint(butil::IP_ANY, FLAGS_port);
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
