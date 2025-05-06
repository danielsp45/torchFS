#include "metadata.pb.h"
#include "status.h"
#include "storage.h"

#include "rpc_service.h"
#include <brpc/server.h>
#include <google/protobuf/empty.pb.h>

int main(int argc, char **argv) {
    // Generally you only need one Server.
    brpc::Server server;

    // Instance of your service.
    MetadataServiceImpl echo_service_impl;

    // Add the service into server. Notice the second parameter, because the
    // service is put on stack, we don't want server to delete it, otherwise
    // use brpc::SERVER_OWNS_SERVICE.
    if (server.AddService(&echo_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    butil::EndPoint point;
    point = butil::EndPoint(butil::IP_ANY, 8000);
    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = 10;
    if (server.Start(point, &options) != 0) {
        LOG(ERROR) << "Fail to start metadata node";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}