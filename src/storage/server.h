#include "storage.pb.h"
#include "status.h"
#include "storage_backend.h"
#include <brpc/server.h>
#include <iostream>

class StorageServiceImpl : public StorageService {
 public:
    StorageServiceImpl(const std::string &mount_path) : backend_(mount_path) {}
    virtual ~StorageServiceImpl() {}

    void read_chunk(
        ::google::protobuf::RpcController *cntl,
        const ::ReadRequest *request, 
        ::Data *response,
        ::google::protobuf::Closure *done
    );

    void write_chunk(
        ::google::protobuf::RpcController *cntl,
        const ::WriteRequest *request, 
        ::WriteResponse *response,
        ::google::protobuf::Closure *done
    );

    void delete_chunk(
        ::google::protobuf::RpcController *cntl,
        const ::DeleteRequest *request, 
        ::google::protobuf::Empty *response,
        ::google::protobuf::Closure *done
    );

 private:
    StorageBackend backend_;
};
