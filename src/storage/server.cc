#include "server.h"
#include <iostream>

void StorageServiceImpl::readfile(
    ::google::protobuf::RpcController *cntl,
    const ::ReadRequest *request, 
    ::ReadResponse *response,
    ::google::protobuf::Closure *done
) {
    std::cout << "[readfile] Request received for ID: " << request->file_id()
              << ", offset: " << request->offset()
              << ", size: " << request->len() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto [st, data] = backend_.read(request->file_id(), request->offset(), request->len());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    *response->mutable_data() = data;
}

void StorageServiceImpl::writefile(
    ::google::protobuf::RpcController *cntl,
    const ::WriteRequest *request, 
    ::WriteResponse *response,
    ::google::protobuf::Closure *done
) {
    std::cout << "[writefile] Request received for ID: " << request->file_id()
              << ", offset: " << request->offset()
              << ", size: " << request->data().len() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto [st, bytes_written] = backend_.write(request->file_id(), request->data(), request->offset());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    response->set_bytes_written(bytes_written);
}

void StorageServiceImpl::deletefile(
    ::google::protobuf::RpcController *cntl,
    const ::DeleteRequest *request, 
    ::google::protobuf::Empty *response,
    ::google::protobuf::Closure *done
) {
    std::cout << "[deletefile] Request received for ID: " << request->file_id() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto st = backend_.remove_file(request->file_id());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
}
