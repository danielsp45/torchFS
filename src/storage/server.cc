#include "server.h"
#include <iostream>

void StorageServiceImpl::read_chunk(
    ::google::protobuf::RpcController *cntl,
    const ::ReadRequest *request, 
    ::Data *response,
    ::google::protobuf::Closure *done
) {
    std::cout << "[readfile] Request received for ID: " << request->chunk_id() << std::endl;
    brpc::ClosureGuard done_guard(done);
    std::cout << "INSIDE READ CHUNK" << std::endl;
    auto [st, data] = backend_.read_chunk(request->chunk_id());
    if (!st.ok()) {
        std::cerr << "Error reading chunk: " << st.ToString() << std::endl;
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    *response = data;
}

void StorageServiceImpl::write_chunk(
    ::google::protobuf::RpcController *cntl,
    const ::WriteRequest *request, 
    ::WriteResponse *response,
    ::google::protobuf::Closure *done
) {
    std::cout << "[writefile] Request received for ID: " << request->chunk_id()
              << ", size: " << request->data().len() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto [st, bytes_written] = backend_.write_chunk(request->chunk_id(), request->data());
    if (!st.ok()) {
        std::cerr << "Error writing chunk: " << st.ToString() << std::endl;
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    response->set_bytes_written(bytes_written);
}

void StorageServiceImpl::delete_chunk(
    ::google::protobuf::RpcController *cntl,
    const ::DeleteRequest *request, 
    ::google::protobuf::Empty *response,
    ::google::protobuf::Closure *done
) {
    std::cout << "[deletefile] Request received for ID: " << request->chunk_id() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto st = backend_.remove_chunk(request->chunk_id());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
}
