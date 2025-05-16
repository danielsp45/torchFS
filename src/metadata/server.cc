#include "server.h"
#include <iostream>

// RPC method implementations
void MetadataServiceImpl::getattr(::google::protobuf::RpcController *cntl,
                                  const ::InodeRequest *request,
                                  ::Attributes *response,
                                  ::google::protobuf::Closure *done) {
    std::cout << "[getattr] Request received for inode: " << request->inode()
              << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto [st, attr] = storage_.getattr(request->inode());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    *response = attr;
}

void MetadataServiceImpl::setattr(::google::protobuf::RpcController *cntl,
                                  const ::Attributes *request,
                                  ::Attributes *response,
                                  ::google::protobuf::Closure *done) {
    std::cout << "[setattr] Request received for inode: " << request->inode()
              << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto st = storage_.setattr(request->inode(), *request);
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    *response = *request;
}

void MetadataServiceImpl::readdir(::google::protobuf::RpcController *cntl,
                                  const ::ReadDirRequest *request,
                                  ::ReadDirResponse *response,
                                  ::google::protobuf::Closure *done) {
    std::cout << "[readdir] Request received for inode: " << request->inode()
              << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto [st, list] = storage_.readdir(request->inode());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    for (const auto &d : list) {
        *response->add_entries() = d;
    }
}

void MetadataServiceImpl::createfile(::google::protobuf::RpcController *cntl,
                                     const ::CreateRequest *request,
                                     ::Attributes *response,
                                     ::google::protobuf::Closure *done) {
    std::cout << "[createfile] Request received for parent inode: "
              << request->p_inode() << ", name: " << request->name()
              << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto [st, attr] = storage_.create_file(request->p_inode(), request->name());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    *response = attr;
}

void MetadataServiceImpl::createdir(::google::protobuf::RpcController *cntl,
                                    const ::CreateRequest *request,
                                    ::Attributes *response,
                                    ::google::protobuf::Closure *done) {
    std::cout << "[createdir] Request received for parent inode: "
              << request->p_inode() << ", name: " << request->name()
              << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto [st, attr] = storage_.create_dir(request->p_inode(), request->name());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    *response = attr;
}

void MetadataServiceImpl::removefile(::google::protobuf::RpcController *cntl,
                                     const ::RemoveRequest *request,
                                     ::google::protobuf::Empty *response,
                                     ::google::protobuf::Closure *done) {
    std::cout << "[removefile] Request received for parent inode: "
              << request->p_inode() << ", inode: " << request->inode()
              << ", name: " << request->name() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto st = storage_.remove_file(request->p_inode(), request->inode(),
                                   request->name());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
    }
}

void MetadataServiceImpl::removedir(::google::protobuf::RpcController *cntl,
                                    const ::RemoveRequest *request,
                                    ::google::protobuf::Empty *response,
                                    ::google::protobuf::Closure *done) {
    std::cout << "[removedir] Request received for parent inode: "
              << request->p_inode() << ", inode: " << request->inode()
              << ", name: " << request->name() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto st = storage_.remove_dir(request->p_inode(), request->inode(),
                                  request->name());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
    }
}

void MetadataServiceImpl::renamefile(::google::protobuf::RpcController *cntl,
                                     const ::RenameRequest *request,
                                     ::google::protobuf::Empty *response,
                                     ::google::protobuf::Closure *done) {
    std::cout << "[renamefile] Request received for inode: " << request->inode()
              << " from parent: " << request->old_p_inode()
              << " to new parent: " << request->new_p_inode()
              << ", new name: " << request->new_name() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto st =
        storage_.rename_file(request->old_p_inode(), request->new_p_inode(),
                             request->inode(), request->new_name());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
    }
}

void MetadataServiceImpl::renamedir(::google::protobuf::RpcController *cntl,
                                    const ::RenameRequest *request,
                                    ::google::protobuf::Empty *response,
                                    ::google::protobuf::Closure *done) {
    std::cout << "[renamedir] Request received for inode: " << request->inode()
              << " from parent: " << request->old_p_inode()
              << " to new parent: " << request->new_p_inode()
              << ", new name: " << request->new_name() << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto st =
        storage_.rename_dir(request->old_p_inode(), request->new_p_inode(),
                            request->inode(), request->new_name());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
    }
}

void MetadataServiceImpl::open(::google::protobuf::RpcController *cntl,
                               const ::InodeRequest *request,
                               ::FileInfo *response,
                               ::google::protobuf::Closure *done) {
    std::cout << "[open] Request received for inode: " << request->inode()
              << std::endl;
    brpc::ClosureGuard done_guard(done);
    auto [st, info] = storage_.open(request->inode());
    if (!st.ok()) {
        static_cast<brpc::Controller *>(cntl)->SetFailed(st.ToString());
        return;
    }
    *response = info;
}