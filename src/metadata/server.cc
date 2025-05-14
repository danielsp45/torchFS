#include "server.h"
#include "status.h"
#include <iostream>

// RPC method implementations
void MetadataServiceImpl::getattr(google::protobuf::RpcController *cntl,
                                  const InodeRequest *request,
                                  Attributes *response,
                                  google::protobuf::Closure *done) {
    std::cout << "[getattr] Request received for inode: " << request->inode()
              << std::endl;
    kv_store_->getattr(request, response, done);
}

void MetadataServiceImpl::readdir(google::protobuf::RpcController *cntl,
                                  const ReadDirRequest *request,
                                  ReadDirResponse *response,
                                  google::protobuf::Closure *done) {
    std::cout << "[readdir] Request received for inode: " << request->inode()
              << std::endl;
    kv_store_->readdir(request, response, done);
}

// void MetadataServiceImpl::setattr(::google::protobuf::RpcController *cntl,
//                                   const ::Attributes *request,
//                                   ::Attributes *response,
//                                   ::google::protobuf::Closure *done) {
//     std::cout << "[setattr] Request received for inode: " << request->inode()
//               << std::endl;
//     brpc::ClosureGuard done_guard(done);
//     Status s = kv_store_->setattr(request, response);
//     if (!s.ok()) {
//         cntl->SetFailed(s.ToString());
//     }
// }

void MetadataServiceImpl::createfile(google::protobuf::RpcController *cntl,
                                     const CreateRequest *request,
                                     Attributes *response,
                                     google::protobuf::Closure *done) {
    std::cout << "[createfile] Request received for parent inode: "
              << request->p_inode() << ", name: " << request->name()
              << std::endl;
    kv_store_->createfile(request, response, done);
}

// void MetadataServiceImpl::createdir(::google::protobuf::RpcController *cntl,
//                                     const ::CreateRequest *request,
//                                     ::Attributes *response,
//                                     ::google::protobuf::Closure *done) {
//     std::cout << "[createdir] Request received for parent inode: "
//               << request->p_inode() << ", name: " << request->name()
//               << std::endl;
//     brpc::ClosureGuard done_guard(done);
//     Status s = kv_store_->createdir(request, response);
//     if (!s.ok()) {
//         cntl->SetFailed(s.ToString());
//     }
// }

// void MetadataServiceImpl::removefile(::google::protobuf::RpcController *cntl,
//                                      const ::RemoveRequest *request,
//                                      ::google::protobuf::Empty *response,
//                                      ::google::protobuf::Closure *done) {
//     std::cout << "[removefile] Request received for parent inode: "
//               << request->p_inode() << ", inode: " << request->inode()
//               << ", name: " << request->name() << std::endl;
//     brpc::ClosureGuard done_guard(done);
//     Status s = kv_store_->removefile(request);
//     if (!s.ok()) {
//         cntl->SetFailed(s.ToString());
//     }
// }

// void MetadataServiceImpl::removedir(::google::protobuf::RpcController *cntl,
//                                     const ::RemoveRequest *request,
//                                     ::google::protobuf::Empty *response,
//                                     ::google::protobuf::Closure *done) {
//     std::cout << "[removedir] Request received for parent inode: "
//               << request->p_inode() << ", inode: " << request->inode()
//               << ", name: " << request->name() << std::endl;
//     brpc::ClosureGuard done_guard(done);
//     Status s = kv_store_->removedir(request);
//     if (!s.ok()) {
//         cntl->SetFailed(s.ToString());
//     }
// }

// void MetadataServiceImpl::renamefile(::google::protobuf::RpcController *cntl,
//                                      const ::RenameRequest *request,
//                                      ::google::protobuf::Empty *response,
//                                      ::google::protobuf::Closure *done) {
//     std::cout << "[renamefile] Request received for inode: " <<
//     request->inode()
//               << " from parent: " << request->old_p_inode()
//               << " to new parent: " << request->new_p_inode()
//               << ", new name: " << request->new_name() << std::endl;
//     brpc::ClosureGuard done_guard(done);
//     Status s = kv_store_->renamefile(request);
//     if (!s.ok()) {
//         cntl->SetFailed(s.ToString());
//     }
// }

// void MetadataServiceImpl::renamedir(::google::protobuf::RpcController *cntl,
//                                     const ::RenameRequest *request,
//                                     ::google::protobuf::Empty *response,
//                                     ::google::protobuf::Closure *done) {
//     std::cout << "[renamedir] Request received for inode: " <<
//     request->inode()
//               << " from parent: " << request->old_p_inode()
//               << " to new parent: " << request->new_p_inode()
//               << ", new name: " << request->new_name() << std::endl;
//     brpc::ClosureGuard done_guard(done);
//     Status s = kv_store_->renamedir(request);
//     if (!s.ok()) {
//         cntl->SetFailed(s.ToString());
//     }
// }

// void MetadataServiceImpl::open(::google::protobuf::RpcController *cntl,
//                                const ::InodeRequest *request,
//                                ::FileInfo *response,
//                                ::google::protobuf::Closure *done) {
//     std::cout << "[open] Request received for inode: " << request->inode()
//               << std::endl;
//     brpc::ClosureGuard done_guard(done);
//     auto [s, info] = kv_store_->open(request->inode());
//     if (!s.ok()) {
//         cntl->SetFailed(s.ToString());
//     } else {
//         *response = info;
//     }
// }