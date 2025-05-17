#include "server.h"
#include <iostream>

// RPC method implementations
void MetadataServiceImpl::open(google::protobuf::RpcController *cntl,
                               const InodeRequest *request, FileInfo *response,
                               google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[open] Request received for inode: " << request->inode()
              << std::endl;
    kv_store_->open(request, response, done);
}

void MetadataServiceImpl::getattr(google::protobuf::RpcController *cntl,
                                  const InodeRequest *request,
                                  Attributes *response,
                                  google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[getattr] Request received for inode: " << request->inode()
              << std::endl;
    kv_store_->getattr(request, response, done);
}

void MetadataServiceImpl::readdir(google::protobuf::RpcController *cntl,
                                  const ReadDirRequest *request,
                                  ReadDirResponse *response,
                                  google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[readdir] Request received for inode: " << request->inode()
              << std::endl;
    kv_store_->readdir(request, response, done);
}

void MetadataServiceImpl::setattr(google::protobuf::RpcController *cntl,
                                  const Attributes *request,
                                  Attributes *response,
                                  google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[setattr] Request received for inode: " << request->inode()
              << std::endl;
    kv_store_->setattr(request, response, done);
}

void MetadataServiceImpl::createfile(google::protobuf::RpcController *cntl,
                                     const CreateRequest *request,
                                     Attributes *response,
                                     google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[createfile] Request received for parent inode: "
              << request->p_inode() << ", name: " << request->name()
              << std::endl;
    kv_store_->createfile(request, response, done);
}

void MetadataServiceImpl::createdir(google::protobuf::RpcController *cntl,
                                    const CreateRequest *request,
                                    Attributes *response,
                                    google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[createdir] Request received for parent inode: "
              << request->p_inode() << ", name: " << request->name()
              << std::endl;
    kv_store_->createdir(request, response, done);
}

void MetadataServiceImpl::removefile(google::protobuf::RpcController *cntl,
                                     const RemoveRequest *request,
                                     google::protobuf::Empty *response,
                                     google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[removefile] Request received for parent inode: "
              << request->p_inode() << ", inode: " << request->inode()
              << ", name: " << request->name() << std::endl;
    kv_store_->removefile(request, response, done);
}

void MetadataServiceImpl::removedir(google::protobuf::RpcController *cntl,
                                    const ::RemoveRequest *request,
                                    google::protobuf::Empty *response,
                                    google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[removedir] Request received for parent inode: "
              << request->p_inode() << ", inode: " << request->inode()
              << ", name: " << request->name() << std::endl;
    kv_store_->removedir(request, response, done);
}

void MetadataServiceImpl::renamefile(google::protobuf::RpcController *cntl,
                                     const ::RenameRequest *request,
                                     google::protobuf::Empty *response,
                                     google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[renamefile] Request received for inode: " << std::endl;
    kv_store_->renamefile(request, response, done);
}

void MetadataServiceImpl::renamedir(google::protobuf::RpcController *cntl,
                                    const RenameRequest *request,
                                    google::protobuf::Empty *response,
                                    google::protobuf::Closure *done) {
    (void)cntl;
    std::cout << "[renamedir] Request received for inode: " << std::endl;
    kv_store_->renamedir(request, response, done);
}
