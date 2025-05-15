#include "metadata_client.h"
#include <brpc/channel.h>
#include <google/protobuf/empty.pb.h>
#include <stdexcept>

// Constructor: initialize BRPC channel and stub
MetadataClient::MetadataClient(const std::string &server_address)
    : channel_(), stub_(nullptr) {
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 1000;
    options.max_retry = 3;
    if (channel_.Init("127.0.2.1:8000", "", &options) != 0) {
        throw std::runtime_error(
            "Failed to initialize channel to 127.0.2.1:8000");
    }
    stub_ = new MetadataService_Stub(&channel_);
}

// getattr → RPC
std::pair<Status, Attributes> MetadataClient::getattr(const uint64_t &inode) {
    InodeRequest req;
    req.set_inode(inode);
    Attributes resp;
    brpc::Controller cntl;
    stub_->getattr(&cntl, &req, &resp, nullptr);
    if (cntl.Failed())
        return {Status::IOError(cntl.ErrorText()), Attributes()};
    return {Status::OK(), resp};
}

// readdir → RPC
std::pair<Status, std::vector<Dirent>>
MetadataClient::readdir(const uint64_t &inode) {
    ReadDirRequest req;
    req.set_inode(inode);
    ReadDirResponse resp;
    brpc::Controller cntl;
    stub_->readdir(&cntl, &req, &resp, nullptr);
    if (cntl.Failed())
        return {Status::IOError(cntl.ErrorText()), {}};
    std::vector<Dirent> list;
    for (const auto &d : resp.entries())
        list.push_back(d);
    return {Status::OK(), list};
}

// open → RPC
std::pair<Status, FileInfo> MetadataClient::open(const uint64_t &inode) {
    InodeRequest req;
    req.set_inode(inode);
    FileInfo resp;
    brpc::Controller cntl;
    stub_->open(&cntl, &req, &resp, nullptr);
    if (cntl.Failed())
        return {Status::IOError(cntl.ErrorText()), FileInfo()};
    return {Status::OK(), resp};
}

// create_file → RPC
std::pair<Status, Attributes>
MetadataClient::create_file(const uint64_t &p_inode, const std::string &name) {
    CreateRequest req;
    req.set_p_inode(p_inode);
    req.set_name(name);
    Attributes resp;
    brpc::Controller cntl;
    stub_->createfile(&cntl, &req, &resp, nullptr);
    if (cntl.Failed())
        return {Status::IOError(cntl.ErrorText()), Attributes()};
    return {Status::OK(), resp};
}

// create_dir → RPC
std::pair<Status, Attributes>
MetadataClient::create_dir(const uint64_t &p_inode, const std::string &name) {
    CreateRequest req;
    req.set_p_inode(p_inode);
    req.set_name(name);
    Attributes resp;
    brpc::Controller cntl;
    stub_->createdir(&cntl, &req, &resp, nullptr);
    if (cntl.Failed())
        return {Status::IOError(cntl.ErrorText()), Attributes()};
    return {Status::OK(), resp};
}

// remove_file → RPC
Status MetadataClient::remove_file(const uint64_t &p_inode,
                                   const uint64_t &inode,
                                   const std::string &name) {
    RemoveRequest req;
    req.set_p_inode(p_inode);
    req.set_inode(inode);
    req.set_name(name);
    google::protobuf::Empty resp;
    brpc::Controller cntl;
    stub_->removefile(&cntl, &req, &resp, nullptr);
    return cntl.Failed() ? Status::IOError(cntl.ErrorText()) : Status::OK();
}

// remove_dir → RPC
Status MetadataClient::remove_dir(const uint64_t &p_inode,
                                  const uint64_t &inode,
                                  const std::string &name) {
    RemoveRequest req;
    req.set_p_inode(p_inode);
    req.set_inode(inode);
    req.set_name(name);
    google::protobuf::Empty resp;
    brpc::Controller cntl;
    stub_->removedir(&cntl, &req, &resp, nullptr);
    return cntl.Failed() ? Status::IOError(cntl.ErrorText()) : Status::OK();
}

// rename_file → RPC
Status MetadataClient::rename_file(const uint64_t &old_p_inode,
                                   const uint64_t &new_p_inode,
                                   const uint64_t &inode,
                                   const std::string &new_name) {
    RenameRequest req;
    req.set_old_p_inode(old_p_inode);
    req.set_new_p_inode(new_p_inode);
    req.set_inode(inode);
    req.set_new_name(new_name);
    google::protobuf::Empty resp;
    brpc::Controller cntl;
    stub_->renamefile(&cntl, &req, &resp, nullptr);
    return cntl.Failed() ? Status::IOError(cntl.ErrorText()) : Status::OK();
}

// rename_dir → RPC
Status MetadataClient::rename_dir(const uint64_t &old_p_inode,
                                  const uint64_t &new_p_inode,
                                  const uint64_t &inode,
                                  const std::string &new_name) {
    RenameRequest req;
    req.set_old_p_inode(old_p_inode);
    req.set_new_p_inode(new_p_inode);
    req.set_inode(inode);
    req.set_new_name(new_name);
    google::protobuf::Empty resp;
    brpc::Controller cntl;
    stub_->renamedir(&cntl, &req, &resp, nullptr);
    return cntl.Failed() ? Status::IOError(cntl.ErrorText()) : Status::OK();
}

// setattr → RPC
Status MetadataClient::setattr(const uint64_t &inode, const Attributes &attr) {
    Attributes req = attr, resp;
    brpc::Controller cntl;
    stub_->setattr(&cntl, &req, &resp, nullptr);
    return cntl.Failed() ? Status::IOError(cntl.ErrorText()) : Status::OK();
}