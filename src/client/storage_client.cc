#include "storage_client.h"
#include <brpc/channel.h>
#include <google/protobuf/empty.pb.h>
#include <stdexcept>

StorageClient::StorageClient(const std::string &server_address)
    : channel_(), stub_(nullptr) {
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 1000;
    options.max_retry = 3;
    if (channel_.Init(server_address.c_str(), "", &options) != 0) {
        throw std::runtime_error("Failed to initialize channel to " + server_address);
    }
    stub_ = new StorageService_Stub(&channel_);
}

std::pair<Status, Data> StorageClient::read(const std::string &file_id, off_t offset, size_t size) {
    ReadRequest req;
    req.set_file_id(file_id);
    req.set_offset(offset);
    req.set_len(size);
    ReadResponse resp;
    brpc::Controller cntl;
    stub_->readfile(&cntl, &req, &resp, nullptr);
    if (cntl.Failed())
        return {Status::IOError(cntl.ErrorText()), Data()};
    return {Status::OK(), resp.data()};
}

std::pair<Status, uint64_t> StorageClient::write(const std::string &file_id, const Data &data, off_t offset) {
    WriteRequest req;
    req.set_file_id(file_id);
    *req.mutable_data() = data;
    req.set_offset(offset);
    WriteResponse resp;
    brpc::Controller cntl;
    stub_->writefile(&cntl, &req, &resp, nullptr);
    if (cntl.Failed())
        return {Status::IOError(cntl.ErrorText()), 0};
    return {Status::OK(), resp.bytes_written()};
}

Status StorageClient::remove_file(const std::string &file_id) {
    DeleteRequest req;
    req.set_file_id(file_id);
    google::protobuf::Empty resp;
    brpc::Controller cntl;
    stub_->deletefile(&cntl, &req, &resp, nullptr);
    return cntl.Failed() ? Status::IOError(cntl.ErrorText()) : Status::OK();
}
