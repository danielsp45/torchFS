#include "storage_client.h"
#include <brpc/channel.h>
#include <google/protobuf/empty.pb.h>
#include <stdexcept>
#include <utility>
#include <vector>

StorageClient::StorageClient() {
    const std::vector<std::pair<std::string, std::string>> servers = {
        {"node1", "127.0.0.1:9000"},
        {"node2", "127.0.0.1:9001"},
        {"node3", "127.0.0.1:9002"},
        {"node4", "127.0.0.1:9003"},
        {"node5", "127.0.0.1:9004"}
    };

    for (const auto &entry : servers) {
        const std::string &server_name = entry.first;
        const std::string &address = entry.second;
        auto node = std::make_unique<StorageNode>();
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = 1000;
        options.max_retry = 3;
        if (node->channel.Init(address.c_str(), "", &options) != 0) {
            throw std::runtime_error("Failed to initialize channel to " + address);
        }
        node->stub = std::make_unique<StorageService_Stub>(&node->channel);
        nodes_.emplace(server_name, std::move(node));
    }
}

std::pair<Status, Data> StorageClient::read(const std::string &server, const std::string &file_id, off_t offset, size_t size) {
    if (!nodes_.contains(server)) {
        return { Status::IOError("Server not found: " + server), Data() };
    }
    
    ReadRequest req;
    req.set_file_id(file_id);
    req.set_offset(offset);
    req.set_len(size);
    ReadResponse resp;
    brpc::Controller cntl;

    nodes_[server]->stub->readfile(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return {Status::IOError(cntl.ErrorText()), Data()};
    }
    return {Status::OK(), resp.data()};
}

std::pair<Status, uint64_t> StorageClient::write(const std::string &server, const std::string &file_id, const Data &data, off_t offset) {
    if (nodes_.find(server) == nodes_.end()) {
        return {Status::IOError("Server not found: " + server), 0};
    }
    
    WriteRequest req;
    req.set_file_id(file_id);
    *req.mutable_data() = data;
    req.set_offset(offset);
    WriteResponse resp;
    brpc::Controller cntl;

    nodes_[server]->stub->writefile(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return {Status::IOError(cntl.ErrorText()), 0};
    }
    return {Status::OK(), resp.bytes_written()};
}

Status StorageClient::remove_file(const std::string &server, const std::string &file_id) {
    if (nodes_.find(server) == nodes_.end()) {
        return Status::IOError("Server not found: " + server);
    }
    
    DeleteRequest req;
    req.set_file_id(file_id);
    google::protobuf::Empty resp;
    brpc::Controller cntl;

    nodes_[server]->stub->deletefile(&cntl, &req, &resp, nullptr);
    return cntl.Failed() ? Status::IOError(cntl.ErrorText()) : Status::OK();
}
