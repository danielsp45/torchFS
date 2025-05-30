#include "storage_client.h"
#include <brpc/channel.h>
#include <cassert>
#include <google/protobuf/empty.pb.h>
#include <iostream>
#include <liberasurecode/erasurecode.h>
#include <stdexcept>
#include <utility>
#include <vector>

StorageClient::StorageClient() {
    const std::vector<std::pair<std::string, std::string>> servers = {
        {"node1", "127.0.0.1:9001"}, {"node2", "127.0.0.1:9002"},
        {"node3", "127.0.0.1:9003"}, {"node4", "127.0.0.1:9004"},
        {"node5", "127.0.0.1:9005"}, {"node6", "127.0.0.1:9006"}};

    for (const auto &entry : servers) {
        const std::string &server_name = entry.first;
        const std::string &address = entry.second;
        auto node = std::make_unique<StorageNode>();
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = 1000;
        options.max_retry = 3;
        if (node->channel.Init(address.c_str(), "", &options) != 0) {
            throw std::runtime_error("Failed to initialize channel to " +
                                     address);
        }
        node->stub = std::make_unique<StorageService_Stub>(&node->channel);
        nodes_.emplace(server_name, std::move(node));
    }

    // Init Erasure Code
    struct ec_args args = {
        .k = EC_K, // data frags
        .m = EC_M  // parity frags
    };

    ec_descriptor_ = liberasurecode_instance_create(
        EC_BACKEND_LIBERASURECODE_RS_VAND, &args);
    if (!ec_descriptor_) {
        throw std::runtime_error("Failed to create erasure code instance\n");
    }
}

StorageClient::~StorageClient() {
    nodes_.clear();
    int res = liberasurecode_instance_destroy(ec_descriptor_);
    if (res != 0) {
        LOG(ERROR) << "Failed to destroy erasure code instance: " << res;
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

std::pair<Status, Data>
StorageClient::read(const std::vector<std::string> &data_nodes,
                    const std::vector<std::string> &parity_nodes,
                    const std::string &file_id) {
    if (data_nodes.size() != EC_K || parity_nodes.size() != EC_M) {
        return {Status::IOError("Invalid node configuration"), Data()};
    }

    // Read from data nodes
    std::vector<std::string> fragment_data;
    uint64_t frag_len = 0;
    for (const auto &node : data_nodes) {
        if (!nodes_.contains(node)) {
            return {Status::IOError("Node not found: " + node), Data()};
        }

        ReadRequest req;
        req.set_chunk_id(file_id);
        Data resp;
        brpc::Controller cntl;

        nodes_[node]->stub->read_chunk(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            std::cerr << "Failed to read from data node " << node << ": "
                      << cntl.ErrorText() << std::endl;
        } else {
            fragment_data.push_back(resp.payload());
            if (frag_len == 0) {
                frag_len = resp.len();
            } else {
                assert(frag_len == resp.len());
            }
        }
    }

    // Read from parity nodes if needed
    for (int i = 0; i < parity_nodes.size() && fragment_data.size() < EC_K;
         i++) {
        const std::string &node = parity_nodes[i];
        if (!nodes_.contains(node)) {
            return {Status::IOError("Node not found: " + node), Data()};
        }

        ReadRequest req;
        req.set_chunk_id(file_id);
        Data resp;
        brpc::Controller cntl;

        nodes_[node]->stub->read_chunk(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            std::cerr << "Failed to read from parity node " << node << ": "
                      << cntl.ErrorText() << std::endl;
        } else {
            fragment_data.push_back(resp.payload());
        }
    }

    // Check fragment count
    if (fragment_data.size() < EC_K) {
        return {Status::IOError("Insufficient fragments for reconstruction"),
                Data()};
    }

    // Convert to pointer array to match liberasurecode API
    std::vector<char *> fragments;
    for (auto &frag : fragment_data) {
        fragments.push_back(const_cast<char *>(frag.data()));
    }

    // Decode data
    char *decoded_data = nullptr;
    uint64_t decoded_len = 0;

    int res = liberasurecode_decode(ec_descriptor_, fragments.data(), EC_K,
                                    frag_len, 0, &decoded_data, &decoded_len);

    if (res != 0) {
        return {Status::IOError("Decode failed: " + std::to_string(res)),
                Data()};
    }

    Data result;
    result.set_len(decoded_len);
    result.set_payload(std::string(decoded_data, decoded_len));

    res = liberasurecode_decode_cleanup(ec_descriptor_, decoded_data);
    if (res != 0) {
        return {Status::IOError("Decode cleanup failed!"), Data()};
    }
    return {Status::OK(), result};
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

std::pair<Status, uint64_t>
StorageClient::write(const std::vector<std::string> &data_nodes,
                     const std::vector<std::string> &parity_nodes,
                     const std::string &file_id, const Data &data) {
    if (data_nodes.size() != EC_K || parity_nodes.size() != EC_M) {
        return {Status::IOError("Invalid node configuration"), 0};
    }

    // Encode data
    char **data_fragments = nullptr;
    char **parity_fragments = nullptr;
    uint64_t fragment_len = 0;

    int res = liberasurecode_encode(ec_descriptor_, data.payload().data(),
                                    data.len(), &data_fragments,
                                    &parity_fragments, &fragment_len);

    if (res != 0) {
        return {Status::IOError("Encode failed: " + std::to_string(res)), 0};
    }

    // Write to data nodes
    for (int i = 0; i < EC_K; i++) {
        std::string node = data_nodes[i];
        if (!nodes_.contains(node)) {
            return {Status::IOError("Node not found: " + node), 0};
        }
        Data node_data;
        node_data.set_len(fragment_len);
        node_data.set_payload(std::string(data_fragments[i], fragment_len));

        WriteRequest req;
        req.set_chunk_id(file_id);
        *req.mutable_data() = node_data;
        WriteResponse resp;
        brpc::Controller cntl;

        nodes_[node]->stub->write_chunk(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            std::cerr << "Failed to write to data node " << node << ": "
                      << cntl.ErrorText() << std::endl;
        }
    }


    // Write to parity nodes
    for (int i = 0; i < EC_M; i++) {
        std::string node = parity_nodes[i];
        if (!nodes_.contains(node)) {
            return {Status::IOError("Node not found: " + node), 0};
        }

        Data node_data;
        node_data.set_len(fragment_len);
        node_data.set_payload(std::string(parity_fragments[i], fragment_len));

        WriteRequest req;
        req.set_chunk_id(file_id);
        *req.mutable_data() = node_data;
        WriteResponse resp;
        brpc::Controller cntl;

        nodes_[node]->stub->write_chunk(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            std::cerr << "Failed to write to parity node " << node << ": "
                      << cntl.ErrorText() << std::endl;
        }    
    }

    res = liberasurecode_encode_cleanup(ec_descriptor_, data_fragments,
                                        parity_fragments);
    if (res != 0) {
        return {Status::IOError("Encode cleanup failed!"), 0};
    }
    return {Status::OK(), 0}; // resp.bytes_written()};
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

Status StorageClient::remove_file(const std::vector<std::string> &data_nodes,
                                  const std::vector<std::string> &parity_nodes,
                                  const std::string &file_id) {
    if (data_nodes.size() != EC_K || parity_nodes.size() != EC_M) {
        return {Status::IOError("Invalid node configuration")};
    }

    // Remove from nodes
    for (size_t i = 0; i < EC_K + EC_M; i++) {
        const std::string &node =
            (i < EC_K) ? data_nodes[i] : parity_nodes[i - EC_K];
        if (!nodes_.contains(node)) {
            return {Status::IOError("Node not found: " + node)};
        }

        DeleteRequest req;
        req.set_chunk_id(file_id);
        google::protobuf::Empty resp;
        brpc::Controller cntl;

        nodes_[node]->stub->delete_chunk(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            std::cerr << "Failed to delete file from node " << node << ": "
                      << cntl.ErrorText() << std::endl;
            return Status::IOError(cntl.ErrorText());
        }
    }

    return Status::OK();
}
