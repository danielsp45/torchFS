#include "storage_client.h"
#include <cassert>
#include <google/protobuf/empty.pb.h>
#include <iostream>
#include <liberasurecode/erasurecode.h>
#include <stdexcept>
#include <utility>

// Helper struct to capture a single read-RPC result
struct FragmentResult {
    std::string node_name;
    bool        ok;
    std::string payload;
    uint64_t    len;
    std::string error_text;
};

// Launch one read_chunk RPC against `node` and return a FragmentResult
static FragmentResult fetch_one_fragment(
    const std::string &node,
    const std::string &file_id,
    StorageService_Stub *stub)
{
    ReadRequest req;
    req.set_chunk_id(file_id);

    Data resp;
    brpc::Controller cntl;
    stub->read_chunk(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return FragmentResult{node, false, "", 0, cntl.ErrorText()};
    } else {
        return FragmentResult{node, true,
                              std::string(resp.payload()), resp.len(), ""};
    }
}

// Helper struct to capture a single write-RPC result
struct WriteResult {
    std::string node_name;
    bool        ok;
    std::string error_text;
};

// Launch one write_chunk RPC to `node` with the given payload
static WriteResult send_one_fragment(
    const std::string &node,
    const std::string &file_id,
    const char        *payload,
    uint64_t           len,
    StorageService_Stub *stub)
{
    Data node_data;
    node_data.set_len(len);
    node_data.set_payload(std::string(payload, len));

    WriteRequest req;
    req.set_chunk_id(file_id);
    *req.mutable_data() = node_data;

    WriteResponse resp;
    brpc::Controller cntl;
    stub->write_chunk(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return WriteResult{node, false, cntl.ErrorText()};
    } else {
        return WriteResult{node, true, ""};
    }
}

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

    struct ec_args args = { .k = EC_K, .m = EC_M };
    ec_descriptor_ = liberasurecode_instance_create(
        EC_BACKEND_LIBERASURECODE_RS_VAND, &args);
    if (!ec_descriptor_) {
        throw std::runtime_error("Failed to create erasure code instance\n");
    }
}

StorageClient::~StorageClient() {
    nodes_.clear();
    liberasurecode_instance_destroy(ec_descriptor_);
}

std::pair<Status, Data>
StorageClient::read(const std::vector<std::string> &data_nodes,
                    const std::vector<std::string> &parity_nodes,
                    const std::string &file_id) {
    if (data_nodes.size() != EC_K || parity_nodes.size() != EC_M) {
        return {Status::IOError("Invalid node configuration"), Data()};
    }

    // 1) Launch all K data-node reads in parallel
    std::vector<std::future<FragmentResult>> data_futures;
    data_futures.reserve(EC_K);

    for (const auto &node : data_nodes) {
        if (!nodes_.contains(node)) {
            return {Status::IOError("Node not found: " + node), Data()};
        }
        StorageService_Stub *stub = nodes_[node]->stub.get();
        data_futures.push_back(
            std::async(std::launch::async,
                       &fetch_one_fragment,
                       node,
                       file_id,
                       stub));
    }

    // 2) Collect results
    std::vector<std::string> fragment_data;
    fragment_data.reserve(EC_K);
    uint64_t frag_len = 0;

    for (auto &fut : data_futures) {
        FragmentResult fr = fut.get();
        if (fr.ok) {
            if (frag_len == 0) frag_len = fr.len;
            else                assert(frag_len == fr.len);
            fragment_data.push_back(std::move(fr.payload));
        } else {
            std::cerr << "Failed to read from data node "
                      << fr.node_name << ": "
                      << fr.error_text << "\n";
        }
    }

    // 3) If fewer than K, launch parity reads in parallel to fill missing
    if (fragment_data.size() < EC_K) {
        std::vector<std::future<FragmentResult>> parity_futures;
        parity_futures.reserve(EC_M);

        for (const auto &node : parity_nodes) {
            if (!nodes_.contains(node)) {
                return {Status::IOError("Node not found: " + node), Data()};
            }
            StorageService_Stub *stub = nodes_[node]->stub.get();
            parity_futures.push_back(
                std::async(std::launch::async,
                           &fetch_one_fragment,
                           node,
                           file_id,
                           stub));
        }

        for (auto &fut : parity_futures) {
            if (fragment_data.size() >= EC_K) break;
            FragmentResult fr = fut.get();
            if (fr.ok) {
                fragment_data.push_back(std::move(fr.payload));
            } else {
                std::cerr << "Failed to read from parity node "
                          << fr.node_name << ": "
                          << fr.error_text << "\n";
            }
        }
    }

    // 4) If still fewer than K, error
    if (fragment_data.size() < EC_K) {
        return {Status::IOError("Insufficient fragments for reconstruction"), Data()};
    }

    // 5) Prepare for decode
    std::vector<char *> fragments;
    fragments.reserve(fragment_data.size());
    for (auto &frag : fragment_data) {
        fragments.push_back(const_cast<char *>(frag.data()));
    }

    // 6) Decode
    char *decoded_data = nullptr;
    uint64_t decoded_len = 0;
    int res = liberasurecode_decode(
        ec_descriptor_, fragments.data(), EC_K, frag_len, 0,
        &decoded_data, &decoded_len);

    if (res != 0) {
        return {Status::IOError("Decode failed: " + std::to_string(res)), Data()};
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

std::pair<Status, uint64_t>
StorageClient::write(const std::vector<std::string> &data_nodes,
                     const std::vector<std::string> &parity_nodes,
                     const std::string &file_id,
                     const Data &data) {
    if (data_nodes.size() != EC_K || parity_nodes.size() != EC_M) {
        return {Status::IOError("Invalid node configuration"), 0};
    }

    WriteJob job;
    job.data_nodes   = data_nodes;
    job.parity_nodes = parity_nodes;
    job.file_id      = file_id;
    job.data_payload = data;

    // Perform write (blocking until all fragments written)
    process_write(job);
    return {Status::OK(), 0ULL}; // bytes_written not computed here
}

Status StorageClient::remove_file(const std::vector<std::string> &data_nodes,
                                  const std::vector<std::string> &parity_nodes,
                                  const std::string &file_id) {
    if (data_nodes.size() != EC_K || parity_nodes.size() != EC_M) {
        return {Status::IOError("Invalid node configuration")};
    }

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
            std::cerr << "Failed to delete file from node " << node
                      << ": " << cntl.ErrorText() << "\n";
            return Status::IOError(cntl.ErrorText());
        }
    }

    return Status::OK();
}

void StorageClient::process_write(const WriteJob &job) {
    // 1) Erasure-encode
    char **data_fragments   = nullptr;
    char **parity_fragments = nullptr;
    uint64_t fragment_len   = 0;

    int r = liberasurecode_encode(
        ec_descriptor_,
        job.data_payload.payload().data(),
        job.data_payload.len(),
        &data_fragments,
        &parity_fragments,
        &fragment_len);
    if (r != 0) {
        if (data_fragments)
            liberasurecode_encode_cleanup(ec_descriptor_, data_fragments, nullptr);
        if (parity_fragments)
            liberasurecode_encode_cleanup(ec_descriptor_, nullptr, parity_fragments);
        return;
    }

    // 2) Launch all K+M writes in parallel
    std::vector<std::future<WriteResult>> write_futures;
    write_futures.reserve(EC_K + EC_M);

    // 2a) Data-node writes
    for (int i = 0; i < EC_K; ++i) {
        const std::string &node = job.data_nodes[i];
        if (!nodes_.count(node)) continue;
        StorageService_Stub *stub = nodes_.at(node)->stub.get();
        write_futures.push_back(
            std::async(std::launch::async,
                       &send_one_fragment,
                       node,
                       job.file_id,
                       data_fragments[i],
                       fragment_len,
                       stub));
    }

    // 2b) Parity-node writes
    for (int i = 0; i < EC_M; ++i) {
        const std::string &node = job.parity_nodes[i];
        if (!nodes_.count(node)) continue;
        StorageService_Stub *stub = nodes_.at(node)->stub.get();
        write_futures.push_back(
            std::async(std::launch::async,
                       &send_one_fragment,
                       node,
                       job.file_id,
                       parity_fragments[i],
                       fragment_len,
                       stub));
    }

    // 3) Wait for all to complete
    for (auto &fut : write_futures) {
        WriteResult wr = fut.get();
        if (!wr.ok) {
            std::cerr << "Failed to write to node " << wr.node_name
                      << ": " << wr.error_text << "\n";
        }
    }

    // 4) Cleanup buffers
    int cleanup_res = liberasurecode_encode_cleanup(
        ec_descriptor_,
        data_fragments,
        parity_fragments);
    if (cleanup_res != 0) {
        std::cerr << "Error during erasure-encode cleanup: "
                  << cleanup_res << "\n";
    }
}
