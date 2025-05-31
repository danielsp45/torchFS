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

    stop_worker_   = false;
    worker_thread_ = std::thread(&StorageClient::worker_loop, this);
}

StorageClient::~StorageClient() {
    {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        stop_worker_ = true;
    }
    queue_cv_.notify_one();
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    nodes_.clear();
    liberasurecode_instance_destroy(ec_descriptor_);
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
    for (size_t i = 0; i < parity_nodes.size() && fragment_data.size() < EC_K; i++) {
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
std::pair<Status, uint64_t> StorageClient::write(const std::vector<std::string> &data_nodes,
                                                 const std::vector<std::string> &parity_nodes,
                                                 const std::string &file_id,
                                                 const Data &data) {
    if (data_nodes.size() != EC_K || parity_nodes.size() != EC_M) {
        return { Status::IOError("Invalid node configuration"), 0 };
    }

    WriteJob job;
    job.data_nodes   = data_nodes;
    job.parity_nodes = parity_nodes;
    job.file_id      = file_id;
    job.data_payload = data;  // copy: Data holds payload + length

    {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        write_queue_.push(std::move(job));
    }
    queue_cv_.notify_one();
    // We don't know “bytes_written” yet, since the job runs in background.
    return { Status::OK(), 0ULL };
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

// -------------------------------------------------------------------------
// The background worker thread: wait for jobs, then call process_one_write().
// -------------------------------------------------------------------------
void StorageClient::worker_loop() {
    while (true) {
        WriteJob job;
        {
            std::unique_lock<std::mutex> lk(queue_mutex_);
            queue_cv_.wait(lk, [this]() {
                return stop_worker_ || !write_queue_.empty();
            });
            if (stop_worker_ && write_queue_.empty()) {
                // Time to exit cleanly
                return;
            }
            job = std::move(write_queue_.front());
            write_queue_.pop();
        }
        // Outside the lock, perform the actual erasure encode + RPC
        process_write(job);
    }
}

// -------------------------------------------------------------------------
// Exactly the same logic you had before in write(...), but now in one place.
// This method runs in the background worker thread.
// -------------------------------------------------------------------------
void StorageClient::process_write(const WriteJob &job) {
    // 1) Erasure‐encode job.data_payload
    char **data_fragments   = nullptr;
    char **parity_fragments = nullptr;
    uint64_t fragment_len   = 0;

    int r = liberasurecode_encode(ec_descriptor_,
                                  job.data_payload.payload().data(),
                                  job.data_payload.len(),
                                  &data_fragments,
                                  &parity_fragments,
                                  &fragment_len);
    if (r != 0) {
        if (data_fragments)   liberasurecode_encode_cleanup(ec_descriptor_, data_fragments, nullptr);
        if (parity_fragments) liberasurecode_encode_cleanup(ec_descriptor_, nullptr, parity_fragments);
        return;
    }

    // 2) RPC to each data node
    for (int i = 0; i < EC_K; i++) {
        const std::string &node = job.data_nodes[i];
        if (!nodes_.count(node)) {
            continue;
        }
        Data node_data;
        node_data.set_len(fragment_len);
        node_data.set_payload(std::string(data_fragments[i], fragment_len));

        WriteRequest req;
        req.set_chunk_id(job.file_id);
        *req.mutable_data() = node_data;

        WriteResponse resp;
        brpc::Controller cntl;
        nodes_.at(node)->stub->write_chunk(&cntl, &req, &resp, nullptr);
    }

    // 3) RPC to each parity node
    for (int i = 0; i < EC_M; i++) {
        const std::string &node = job.parity_nodes[i];
        if (!nodes_.count(node)) {
            continue;
        }
        Data node_data;
        node_data.set_len(fragment_len);
        node_data.set_payload(std::string(parity_fragments[i], fragment_len));

        WriteRequest req;
        req.set_chunk_id(job.file_id);
        *req.mutable_data() = node_data;

        WriteResponse resp;
        brpc::Controller cntl;
        nodes_.at(node)->stub->write_chunk(&cntl, &req, &resp, nullptr);
    }

    // 4) Cleanup erasure buffers
    r = liberasurecode_encode_cleanup(ec_descriptor_,
                                      data_fragments,
                                      parity_fragments);
}

