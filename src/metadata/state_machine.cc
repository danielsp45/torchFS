#include "state_machine.h"
#include "metadata.pb.h"
#include "server.h"
#include "util.h"
#include <braft/raft.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/at_exit.h>
#include <iostream>
#include <memory>

// Reimplemented OperationClosure with proper getters.
class OperationClosure : public braft::Closure {
  public:
    OperationClosure(KVStore *kv_store, OpType op_type,
                     const google::protobuf::Message *request,
                     google::protobuf::Message *response,
                     google::protobuf::Closure *done)
        : kv_store_(kv_store), op_type_(op_type), request_(request),
          response_(response), done_(done) {}

    void Run() override {
        std::unique_ptr<OperationClosure> self_guard(this);
        brpc::ClosureGuard done_guard(done_);
    }

    OpType op_type() { return op_type_; }

    google::protobuf::Message *get_request() {
        return const_cast<google::protobuf::Message *>(request_);
    }

    google::protobuf::Message *get_response() { return response_; }

    KVStore *kv_store_;
    OpType op_type_;
    const google::protobuf::Message *request_;
    google::protobuf::Message *response_;
    google::protobuf::Closure *done_;
};

int KVStore::start(int port, const std::string &conf, const std::string &path) {
    // 1. Local storage initialization.
    storage_ = std::make_unique<MetadataStorage>(join_paths(path, "db"));
    if (auto st = storage_->init(); !st.ok()) {
        LOG(ERROR) << st.ToString();
        return -1;
    }
    LOG(INFO) << "[KVStore] storage initialised";

    // 2. Raft options setup.
    butil::EndPoint self_ep(butil::my_ip(), port);
    braft::NodeOptions opts;
    opts.election_timeout_ms = 5000;
    opts.fsm = this;
    opts.node_owns_fsm = false;

    const std::string prefix = "local://" + join_paths(path, "kvstore");
    opts.log_uri = prefix + "/log";
    opts.raft_meta_uri = prefix + "/meta";
    opts.snapshot_uri = prefix + "/snapshot";

    if (!conf.empty()) {
        if (opts.initial_conf.parse_from(conf) != 0) {
            LOG(ERROR) << "invalid --conf: " << conf;
            return -1;
        }
    }

    // 3. Create Raft node.
    node_ = new braft::Node("kv_store", braft::PeerId(self_ep));
    if (node_->init(opts) != 0) {
        LOG(ERROR) << "raft node init failed";
        delete node_;
        node_ = nullptr;
        return -1;
    }
    return 0;
}

void KVStore::shutdown() {
    if (node_) {
        node_->shutdown(nullptr);
    }
}

void KVStore::join() {
    if (node_) {
        node_->join();
    }
}

void KVStore::on_apply(braft::Iterator &iter) {
    for (; iter.valid(); iter.next()) {
        // Ensure closure's Run() is called.
        braft::AsyncClosureGuard closure_guard(iter.done());
        butil::IOBuf data = iter.data();
        OpType op;

        // Determine op type either from the closure or by parsing the log data.
        if (iter.done()) {
            OperationClosure *closure =
                dynamic_cast<OperationClosure *>(iter.done());
            if (closure) {
                op = closure->op_type();
            } else {
                LOG(ERROR) << "Invalid closure type";
                continue;
            }
        } else {
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            google::protobuf::io::CodedInputStream coded_input(&wrapper);
            uint32_t op_val = 0;
            CHECK(coded_input.ReadVarint32(&op_val));
            op = static_cast<OpType>(op_val);
        }

        switch (op) {
        case OP_CREATEFILE: {
            CreateRequest *request = nullptr;
            Attributes *response = nullptr;
            if (iter.done()) {
                OperationClosure *closure =
                    dynamic_cast<OperationClosure *>(iter.done());
                if (closure) {
                    request =
                        dynamic_cast<CreateRequest *>(closure->get_request());
                    response =
                        dynamic_cast<Attributes *>(closure->get_response());
                }
            } else {
                // Parse the op type (already extracted) and then the
                // CreateRequest payload.
                butil::IOBufAsZeroCopyInputStream wrapper(data);
                google::protobuf::io::CodedInputStream coded_input(&wrapper);
                // Skip the op type field.
                uint32_t dummy;
                coded_input.ReadVarint32(&dummy);
                CreateRequest tmp;
                CHECK(tmp.ParseFromCodedStream(&coded_input));
                request = new CreateRequest(tmp);
            }
            if (request) {
                LOG(INFO) << "Performing CreateFile operation for inode "
                          << request->p_inode();
                auto [status, attr] =
                    storage_->create_file(request->p_inode(), request->name());
                if (response) {
                    if (status.ok()) {
                        LOG(INFO) << "CreateFile operation succeeded";
                        response->CopyFrom(attr);
                    } else {
                        LOG(ERROR) << "CreateFile operation failed: "
                                   << status.ToString();
                        response->set_inode(0); // Indicate error.
                    }
                }
            }
            // Free the temporary request if it was allocated.
            if (!iter.done() && request) {
                delete request;
            }
            break;
        }
        default:
            LOG(WARNING) << "Unknown op type: " << op;
            break;
        }
    }
}

void KVStore::on_snapshot_save(braft::SnapshotWriter * /*writer*/,
                               braft::Closure *done) {
    braft::AsyncClosureGuard guard(done);
}

int KVStore::on_snapshot_load(braft::SnapshotReader * /*reader*/) { return 0; }

bool KVStore::is_leader() const {
    return leader_term_.load(butil::memory_order_acquire) > 0;
}

void KVStore::on_leader_start(int64_t term) {
    leader_term_.store(term, butil::memory_order_release);
    LOG(INFO) << "Node becomes leader";
}

void KVStore::on_leader_stop(const butil::Status &status) {
    leader_term_.store(-1, butil::memory_order_release);
    LOG(INFO) << "Node stepped down : " << status;
}

Status KVStore::getattr(const InodeRequest *request, Attributes *response,
                        google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    auto [s, attr] = storage_->getattr(request->inode());
    if (!s.ok()) {
        return s;
    }
    response->CopyFrom(attr);
    return Status::OK();
}

Status KVStore::readdir(const ReadDirRequest *request,
                        ReadDirResponse *response,
                        google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    auto [s, entries] = storage_->readdir(request->inode());
    if (!s.ok()) {
        return s;
    }
    for (const auto &entry : entries) {
        Dirent *dirent = response->add_entries();
        dirent->set_inode(entry.inode());
        dirent->set_name(entry.name());
    }
    return Status::OK();
}

Status KVStore::createfile(const CreateRequest *req, Attributes *response,
                           google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    if (!is_leader()) {
        return Status::IOError("Not the leader");
    }

    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream wrapper(&log);

    // Serialize the op_type first.
    {
        google::protobuf::io::CodedOutputStream coded_output(&wrapper);
        coded_output.WriteVarint32(OP_CREATEFILE);
    }

    // Then serialize the actual request.
    if (!req->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to serialize request";
        return Status::IOError("Failed to serialize request");
    }

    braft::Task task;
    task.data = &log;
    task.done = new OperationClosure(this, OP_CREATEFILE, req, response,
                                     done_guard.release());
    node_->apply(task);
    return Status::OK();
}