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
    OperationClosure(KVStore *kv_store,
                     const google::protobuf::Message *request,
                     google::protobuf::Message *response,
                     google::protobuf::Closure *done)
        : kv_store_(kv_store), request_(request), response_(response),
          done_(done) {}

    void Run() override {
        std::unique_ptr<OperationClosure> self_guard(this);
        brpc::ClosureGuard done_guard(done_);
    }

    // Fix: get_request() must return the original request, not response.
    google::protobuf::Message *get_request() {
        return const_cast<google::protobuf::Message *>(request_);
    }

    google::protobuf::Message *get_response() { return response_; }

    KVStore *kv_store_;
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
        // Guard to ensure closure's Run() is called.
        braft::AsyncClosureGuard closure_guard(iter.done());
        // Parse the log data.
        butil::IOBuf data = iter.data();

        CreateRequest *request = nullptr;
        Attributes *response = nullptr;
        OperationClosure *closure = nullptr;

        if (iter.done()) {
            // Task applied on this node: get request from closure.
            std::cout << "iter.done() is true" << std::endl;
            closure = dynamic_cast<OperationClosure *>(iter.done());
            if (closure) {
                request = dynamic_cast<CreateRequest *>(closure->get_request());
                response = dynamic_cast<Attributes *>(closure->get_response());
            }
            if (request) {
                std::cout << "request: " << request->DebugString() << std::endl;
            }
        } else {
            // Parse the request from the log.
            std::cout << "iter.done() is false" << std::endl;
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            CreateRequest tmp_req; // temporary request to be filled
            CHECK(tmp_req.ParseFromZeroCopyStream(&wrapper));
            // Copy parsed data into request pointer.
            // (If needed, you can allocate a new CreateRequest.)
            request = new CreateRequest(tmp_req);
        }

        // Perform the CreateFile operation.
        if (request) {
            std::cout << "Performing CreateFile operation for inode "
                      << request->p_inode() << std::endl;
            auto [status, attr] =
                storage_->create_file(request->p_inode(), request->name());
            if (response) {
                if (status.ok()) {
                    std::cout << "CreateFile operation succeeded" << std::endl;
                    response->CopyFrom(attr);
                } else {
                    std::cout << "CreateFile operation failed" << std::endl;
                    // Handle error case.
                    response->set_inode(0); // or any error-indicative code.
                    LOG(ERROR)
                        << "Failed to create file: " << status.ToString();
                }
            }
        }
        // Note: If you allocated a temporary CreateRequest, ensure you free it.
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
    if (!req->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to serialize request";
        return Status::IOError("Failed to serialize request");
    }

    braft::Task task;
    task.data = &log;
    task.done = new OperationClosure(this, req, response, done_guard.release());
    node_->apply(task);
    return Status::OK();
}