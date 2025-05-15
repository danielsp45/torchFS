#include "state_machine.h"
#include "server.h"
#include "util.h"
#include <absl/utility/internal/if_constexpr.h>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>

class OperationClosure : public braft::Closure {
  public:
    OperationClosure(KVStore *kv_store, const CreateRequest *request,
                     Attributes *response, google::protobuf::Closure *done)
        : kv_store_(kv_store), request_(request), response_(response),
          done_(done) {}

    void Run() override {
        std::unique_ptr<OperationClosure> self_guard(this);
        brpc::ClosureGuard done_guard(done_);
    }

    KVStore *kv_store_;
    const CreateRequest *request_;
    Attributes *response_;
    google::protobuf::Closure *done_;
};

int KVStore::start(int port, const std::string &conf, const std::string &path) {
    // 1. local storage ----------------------------------------------------
    storage_ = std::make_unique<MetadataStorage>(join_paths(path, "db"));
    if (auto st = storage_->init(); !st.ok()) {
        LOG(ERROR) << st.ToString();
        return -1;
    }
    LOG(INFO) << "[KVStore] storage initialised";

    // 2. raft options -----------------------------------------------------
    butil::EndPoint self_ep(butil::my_ip(), port);

    braft::NodeOptions opts;
    opts.election_timeout_ms = 5000;
    opts.fsm = this;
    opts.node_owns_fsm = false;

    const std::string prefix = "local://" + join_paths(path, "kvstore");
    opts.log_uri = prefix + "/log";
    opts.raft_meta_uri = prefix + "/meta";
    opts.snapshot_uri = prefix + "/snapshot";

    // use --conf only at **first** start-up
    if (!conf.empty()) {
        if (opts.initial_conf.parse_from(conf) != 0) {
            LOG(ERROR) << "invalid --conf: " << conf;
            return -1;
        }
    }

    // 3. create node ------------------------------------------------------
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
    if (node_)
        node_->shutdown(nullptr);
}

void KVStore::join() {
    if (node_)
        node_->join();
}

void KVStore::on_apply(braft::Iterator &iter) {
    for (; iter.valid(); iter.next()) {
        // This guard ensures that the closure's `Run()` method is called
        braft::AsyncClosureGuard closure_guard(iter.done());

        // Parse the log data
        butil::IOBuf data = iter.data();

        OperationClosure *closure =
            iter.done() ? dynamic_cast<OperationClosure *>(iter.done())
                        : nullptr;
        const CreateRequest *request = closure ? closure->request_ : nullptr;
        Attributes *response = closure ? closure->response_ : nullptr;

        CreateRequest req;
        if (request) {
            // Use the request from the closure if available
            req = *request;
        } else {
            // Parse the request from the log
            butil::IOBufAsZeroCopyInputStream wrapper(data);
            CHECK(req.ParseFromZeroCopyStream(&wrapper));
        }

        // Perform the CreateFile operation
        auto [status, attr] = storage_->create_file(req.p_inode(), req.name());
        if (response) {
            if (status.ok()) {
                response->CopyFrom(attr);
            } else {
                // Handle error case
                response->set_inode(0); // or some error code
                LOG(ERROR) << "Failed to create file: " << status.ToString();
            }
        }

        // Log the operation for debugging
        LOG(INFO) << "Applied CreateFile operation at log_index="
                  << iter.index();
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

// Direct, non-raft read methods changed to return a std::pair instead of using
// output params

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

Status KVStore::createfile(const CreateRequest *request, Attributes *response,
                           google::protobuf::Closure *done) {
    brpc::ClosureGuard done_guard(done);
    if (!is_leader()) {
        return Status::IOError("Not the leader");
    }

    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream wrapper(&log);
    if (!request->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to serialize request";
        // TODO return failed request
        return Status::IOError("Failed to serialize request");
    }

    // Apply this log as a braft::Task
    braft::Task task;
    task.data = &log;
    // This callback would be iovoked when the task actually excuted or
    // fail
    task.done =
        new OperationClosure(this, request, response, done_guard.release());

    node_->apply(task);

    return Status::OK();
}