#pragma once

#include "metadata.pb.h"
#include "status.h"
#include "storage.h"

#include <braft/protobuf_file.h> // braft::ProtoBufFile
#include <braft/raft.h>          // braft::Node braft::StateMachine
#include <braft/storage.h>       // braft::SnapshotWriter
#include <braft/util.h>          // braft::AsyncClosureGuard
#include <brpc/controller.h>     // brpc::Controller
#include <brpc/server.h>         // brpc::Server
#include <cstdint>

enum OpType : int32_t {
    OP_CREATEFILE = 1,
};

class KVStore : public braft::StateMachine {
  public:
    KVStore() : storage_(nullptr), node_(nullptr), leader_term_(-1){};
    ~KVStore() {
        // Cleanup resources if needed.
        // For example:
        storage_.reset();
        if (node_) {
            node_->shutdown(nullptr);
            node_->join();
            delete node_;
            node_ = nullptr;
        }
    }

    int start(int port, const std::string &conf, const std::string &db);
    void shutdown();

    void join();

    Status getattr(const InodeRequest *request, Attributes *response,
                   google::protobuf::Closure *done);
    Status readdir(const ReadDirRequest *request, ReadDirResponse *response,
                   google::protobuf::Closure *done);

    std::pair<Status, FileInfo> open(uint64_t inode);

    Status setattr(const ::Attributes *request, ::Attributes *response);
    Status createfile(const ::CreateRequest *request, ::Attributes *response,
                      google::protobuf::Closure *done);
    Status createdir(const ::CreateRequest *request, ::Attributes *response);
    Status removefile(const ::RemoveRequest *request);
    Status removedir(const ::RemoveRequest *request);
    Status renamefile(const ::RenameRequest *request);
    Status renamedir(const ::RenameRequest *request);

    // Implement the StateMachine interface

    void on_apply(braft::Iterator &iter) override;
    void on_snapshot_save(braft::SnapshotWriter *writer,
                          braft::Closure *done) override;
    int on_snapshot_load(braft::SnapshotReader *reader) override;
    bool is_leader() const;
    void on_leader_start(int64_t term) override;
    void on_leader_stop(const butil::Status &status) override;

  private:
    std::unique_ptr<MetadataStorage> storage_;
    braft::Node *volatile node_;
    butil::atomic<int64_t> leader_term_;
};