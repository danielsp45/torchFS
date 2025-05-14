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

enum OpType : int32_t {
    OP_CREATEFILE = 1,
};

class KVStore : public braft::StateMachine {
  public:
    KVStore();
    ~KVStore();

    int start();
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

class RpcDone : public braft::Closure {
  public:
    explicit RpcDone(::Attributes *response) : response_(response) {}

    void Run() override {
        if (status().ok()) {
            // Task succeeded, no additional action needed
        } else {
            // Handle failure
            LOG(ERROR) << "Task failed: " << status();
        }
        delete this; // Automatically delete the closure after execution
    }

    ::Attributes *response_ptr() { return response_; }

  private:
    ::Attributes *response_;
};