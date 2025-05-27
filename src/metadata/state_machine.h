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
    OP_SETATTR = 1,
    OP_CREATEFILE = 2,
    OP_CREATEDIR = 3,
    OP_REMOVEFILE = 4,
    OP_REMOVEDIR = 5,
    OP_RENAMEFILE = 6,
    OP_RENAMEDIR = 7,
};

class MetadataStateMachine : public braft::StateMachine {
  public:
    MetadataStateMachine()
        : storage_(nullptr), node_(nullptr), leader_term_(-1){};
    ~MetadataStateMachine() {
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

    Status open(const InodeRequest *request, FileInfo *response,
                google::protobuf::Closure *done);
    Status getattr(const InodeRequest *request, Attributes *response,
                   google::protobuf::Closure *done);
    Status readdir(const ReadDirRequest *request, ReadDirResponse *response,
                   google::protobuf::Closure *done);
    Status createfile(const ::CreateRequest *request, ::Attributes *response,
                      google::protobuf::Closure *done);
    Status setattr(const ::Attributes *request, ::Attributes *response,
                   google::protobuf::Closure *done);
    Status createdir(const CreateRequest *request, Attributes *response,
                     google::protobuf::Closure *done);
    Status removefile(const RemoveRequest *request,
                      google::protobuf::Empty *response,
                      google::protobuf::Closure *done);
    Status removedir(const RemoveRequest *request,
                     google::protobuf::Empty *response,
                     google::protobuf::Closure *done);
    Status renamefile(const ::RenameRequest *request,
                      google::protobuf::Empty *response,
                      google::protobuf::Closure *done);
    Status renamedir(const RenameRequest *request,
                     google::protobuf::Empty *response,
                     google::protobuf::Closure *done);

    // Implement the StateMachine interface

    void on_apply(braft::Iterator &iter) override;
    Status apply_operation(const google::protobuf::Message *request,
                           google::protobuf::Message *response,
                           google::protobuf::Closure *done, OpType op);
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