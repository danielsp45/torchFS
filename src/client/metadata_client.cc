// metadata_client.cpp
#include "metadata_client.h"
#include "metadata.pb.h"

#include <braft/route_table.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/logging.h>
#include <google/protobuf/empty.pb.h>
#include <stdexcept>
#include <unistd.h> // usleep

// TODO: remove hardcoded config
// ─── Hard-coded cluster settings ─────────────────────────────────────────────
static const std::string kMetadataGroup = "kv_store";
static const std::string kMetadataConf =
    "127.0.2.1:8000:0,127.0.2.1:9000:1,127.0.2.1:7000:2,"; // adjust to your
                                                           // peers
static constexpr int kTimeoutMs = 1000;                    // RPC timeout
static constexpr int kMaxRetries = 5;                      // retry attempts
static constexpr int kRetryBackoffUs = 100000;             // 100 ms
// ────────────────────────────────────────────────────────────────────────────────

MetadataClient::MetadataClient(const std::string & /*server_addr*/) {
    // Tell RouteTable about our group once:
    if (braft::rtb::update_configuration(kMetadataGroup, kMetadataConf) != 0) {
        throw std::runtime_error("Failed to register " + kMetadataConf +
                                 " for group " + kMetadataGroup);
    }
}

// Helper to pick or refresh the leader
static bool pick_leader(braft::PeerId *leader) {
    // If we already know a leader, select_leader returns 0:
    if (braft::rtb::select_leader(kMetadataGroup, leader) == 0) {
        return true;
    }
    // Otherwise ask everyone who the leader is:
    auto st = braft::rtb::refresh_leader(kMetadataGroup, kTimeoutMs);
    if (!st.ok())
        return false;
    // Now try again:
    return (braft::rtb::select_leader(kMetadataGroup, leader) == 0);
}

std::pair<Status, Attributes> MetadataClient::getattr(const uint64_t &inode) {
    InodeRequest req;
    req.set_inode(inode);
    Attributes resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.getattr(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            // on transport error or NOT_LEADER redirect
            LOG(WARNING) << "getattr() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        // success
        return {Status::OK(), resp};
    }
    return {Status::IOError("getattr() failed after retries"), Attributes()};
}

std::pair<Status, std::vector<Dirent>>
MetadataClient::readdir(const uint64_t &inode) {
    ReadDirRequest req;
    req.set_inode(inode);
    ReadDirResponse resp;
    std::vector<Dirent> out;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.readdir(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "readdir() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        for (const auto &d : resp.entries()) {
            out.push_back(d);
        }
        return {Status::OK(), out};
    }
    return {Status::IOError("readdir() failed after retries"), {}};
}

std::pair<Status, FileInfo> MetadataClient::open(const uint64_t &inode) {
    InodeRequest req;
    req.set_inode(inode);
    FileInfo resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.open(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "open() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        return {Status::OK(), resp};
    }
    return {Status::IOError("open() failed after retries"), FileInfo()};
}

std::pair<Status, Attributes>
MetadataClient::create_file(const uint64_t &p_inode, const std::string &name) {
    CreateRequest req;
    req.set_p_inode(p_inode);
    req.set_name(name);
    Attributes resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.createfile(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "createfile() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        return {Status::OK(), resp};
    }
    return {Status::IOError("create_file() failed after retries"),
            Attributes()};
}

std::pair<Status, Attributes>
MetadataClient::create_dir(const uint64_t &p_inode, const std::string &name) {
    CreateRequest req;
    req.set_p_inode(p_inode);
    req.set_name(name);
    Attributes resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.createdir(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "createdir() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        return {Status::OK(), resp};
    }
    return {Status::IOError("create_dir() failed after retries"), Attributes()};
}

Status MetadataClient::remove_file(const uint64_t &p_inode,
                                   const uint64_t &inode,
                                   const std::string &name) {
    RemoveRequest req;
    req.set_p_inode(p_inode);
    req.set_inode(inode);
    req.set_name(name);
    google::protobuf::Empty resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.removefile(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "removefile() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        return Status::OK();
    }
    return Status::IOError("remove_file() failed after retries");
}

Status MetadataClient::remove_dir(const uint64_t &p_inode,
                                  const uint64_t &inode,
                                  const std::string &name) {
    RemoveRequest req;
    req.set_p_inode(p_inode);
    req.set_inode(inode);
    req.set_name(name);
    google::protobuf::Empty resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.removedir(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "removedir() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        return Status::OK();
    }
    return Status::IOError("remove_dir() failed after retries");
}

Status MetadataClient::rename_file(const uint64_t &old_p_inode,
                                   const uint64_t &new_p_inode,
                                   const uint64_t &inode,
                                   const std::string &new_name) {
    RenameRequest req;
    req.set_old_p_inode(old_p_inode);
    req.set_new_p_inode(new_p_inode);
    req.set_inode(inode);
    req.set_new_name(new_name);
    google::protobuf::Empty resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.renamefile(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "renamefile() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        return Status::OK();
    }
    return Status::IOError("rename_file() failed after retries");
}

Status MetadataClient::rename_dir(const uint64_t &old_p_inode,
                                  const uint64_t &new_p_inode,
                                  const uint64_t &inode,
                                  const std::string &new_name) {
    RenameRequest req;
    req.set_old_p_inode(old_p_inode);
    req.set_new_p_inode(new_p_inode);
    req.set_inode(inode);
    req.set_new_name(new_name);
    google::protobuf::Empty resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.renamedir(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "renamedir() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        return Status::OK();
    }
    return Status::IOError("rename_dir() failed after retries");
}

// setattr → RPC
Status MetadataClient::setattr(const Attributes &attr) {
    Attributes req = attr;
    Attributes resp;

    for (int tries = 0; tries < kMaxRetries; ++tries) {
        braft::PeerId leader;
        if (!pick_leader(&leader)) {
            usleep(kRetryBackoffUs);
            continue;
        }
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_timeout_ms(kTimeoutMs);

        if (channel.Init(leader.addr, nullptr) != 0) {
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        MetadataService_Stub stub(&channel);
        stub.setattr(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(WARNING) << "setattr() to " << leader
                         << " failed: " << cntl.ErrorText();
            braft::rtb::update_leader(kMetadataGroup, braft::PeerId());
            usleep(kRetryBackoffUs);
            continue;
        }
        return Status::OK();
    }
    return Status::IOError("setattr() failed after retries");
}

std::pair<Status, ChunksLocation> MetadataClient::get_chunks(const uint64_t &inode) {
    ChunksRequest req;
    req.set_inode(inode);
    ChunksLocation resp;
    brpc::Controller cntl;
    stub_->getchunks(&cntl, &req, &resp, nullptr);
    if (cntl.Failed())
        return {Status::IOError(cntl.ErrorText()), ChunksLocation()};
    return {Status::OK(), resp};
}
