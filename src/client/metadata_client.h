#pragma once

#include "metadata.pb.h"
#include "status.h"
#include <brpc/channel.h>
#include <cstdint>
#include <vector>

class MetadataClient {
  public:
    MetadataClient(const std::string &server_address = "127.0.0.1:8000");
    ~MetadataClient() = default;

    std::pair<Status, Attributes> getattr(const uint64_t &inode);
    std::pair<Status, std::vector<Dirent>> readdir(const uint64_t &inode);
    std::pair<Status, FileInfo> open(const uint64_t &inode);

    std::pair<Status, Attributes> create_file(const uint64_t &p_inode,
                                              const std::string &name);
    std::pair<Status, Attributes> create_dir(const uint64_t &p_inode,
                                             const std::string &name);

    Status remove_file(const uint64_t &p_inode, const uint64_t &inode,
                       const std::string &name);
    Status remove_dir(const uint64_t &p_inode, const uint64_t &inode,
                      const std::string &name);

    Status rename_file(const uint64_t &old_p_inode, const uint64_t &new_p_inode,
                       const uint64_t &inode, const std::string &new_name);
    Status rename_dir(const uint64_t &old_p_inode, const uint64_t &new_p_inode,
                      const uint64_t &inode, const std::string &new_name);

    Status setattr(const Attributes &attr);

  private:
    brpc::Channel channel_;
    MetadataService_Stub *stub_;
};