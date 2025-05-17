#include "metadata.pb.h"
#include "state_machine.h"
#include "storage.h"
#include <brpc/server.h>
#include <google/protobuf/empty.pb.h>

class MetadataServiceImpl : public MetadataService {
  public:
    MetadataServiceImpl(KVStore *kv_store) : kv_store_(kv_store) {}
    virtual ~MetadataServiceImpl() {}

    // RPC method declarations
    void getattr(::google::protobuf::RpcController *cntl,
                 const ::InodeRequest *request, ::Attributes *response,
                 ::google::protobuf::Closure *done);
    void readdir(google::protobuf::RpcController *cntl,
                 const ReadDirRequest *request, ReadDirResponse *response,
                 google::protobuf::Closure *done);
    void createfile(google::protobuf::RpcController *cntl,
                    const CreateRequest *request, Attributes *response,
                    google::protobuf::Closure *done);
    void setattr(::google::protobuf::RpcController *cntl,
                 const ::Attributes *request, ::Attributes *response,
                 ::google::protobuf::Closure *done);
    void createdir(::google::protobuf::RpcController *cntl,
                   const ::CreateRequest *request, ::Attributes *response,
                   ::google::protobuf::Closure *done);
    void removefile(::google::protobuf::RpcController *cntl,
                    const ::RemoveRequest *request,
                    ::google::protobuf::Empty *response,
                    ::google::protobuf::Closure *done);
    void removedir(::google::protobuf::RpcController *cntl,
                   const ::RemoveRequest *request,
                   ::google::protobuf::Empty *response,
                   ::google::protobuf::Closure *done);
    void renamefile(::google::protobuf::RpcController *cntl,
                    const ::RenameRequest *request,
                    ::google::protobuf::Empty *response,
                    ::google::protobuf::Closure *done);
    void renamedir(::google::protobuf::RpcController *cntl,
                   const ::RenameRequest *request,
                   ::google::protobuf::Empty *response,
                   ::google::protobuf::Closure *done);
    void open(::google::protobuf::RpcController *cntl,
              const ::InodeRequest *request, ::FileInfo *response,
              ::google::protobuf::Closure *done);

  private:
    KVStore *kv_store_; // Pointer to the KVStore instance
};