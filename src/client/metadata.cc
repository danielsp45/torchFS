#include "metadata.h"

MetadataStorage::MetadataStorage() {
    // Initialize RocksDB or any other database connection here.
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/torchdb", &db_);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " +
                                 status.ToString());
    }

    rocksdb::ColumnFamilyOptions cf_options;
    cf_options.comparator = rocksdb::BytewiseComparator();

    // create the inode column family
    status = db_->CreateColumnFamily(cf_options, "inode", &cf_inode_);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create column family: " +
                                 status.ToString());
    }

    // create the dentry column family
    status = db_->CreateColumnFamily(cf_options, "dentry", &cf_dentry_);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create column family: " +
                                 status.ToString());
    }

    // create the nodes column family
    status = db_->CreateColumnFamily(cf_options, "nodes", &cf_nodes_);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create column family: " +
                                 status.ToString());
    }
}

Attributes MetadataStorage::getattr(const uint64_t &inode) {
    // Get the file from the inode column family
    std::string value;
    rocksdb::ReadOptions read_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Get(read_options, cf_inode_, key, &value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to get inode: " + status.ToString());
    }

    // Deserialize the value into an Attributes object
    Attributes attr; // Use the namespace specified in your .proto file
    if (!attr.ParseFromString(value)) {
        throw std::runtime_error("Failed to deserialize Attributes");
    }

    return attr;
}

std::vector<Dirent> MetadataStorage::readdir(const uint64_t &inode) {
    // Get the directory entries from the dentry column family
    std::string prefix = std::to_string(inode) + ":";
    std::vector<Dirent> dirents;

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;

    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(read_options, cf_dentry_));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
         it->Next()) {
        Dirent dirent;
        if (!dirent.ParseFromString(it->value().ToString())) {
            throw std::runtime_error("Failed to deserialize Dirent");
        }
        dirents.push_back(dirent);
    }

    return dirents;
}

FileInfo MetadataStorage::open(const uint64_t &inode) {
    // Get the file info from the nodes column family
    std::string value;
    rocksdb::ReadOptions read_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Get(read_options, cf_inode_, key, &value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to get file info: " +
                                 status.ToString());
    }

    // Deserialize the value into a FileInfo object
    FileInfo file_info;
    if (!file_info.ParseFromString(value)) {
        throw std::runtime_error("Failed to deserialize FileInfo");
    }

    return file_info;
}

Status MetadataStorage::create_file(const uint64_t &p_inode,
                                    const std::string &name) {
    Attributes attr;
    attr.set_inode(next_inode_++);
    attr.set_size(0);
    attr.set_path(name);
    attr.set_creation_time(time(nullptr));

    std::string value;
    if (!attr.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Attributes");
    }

    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(attr.inode()));
    rocksdb::Status status = db_->Put(write_options, cf_inode_, key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to create file: " + status.ToString());
    }

    // Create a new directory entry in the dentry column family
    Dirent dirent;
    dirent.set_name(name);
    dirent.set_inode(attr.inode());

    if (!dirent.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Dirent");
    }

    key = rocksdb::Slice(std::to_string(p_inode) + ":" + name);
    status = db_->Put(write_options, cf_dentry_, key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to create directory entry: " +
                               status.ToString());
    }

    // TODO: Also create a new entry in the nodes column family

    return Status::OK();
}

Status MetadataStorage::create_dir(const uint64_t &p_inode,
                                   const std::string &name) {
    Attributes attr;
    attr.set_inode(next_inode_++);
    attr.set_path(name);
    attr.set_creation_time(time(nullptr));

    std::string value;
    if (!attr.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Attributes");
    }

    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(attr.inode()));
    rocksdb::Status status = db_->Put(write_options, cf_inode_, key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to create directory: " +
                               status.ToString());
    }

    // Create a new directory entry in the dentry column family
    Dirent dirent;
    dirent.set_name(name);
    dirent.set_inode(attr.inode());

    if (!dirent.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Dirent");
    }

    key = rocksdb::Slice(std::to_string(p_inode) + ":" + name);
    status = db_->Put(write_options, cf_dentry_, key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to create directory entry: " +
                               status.ToString());
    }

    return Status::OK();
}

Status MetadataStorage::remove_file(const uint64_t &p_inode,
                                    const uint64_t &inode) {
    // Remove the file from the inode column family
    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Delete(write_options, cf_inode_, key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove file: " + status.ToString());
    }

    // Remove the directory entry from the dentry column family
    std::string dentry_key =
        std::to_string(p_inode) + ":" + std::to_string(inode);
    status = db_->Delete(write_options, cf_dentry_, dentry_key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove directory entry: " +
                               status.ToString());
    }

    // TODO: Also remove the file in the nodes column family

    return Status::OK();
}

Status MetadataStorage::remove_dir(const uint64_t &inode) {
    // Remove the directory from the inode column family
    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Delete(write_options, cf_inode_, key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove directory: " +
                               status.ToString());
    }

    // INFO: Since every file is removed before removing the directory, we don't
    // need to remove the dentry entry

    return Status::OK();
}

Status MetadataStorage::rename_file(const uint64_t &old_p_inode,
                                    const uint64_t &new_p_inode,
                                    const uint64_t &inode,
                                    const std::string &new_name) {

    // go to inode entry and change the name of the file
    Attributes attr = getattr(inode);
    attr.set_path(new_name);
    std::string value;
    if (!attr.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Attributes");
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Put(write_options, cf_inode_, key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to rename file: " + status.ToString());
    }

    // remove the old entry in the dentry column family
    std::string old_dentry_key =
        std::to_string(old_p_inode) + ":" + std::to_string(inode);
    status = db_->Delete(write_options, cf_dentry_, old_dentry_key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove old directory entry: " +
                               status.ToString());
    }
    // create a new entry in the dentry column family
    Dirent dirent;
    dirent.set_name(new_name);
    dirent.set_inode(inode);
    if (!dirent.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Dirent");
    }
    std::string new_dentry_key = std::to_string(new_p_inode) + ":" + new_name;
    status = db_->Put(write_options, cf_dentry_, new_dentry_key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to create new directory entry: " +
                               status.ToString());
    }
    return Status::OK();
}

Status MetadataStorage::rename_dir(const uint64_t &old_p_inode,
                                   const uint64_t &new_p_inode,
                                   const uint64_t &inode,
                                   const std::string &new_name) {
    // go to inode entry and change the name of the file
    Attributes attr = getattr(inode);
    attr.set_path(new_name);
    std::string value;
    if (!attr.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Attributes");
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Put(write_options, cf_inode_, key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to rename directory: " +
                               status.ToString());
    }

    // remove the old entry in the dentry column family
    std::string old_dentry_key =
        std::to_string(old_p_inode) + ":" + std::to_string(inode);
    status = db_->Delete(write_options, cf_dentry_, old_dentry_key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove old directory entry: " +
                               status.ToString());
    }
    // create a new entry in the dentry column family
    Dirent dirent;
    dirent.set_name(new_name);
    dirent.set_inode(inode);
    if (!dirent.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Dirent");
    }
    std::string new_dentry_key = std::to_string(new_p_inode) + ":" + new_name;
    status = db_->Put(write_options, cf_dentry_, new_dentry_key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to create new directory entry: " +
                               status.ToString());
    }

    return Status::OK();
}