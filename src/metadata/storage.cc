#include "storage.h"
#include <cstdint>
#include <iostream>
#include <string>
#include <sys/stat.h>

Status MetadataStorage::init() {
    // Set up options.
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    rocksdb::ColumnFamilyOptions cf_options;
    cf_options.comparator = rocksdb::BytewiseComparator();

    // Define the column families.
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    column_families.push_back(
        rocksdb::ColumnFamilyDescriptor("inode", cf_options));
    column_families.push_back(
        rocksdb::ColumnFamilyDescriptor("dentry", cf_options));
    column_families.push_back(
        rocksdb::ColumnFamilyDescriptor("nodes", cf_options));

    // Open (or create) the DB.
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
    rocksdb::Status status =
        rocksdb::DB::Open(options, db_path_, column_families, &handles, &db_);
    if (!status.ok() || db_ == nullptr) {
        std::cout << "[ERROR] Failed to open RocksDB: " << status.ToString()
                  << std::endl;
        return Status::IOError("Failed to open RocksDB: " + status.ToString());
    } else {
        std::cout << "[INFO] RocksDB opened successfully at /tmp/torchdb/"
                  << std::endl;
    }

    // Map the handles to the corresponding member variables.
    cf_inode_ = handles[1];
    cf_dentry_ = handles[2];
    cf_nodes_ = handles[3];

    // 1) Verify on‐disk column families exactly match what we expect.
    std::vector<std::string> existing_cfs;
    rocksdb::Status st = rocksdb::DB::ListColumnFamilies(
        rocksdb::DBOptions(), db_path_, &existing_cfs);
    if (!st.ok()) {
        std::cerr << "[ERROR] Cannot list column families: " << st.ToString()
                  << std::endl;
        return Status::IOError("ListColumnFamilies failed: " + st.ToString());
    }
    std::set<std::string> seen(existing_cfs.begin(), existing_cfs.end());
    std::set<std::string> want = {rocksdb::kDefaultColumnFamilyName, "inode",
                                  "dentry", "nodes"};
    if (seen != want) {
        std::cerr << "[ERROR] CF mismatch: on-disk:";
        for (auto &n : existing_cfs)
            std::cerr << " " << n;
        std::cerr << "  expected:";
        for (auto &n : want)
            std::cerr << " " << n;
        std::cerr << std::endl;
        return Status::IOError(
            "ColumnFamily set on disk does not match schema");
    }
    std::cout << "[INFO] Column families on disk are exactly as expected\n";

    // 2) Initialize inode counter if missing.
    std::string value;
    rocksdb::ReadOptions ro;
    st = db_->Get(ro, cf_inode_, "_counter", &value);
    if (st.IsNotFound()) {
        rocksdb::WriteOptions wo;
        st = db_->Put(wo, cf_inode_, "_counter", "1");
        if (!st.ok()) {
            std::cerr << "[ERROR] Failed to create inode counter: "
                      << st.ToString() << std::endl;
            return Status::IOError("Failed to create inode counter: " +
                                   st.ToString());
        }
        std::cout << "[INFO] Inode counter initialized to 1\n";
    } else if (!st.ok()) {
        std::cerr << "[ERROR] Failed to get inode counter: " << st.ToString()
                  << std::endl;
        return Status::IOError("Failed to get inode counter: " + st.ToString());
    } else {
        std::cout << "[INFO] Inode counter exists: " << value << "\n";
    }

    // 3) Ensure root inode = 1 exists.
    st = db_->Get(ro, cf_inode_, std::to_string(1), &value);
    if (st.IsNotFound()) {
        auto [s2, attr] = create_dir(0, "/");
        if (!s2.ok()) {
            std::cerr << "[ERROR] Failed to create root dir: " << s2.ToString()
                      << std::endl;
            return s2;
        }
        std::cout << "[INFO] Root directory created with inode=" << attr.inode()
                  << "\n";
    } else if (!st.ok()) {
        std::cerr << "[ERROR] Failed to get root inode: " << st.ToString()
                  << std::endl;
        return Status::IOError("Failed to get root inode: " + st.ToString());
    } else {
        std::cout << "[INFO] Root inode exists\n";
    }

    return Status::OK();
}

std::pair<Status, Attributes> MetadataStorage::getattr(const uint64_t &inode) {
    // Get the file from the inode column family
    std::string value;
    rocksdb::ReadOptions read_options;
    rocksdb::Slice key(std::to_string(inode));
    if (cf_inode_ == nullptr) {
        std::cerr << "[ERROR] cf_inode_ is null" << std::endl;
        return {Status::IOError("cf_inode_ is null"), Attributes()};
    }
    rocksdb::Status status = db_->Get(read_options, cf_inode_, key, &value);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            return {Status::NotFound("File not found"), Attributes()};
        }
        return {
            Status::IOError("Failed to get file info: " + status.ToString()),
            Attributes()};
    }

    // Deserialize the value into an Attributes object
    Attributes attr; // Use the namespace specified in your .proto file
    if (!attr.ParseFromString(value)) {
        throw std::runtime_error("Failed to deserialize Attributes");
    }

    return {Status::OK(), attr};
}

std::pair<Status, std::vector<Dirent>>
MetadataStorage::readdir(const uint64_t &inode) {
    const std::string prefix = std::to_string(inode) + ":";
    std::vector<Dirent> dirents;

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;

    auto [s, attr] = getattr(inode);
    if (!s.ok()) {
        std::cout << "Failed to get attributes for inode: " << inode
                  << std::endl;
        return {s, {}};
    }

    auto it = std::unique_ptr<rocksdb::Iterator>(
        db_->NewIterator(read_options, cf_dentry_));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix))
            break;
        Dirent d;
        if (!d.ParseFromString(it->value().ToString())) {
            return {Status::IOError("Failed to deserialize Dirent"), {}};
        }
        dirents.push_back(std::move(d));
    }
    return {Status::OK(), std::move(dirents)};
}

std::pair<Status, FileInfo> MetadataStorage::open(const uint64_t &inode) {
    // Get the file info from the nodes column family
    std::string value;
    rocksdb::ReadOptions read_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Get(read_options, cf_inode_, key, &value);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            return {Status::NotFound("File not found"), FileInfo()};
        }
        return {
            Status::IOError("Failed to get file info: " + status.ToString()),
            FileInfo()};
    }

    // Deserialize the value into a FileInfo object
    FileInfo file_info;
    if (!file_info.ParseFromString(value)) {
        throw std::runtime_error("Failed to deserialize FileInfo");
    }

    return {Status::OK(), file_info};
}

std::pair<Status, Attributes>
MetadataStorage::create_file(const uint64_t &p_inode, const std::string &name) {
    Attributes attr;
    uint64_t inode = get_and_increment_counter();
    attr.set_inode(inode);
    attr.set_size(0);
    attr.set_path(name);
    attr.set_creation_time(time(nullptr));
    attr.set_modification_time(time(nullptr));
    attr.set_access_time(time(nullptr));
    attr.set_user_id(0);           // Set the user ID as needed
    attr.set_group_id(0);          // Set the group ID as needed
    attr.set_mode(S_IFREG | 0644); // Set the file mode (e.g., 0644)

    std::string value;
    if (!attr.SerializeToString(&value)) {
        return {Status::IOError("Failed to serialize Attributes"), attr};
    }

    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(attr.inode()));
    rocksdb::Status status = db_->Put(write_options, cf_inode_, key, value);
    if (!status.ok()) {
        return {Status::IOError("Failed to create file inode: " +
                                status.ToString()),
                attr};
    }

    // Create a new directory entry in the dentry column family
    Dirent dirent;
    dirent.set_name(name);
    dirent.set_inode(attr.inode());

    if (!dirent.SerializeToString(&value)) {
        return {Status::IOError("Failed to serialize Dirent"), attr};
    }

    key = rocksdb::Slice(std::to_string(p_inode) + ":" + name);
    status = db_->Put(write_options, cf_dentry_, key, value);
    if (!status.ok()) {
        return {Status::IOError("Failed to create directory entry: " +
                                status.ToString()),
                attr};
    }

    // TODO: Also create a new entry in the nodes column family

    return {Status::OK(), attr};
}

std::pair<Status, Attributes>
MetadataStorage::create_dir(const uint64_t &p_inode, const std::string &name) {
    Attributes attr;
    uint64_t inode = get_and_increment_counter();
    attr.set_inode(inode);
    attr.set_size(4096);
    attr.set_path(name);
    attr.set_creation_time(time(nullptr));
    attr.set_modification_time(time(nullptr));
    attr.set_access_time(time(nullptr));
    attr.set_user_id(0);           // Set the user ID as needed
    attr.set_group_id(0);          // Set the group ID as needed
    attr.set_mode(S_IFDIR | 0644); // Set the directory mode (e.g., 0755)

    std::string value;
    if (!attr.SerializeToString(&value)) {
        return {Status::IOError("Failed to serialize Attributes"), attr};
    }

    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(attr.inode()));
    rocksdb::Status status = db_->Put(write_options, cf_inode_, key, value);
    if (!status.ok()) {
        return {
            Status::IOError("Failed to create directory: " + status.ToString()),
            attr};
    }

    // if the p_inode is 0, it means this is the root directory
    // and we don't need to create a directory entry
    if (p_inode != 0) {
        Dirent dirent;
        dirent.set_name(name);
        dirent.set_inode(attr.inode());

        if (!dirent.SerializeToString(&value)) {
            return {Status::IOError("Failed to serialize Dirent"), attr};
        }

        key = rocksdb::Slice(std::to_string(p_inode) + ":" + name);
        status = db_->Put(write_options, cf_dentry_, key, value);
        if (!status.ok()) {
            return {Status::IOError("Failed to create directory entry: " +
                                    status.ToString()),
                    attr};
        }
    }

    return {Status::OK(), attr};
}

Status MetadataStorage::remove_file(const uint64_t &p_inode,
                                    const uint64_t &inode,
                                    const std::string &name) {
    // Remove the file from the inode column family
    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Delete(write_options, cf_inode_, key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove file: " + status.ToString());
    }

    // Remove the directory entry from the dentry column family
    std::string dentry_key = std::to_string(p_inode) + ":" + name;
    status = db_->Delete(write_options, cf_dentry_, dentry_key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove directory entry: " +
                               status.ToString());
    }

    // TODO: Also remove the file in the nodes column family

    return Status::OK();
}

Status MetadataStorage::remove_dir(const uint64_t &p_inode,
                                   const uint64_t &inode,
                                   const std::string &name) {
    // Remove the directory from the inode column family
    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Delete(write_options, cf_inode_, key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove directory: " +
                               status.ToString());
    }

    // Remove the directory entry from the dentry column family
    std::string dentry_key = std::to_string(p_inode) + ":" + name;
    status = db_->Delete(write_options, cf_dentry_, dentry_key);
    if (!status.ok()) {
        return Status::IOError("Failed to remove directory entry: " +
                               status.ToString());
    }

    return Status::OK();
}

Status MetadataStorage::rename_file(const uint64_t &old_p_inode,
                                    const uint64_t &new_p_inode,
                                    const uint64_t &inode,
                                    const std::string &new_name) {

    // go to inode entry and change the name of the file
    auto [s, attr] = getattr(inode);
    if (!s.ok()) {
        return s;
    }
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
    auto [s, attr] = getattr(inode);
    if (!s.ok()) {
        return s;
    }
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

Status MetadataStorage::setattr(const uint64_t &inode, const Attributes &attr) {
    // Update the file attributes in the inode column family
    std::string value;
    if (!attr.SerializeToString(&value)) {
        return Status::IOError("Failed to serialize Attributes");
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Slice key(std::to_string(inode));
    rocksdb::Status status = db_->Put(write_options, cf_inode_, key, value);
    if (!status.ok()) {
        return Status::IOError("Failed to update file attributes: " +
                               status.ToString());
    }

    return Status::OK();
}

uint64_t MetadataStorage::get_and_increment_counter() {
    // read the current counter, store that value, and increment it
    rocksdb::ReadOptions read_options;
    std::string value;
    rocksdb::Slice key("_counter");
    rocksdb::Status status = db_->Get(read_options, cf_inode_, key, &value);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            return 0;
        }
        throw std::runtime_error("Failed to get counter: " + status.ToString());
    }
    uint64_t counter = std::stoull(value);
    uint64_t new_counter = counter + 1;
    // write the new counter back to the database
    rocksdb::WriteOptions write_options;
    status =
        db_->Put(write_options, cf_inode_, key, std::to_string(new_counter));
    if (!status.ok()) {
        throw std::runtime_error("Failed to increment counter: " +
                                 status.ToString());
    }
    return counter;
}