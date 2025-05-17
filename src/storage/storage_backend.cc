#include "storage_backend.h"
#include "slice.h"
#include <fstream>
#include <algorithm>
#include <cstdio> // for std::remove
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>

StorageBackend::StorageBackend(const std::string &mount_path)
    : mount_path_(mount_path) {}

std::string StorageBackend::get_file_path(const std::string &file_id) const {
    return mount_path_ + "/storage" + file_id; 
}

std::pair<Status, Data> StorageBackend::read(const std::string &file_id, off_t offset, size_t size) {
    std::string path = get_file_path(file_id);
    Data result;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return { Status::NotFound("Open failed: " + path + " (" + strerror(errno) + ")"), result };
    }
    
    std::string buf(size, '\0');
    ssize_t bytes_read = ::pread(fd, buf.data(), size, offset);
    ::close(fd);
    
    if (bytes_read < 0) {
        return { Status::IOError("Pread failed: " + path + " (" + strerror(errno) + ")"), result };
    }
    
    result.set_payload(buf);
    result.set_len(bytes_read);
    return { Status::OK(), result };
}

std::pair<Status, size_t> StorageBackend::write(const std::string &file_id, const Data &data, off_t offset) {
    std::string path = get_file_path(file_id);
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        return { Status::IOError("Open failed: " + path + " (" + strerror(errno) + ")"), 0 };
    }

    const std::string &buf = data.payload();
    ssize_t bytes_written = ::pwrite(fd, buf.data(), data.len(), offset);
    ::close(fd);

    if (bytes_written < 0) {
        return { Status::IOError("Pwrite failed: " + path + " (" + strerror(errno) + ")"), 0 };
    }

    return { Status::OK(), static_cast<size_t>(bytes_written) };
}

Status StorageBackend::remove_file(const std::string &file_id) {
    std::string path = get_file_path(file_id);
    // Try unlink; if file doesn’t exist (errno == ENOENT) -> success
    if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
        return Status::IOError("Unlink failed: " + path + " (" + std::string(strerror(errno)) + ")");
    }
    return Status::OK();
}
