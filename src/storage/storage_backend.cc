#include "storage_backend.h"
#include <cstdio> // for std::remove
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

StorageBackend::StorageBackend(const std::string &mount_path)
    : mount_path_(mount_path) {}

std::string StorageBackend::get_chunk_path(const std::string &chunk_id) const {
    return mount_path_ + "/chunk-" + chunk_id;
}

std::pair<Status, Data>
StorageBackend::read_chunk(const std::string &chunk_id) {
    std::string path = get_chunk_path(chunk_id);
    Data result;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return {Status::NotFound("Open failed: " + path + " (" +
                                 strerror(errno) + ")"),
                result};
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        ::close(fd);
        return {Status::IOError("fstat failed: " + path + " (" +
                                strerror(errno) + ")"),
                result};
    }

    size_t filesize = st.st_size;
    std::string buf(filesize, '\0');
    ssize_t bytes_read = ::pread(fd, buf.data(), filesize, 0);
    ::close(fd);

    if (bytes_read < 0) {
        return {Status::IOError("Pread failed: " + path + " (" +
                                strerror(errno) + ")"),
                result};
    }

    std::cout << "READ " << bytes_read << " bytes from chunk: " << chunk_id
              << std::endl;
    result.set_payload(buf);
    result.set_len(static_cast<size_t>(bytes_read));

    return {Status::OK(), result};
}

std::pair<Status, size_t>
StorageBackend::write_chunk(const std::string &chunk_id, const Data &data) {
    std::string path = get_chunk_path(chunk_id);
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        return {Status::IOError("Open failed: " + path + " (" +
                                strerror(errno) + ")"),
                0};
    }

    ssize_t bytes_written =
        ::pwrite(fd, data.payload().c_str(), data.len(), 0);
    ::close(fd);

    std::cout << "WROTE bytes: " << bytes_written << std::endl;

    if (bytes_written != static_cast<ssize_t>(data.len())) {
        return {Status::IOError("Pwrite failed: " + path + " (" +
                                strerror(errno) + ")"),
                0};
    }

    return {Status::OK(), static_cast<size_t>(bytes_written)};
}

Status StorageBackend::remove_chunk(const std::string &chunk_id) {
    std::string path = get_chunk_path(chunk_id);
    // Try unlink; if file doesn’t exist (errno == ENOENT) -> success
    if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
        return Status::IOError("Unlink failed: " + path + " (" +
                               std::string(strerror(errno)) + ")");
    }
    return Status::OK();
}
