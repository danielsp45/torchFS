#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

#include "TorchFuse.h"

// Helper: build a full path from root_ and a relative path.
// Assumes that 'path' always starts with '/'.
static std::string make_full_path(const std::string &root, const char *path) {
    // If root does not end with '/', add it.
    std::string r = root;
    if (!r.empty() && r.back() != '/')
        r += "/";
    return r + (path[0] == '/' ? path + 1 : path);
}

// Constructor: store the root directory for passthrough.
TorchFuse::TorchFuse(const std::string &root) : root_(root) {
    // Optionally ensure root_ ends with '/'
    if (!root_.empty() && root_.back() != '/')
        root_ += "/";
}

/*
 * Creates files on the underlying file system in response to a FUSE_MKNOD
 * operation
 */
static int mknod_wrapper(int dirfd, const char *path, const char *link,
                         int mode, dev_t rdev) {
    int res;

    if (S_ISREG(mode)) {
        res = ::openat(dirfd, path, O_CREAT | O_EXCL | O_WRONLY, mode);
        if (res >= 0)
            res = ::close(res);
    } else if (S_ISDIR(mode)) {
        res = ::mkdirat(dirfd, path, mode);
    } else if (S_ISLNK(mode) && link != NULL) {
        res = ::symlinkat(link, dirfd, path);
    } else if (S_ISFIFO(mode)) {
        res = ::mkfifoat(dirfd, path, mode);
#ifdef __FreeBSD__
    } else if (S_ISSOCK(mode)) {
        struct sockaddr_un su;
        int fd;

        if (strlen(path) >= sizeof(su.sun_path)) {
            errno = ENAMETOOLONG;
            return -1;
        }
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd >= 0) {
            /*
             * We must bind the socket to the underlying file
             * system to create the socket file, even though
             * we'll never listen on this socket.
             */
            su.sun_family = AF_UNIX;
            strncpy(su.sun_path, path, sizeof(su.sun_path));
            res = bindat(dirfd, fd, (struct sockaddr *)&su, sizeof(su));
            if (res == 0)
                close(fd);
        } else {
            res = -1;
        }
#endif
    } else {
        res = ::mknodat(dirfd, path, mode, rdev);
    }

    return res;
}

// Destructor.
TorchFuse::~TorchFuse() {
    // Nothing specific to do here.
}

int TorchFuse::getattr(const char *path, struct stat *stbuf,
                       struct fuse_file_info *fi) {
    (void)fi;
    int res;

    res = ::lstat(path, stbuf);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::access(const char *path, int mask) {
    int res;

    res = ::access(path, mask);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::readlink(const char *path, char *buf, size_t size) {
    int res = ::readlink(path, buf, size - 1);
    if (res == -1)
        return -errno;

    buf[res] = '\0';
    return 0;
}

int TorchFuse::readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi) {

    DIR *dp;
    struct dirent *de;

    (void)offset;
    (void)fi;

    dp = ::opendir(path);
    if (dp == NULL)
        return -errno;

    while ((de = ::readdir(dp)) != NULL) {
        struct stat st;
        ::memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
        if (filler(buf, de->d_name, &st, 0,
                   static_cast<fuse_fill_dir_flags>(FUSE_FILL_DIR_PLUS)))
            break;
    }

    ::closedir(dp);
    return 0;
}

int TorchFuse::mknod(const char *path, mode_t mode, dev_t rdev) {
    int res;

    res = ::mknod_wrapper(AT_FDCWD, path, NULL, mode, rdev);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::unlink(const char *path) {
    int res = ::unlink(path);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::mkdir(const char *path, mode_t mode) {
    int res = ::mkdir(path, mode);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::rmdir(const char *path) {
    int res = ::rmdir(path);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::symlink(const char *from, const char *to) {
    int res = ::symlink(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::rename(const char *from, const char *to, unsigned int flags) {
    int res;
    if (flags)
        return -EINVAL;

    res = ::rename(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::link(const char *from, const char *to) {
    int res = ::link(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::chmod(const char *path, mode_t mode, struct fuse_file_info *fi) {
    (void)fi;
    int res = ::chmod(path, mode); // call global chmod
    if (res == -1)
        return -errno;
    return 0;
}

int TorchFuse::chown(const char *path, uid_t uid, gid_t gid,
                     struct fuse_file_info *fi) {
    (void)fi;
    int res = lchown(path, uid, gid);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::truncate(const char *path, off_t size,
                        struct fuse_file_info *fi) {
    int res;
    if (fi != NULL)
        res = ftruncate(fi->fh, size);
    else
        res = ::truncate(path, size); // call global truncate
    if (res == -1)
        return -errno;
    return 0;
}

int TorchFuse::create(const char *path, mode_t mode,
                      struct fuse_file_info *fi) {
    int res = ::open(path, fi->flags, mode); // call global open
    if (res == -1)
        return -errno;
    fi->fh = res;
    return 0;
}

int TorchFuse::open(const char *path, struct fuse_file_info *fi) {
    int res = ::open(path, fi->flags); // use global open
    if (res == -1)
        return -errno;
    fi->fh = res;
    return 0;
}

int TorchFuse::read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi) {
    int fd;
    int res;
    char *new_path = strdup(path);

    if (fi == NULL)
        fd = open(new_path, O_RDONLY);
    else
        fd = fi->fh;

    if (fd == -1)
        return -errno;

    res = pread(fd, buf, size, offset);
    if (res == -1)
        res = -errno;

    if (fi == NULL)
        close(fd);

    return res;
}

int TorchFuse::write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi) {
    int fd;
    int res;
    if (fi == NULL)
        fd = ::open(path, O_WRONLY); // call global open
    else
        fd = fi->fh;

    if (fd == -1)
        return -errno;

    res = pwrite(fd, buf, size, offset);
    if (res == -1)
        res = -errno;

    if (fi == NULL)
        ::close(fd);
    return res;
}

int TorchFuse::statfs(const char *path, struct statvfs *stbuf) {
    int res = statvfs(path, stbuf);
    if (res == -1)
        return -errno;

    return 0;
}

int TorchFuse::release(const char *path, struct fuse_file_info *fi) {
    return close(fi->fh);
}

int TorchFuse::fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi) {
    (void)path;
    if (isdatasync == 0) {
        return ::fsync(fi->fh);
    }
    return ::fdatasync(fi->fh);
}

int TorchFuse::lseek(const char *path, off_t offset, int whence,
                     struct fuse_file_info *fi) {
    int fd;
    off_t res;
    if (fi == NULL)
        fd = ::open(path, O_RDONLY);
    else
        fd = fi->fh;

    if (fd == -1)
        return -errno;

    res = ::lseek(fd, offset, whence); // use 'offset', not 'off'
    if (res == -1)
        res = -errno;

    if (fi == NULL)
        ::close(fd);
    return res;
}
