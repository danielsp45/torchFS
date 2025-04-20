#define FUSE_USE_VERSION 31

#include "storage_engine.h"
#include "spdlog/spdlog.h"
#include "torch_fuse.h"

int main(int argc, char *argv[]) {
    enum { MAX_ARGS = 10 };
    int i, new_argc;
    char *new_argv[MAX_ARGS];
    // Default log level
    std::string log_level_str = "info";

    umask(0);
    int fill_dir_plus;
    for (i = 0, new_argc = 0; (i < argc) && (new_argc < MAX_ARGS); i++) {
        // Handle log level argument
        if (strncmp(argv[i], "--log_level=", sizeof("--log_level=") - 1) == 0) {
            log_level_str = std::string(argv[i] + (sizeof("--log_level=") - 1));
        } else if (!strcmp(argv[i], "--plus")) {
            fill_dir_plus = FUSE_FILL_DIR_PLUS;
        } else {
            new_argv[new_argc++] = argv[i];
        }
    }

    std::string local_path = "/home/vagrant/server";
    StorageEngine *se = new StorageEngine(local_path);

    // Set global log level based on argument
    {
        spdlog::level::level_enum level;
        bool invalid = false;
        if (log_level_str == "trace") {
            level = spdlog::level::trace;
        } else if (log_level_str == "debug") {
            level = spdlog::level::debug;
        } else if (log_level_str == "info") {
            level = spdlog::level::info;
        } else if (log_level_str == "warn" || log_level_str == "warning") {
            level = spdlog::level::warn;
        } else if (log_level_str == "error" || log_level_str == "err") {
            level = spdlog::level::err;
        } else if (log_level_str == "critical") {
            level = spdlog::level::critical;
        } else if (log_level_str == "off") {
            level = spdlog::level::off;
        } else {
            level = spdlog::level::info;
            invalid = true;
        }
        spdlog::set_level(level);
        if (invalid) {
            spdlog::error("Invalid log level '{}', defaulting to 'info'", log_level_str);
        }
    }

    int ret = fuse_main(new_argc, new_argv, &torch_oper, se);

    delete se;
    return 0;
}
