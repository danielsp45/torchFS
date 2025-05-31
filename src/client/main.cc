#include "storage_engine.h"
#include "torch_fuse.h"
#include <gflags/gflags.h>

DEFINE_string(cache_dir, "", "Directory for local storage");

int main(int argc, char *argv[]) {
    enum { MAX_ARGS = 10 };
    int i, new_argc;
    char *new_argv[MAX_ARGS];

    gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

    umask(0);
    for (i = 0, new_argc = 0; (i < argc) && (new_argc < MAX_ARGS); i++) {
        if (!strcmp(argv[i], "--plus")) {
            // Ignored --plus argument.
        } else {
            new_argv[new_argc++] = argv[i];
        }
    }

    StorageEngine *se = new StorageEngine(FLAGS_cache_dir);
    Status s = se->init();
    if (!s.ok()) {
        std::cerr << "Failed to initialize storage engine: " << s.ToString()
                  << std::endl;
        return -1;
    }

    fuse_main(new_argc, new_argv, &torch_oper, se);

    delete se;
    return 0;
}