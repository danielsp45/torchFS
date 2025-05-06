#include "storage_engine.h"
#include "torch_fuse.h"

int main(int argc, char *argv[]) {
    enum { MAX_ARGS = 10 };
    int i, new_argc;
    char *new_argv[MAX_ARGS];

    umask(0);
    int fill_dir_plus;
    for (i = 0, new_argc = 0; (i < argc) && (new_argc < MAX_ARGS); i++) {
        if (!strcmp(argv[i], "--plus")) {
            fill_dir_plus = FUSE_FILL_DIR_PLUS;
        } else {
            new_argv[new_argc++] = argv[i];
        }
    }

    std::string local_path = "/home/vagrant/server";
    StorageEngine *se = new StorageEngine(local_path);

    int ret = fuse_main(new_argc, new_argv, &torch_oper, se);

    delete se;
    return 0;
}
