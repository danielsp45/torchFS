#include "config.h"
#define FUSE_USE_VERSION 31

#include "fuse_wrapper.h"
#include "options.h"
#include "status.h"
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

  TorchFuse *fs = new TorchFuse("/mnt/fs");
  Options *options = parse_options(argc, argv);
  if (fs->init() != Status::OK()) {
    fprintf(stderr, "Failed to initialize TorchFuse\n");
    delete fs;
    return -1;
  }

  // Call fuse_main with the modified arguments and fs as private_data.
  int ret = fuse_main(new_argc, new_argv, &torchfs_oper, fs);

  delete fs;
  return 0;
}
