#ifndef OPTIONS_H
#define OPTIONS_H

#include <cstddef>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>

struct Options {
  std::string mountpoint;
  bool debug_mode;

  Options() : mountpoint("/mnt/fs"), debug_mode(false) {}
};

Options parse_options(int argc, char *argv[]);

#endif // OPTIONS_H