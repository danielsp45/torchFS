#ifndef UTIL_H
#define UTIL_H

#include <string>
#include <vector>

std::pair<std::string, std::string>
split_path_from_target(const std::string &path);

std::vector<std::string> split_path(const std::string &path);

std::string join_paths(const std::string &path1, const std::string &path2);

std::string filename(const std::string &path);

#endif // UTIL_H