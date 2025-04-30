#include "util.h"
#include <sstream> // Include for std::istringstream
#include <string>
#include <vector>
#include <iostream>

std::pair<std::string, std::string>
split_path_from_target(const std::string &path) {
    // Treat both empty string and "/" as referring to root.
    if (path.empty() || path == "/") {
        return {"/", ""};
    }

    // Optional: Trim any trailing slash (but not if it's the only character)
    std::string trimmed = path;
    while (trimmed.size() > 1 && trimmed.back() == '/') {
        trimmed.pop_back();
    }

    size_t pos = trimmed.find_last_of('/');
    if (pos == std::string::npos) {
        // No separator found: assume current directory
        return {".", trimmed};
    }
    std::string dir = (pos == 0) ? "/" : trimmed.substr(0, pos);
    std::string file = trimmed.substr(pos + 1);
    return {dir, file};
}

std::vector<std::string> split_path(const std::string &path) {
    std::vector<std::string> result;
    if (path.empty() || path == "/") {
        return result; // Return an empty vector for root or empty path
    }

    std::istringstream iss(path);
    std::string token;
    while (std::getline(iss, token, '/')) {
        if (!token.empty()) {
            result.push_back(token);
        }
    }

    return result;
}

std::string join_paths(const std::string &path1, const std::string &path2) {
    if (path1.empty()) {
        return path2;
    }
    if (path2.empty()) {
        return path1;
    }
    //path 1 is not "/" but ends in "/"
    if (!path1.compare("/")){
        return "/" + path2;
    }
    if (path1.back() == '/') {
        // remove the trailing slash from path1
        std::string p1 = path1;
        p1.pop_back();
        return p1 + path2;
    }
    return path1 + "/" + path2;
}

std::string filename(const std::string &path) {
    size_t pos = path.find_last_of('/');
    if (pos == std::string::npos) {
        return path; // No separator found: return the whole path
    }
    return path.substr(pos + 1);
}