#pragma once
#include <string>
#include <unordered_map>
#include <memory>
#include "file_handle.h"

class IEvictionPolicy;

class Cache {
    public:
        Cache(std::unique_ptr<IEvictionPolicy> policy);
        std::shared_ptr<FileHandle> lookup(const std::string &key);
        void insert(const std::string &key, std::shared_ptr<FileHandle> value);

    private:
        size_t capacity_;
        std::unordered_map<std::string, std::shared_ptr<FileHandle>> index_;
        std::unique_ptr<IEvictionPolicy> policy_;
};
