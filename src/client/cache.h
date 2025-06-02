#pragma once
#include <unordered_map>
#include <memory>
#include <shared_mutex>

#include "file_handle.h"

class IEvictionPolicy;

class Cache {
    public:
        Cache(std::unique_ptr<IEvictionPolicy> policy);
        std::shared_ptr<FileHandle> lookup(const uint64_t &inode);
        void insert(const uint64_t &inode, std::shared_ptr<FileHandle> value);

        int capacity() const;

    private:
        mutable std::shared_mutex mu_;

        size_t capacity_;
        std::unordered_map<uint64_t, std::shared_ptr<FileHandle>> index_;
        std::unique_ptr<IEvictionPolicy> policy_;
};
