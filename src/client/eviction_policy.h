#pragma once
#include <cstdint>
#include <string>

class IEvictionPolicy {
public:
    virtual ~IEvictionPolicy() = default;
    virtual void insert(const uint64_t &inode) = 0;
    virtual void remove(const uint64_t &inode) = 0;
    virtual void update(const uint64_t &inode) = 0;
    virtual uint64_t evict() = 0;
};
