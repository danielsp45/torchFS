#pragma once
#include <string>

class IEvictionPolicy {
public:
    virtual ~IEvictionPolicy() = default;
    virtual void insert(const std::string &key) = 0;
    virtual void remove(const std::string &key) = 0;
    virtual void update(const std::string &key) = 0;
    virtual std::string evict() = 0;
};
