#pragma once
#include "../eviction_policy.h"
#include <list>
#include <unordered_map>
#include <string>

/**
 * LEAST RECENTLY USED eviction policy
 * Evicts the element that hasn't been used in the longest time
 */
class LRUEvictionPolicy : public IEvictionPolicy {
    public:
        void insert(const uint64_t &key) override { // O(1)
            usage_.push_front(key);
            key_ptrs_[key] = usage_.begin();
        }

        void remove(const uint64_t &key) override { // O(1)
            auto it = key_ptrs_.find(key);
            if (it == key_ptrs_.end()) return;
            usage_.erase(it->second);
            key_ptrs_.erase(it);
        }

        void update(const uint64_t &key) override { // O(1)
            auto it = key_ptrs_.find(key);
            if (it == key_ptrs_.end()) return;
            usage_.erase(it->second);
            usage_.push_front(key);
            it->second = usage_.begin();
        }

        uint64_t evict() override { // O(1)
            if (usage_.empty()) return -1;
            uint64_t lru_key = usage_.back();
            usage_.pop_back();
            key_ptrs_.erase(lru_key);
            return lru_key;
        }

    private:
        std::list<uint64_t> usage_;
        std::unordered_map<uint64_t, std::list<uint64_t>::iterator> key_ptrs_;
};
