#pragma once
#include "../eviction_policy.h"
#include <list>
#include <algorithm>

/**
 * FIRST IN LAST OUT eviction policy
 * Evicts the last element inserted
 */
class FILOEvictionPolicy : public IEvictionPolicy {
    public:
        void insert(const uint64_t &key) override { // O(1)
            stack_.push_back(key);
        }

        void remove(const uint64_t &key) override { // O(N) (Not used often)
            auto it = std::find(stack_.begin(), stack_.end(), key);
            if (it != stack_.end()) {
                stack_.erase(it);
            }
        }
        
        void update(const uint64_t &/*key*/) override {
            // FILO doesn't need to update or reorder elements
        }
        
        uint64_t evict() override { // O(1)
            if (stack_.empty()) { 
                return -1;
            }
            uint64_t key = stack_.back();
            stack_.pop_back();
            return key;
        }

    private:
        std::list<uint64_t> stack_;
};
