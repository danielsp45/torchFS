#pragma once
#include "../eviction_policy.h"
#include <list>
#include <algorithm>

/**
 * FIRST IN FIRST OUT eviction policy
 * Evicts the oldest element inserted
 */
class FIFOEvictionPolicy : public IEvictionPolicy {
    public:
        void insert(const uint64_t &key) override { // O(1)
            queue_.push_back(key);
        }

        void remove(const uint64_t &key) override { // O(N) (Not used often)
            auto it = std::find(queue_.begin(), queue_.end(), key);
            if (it != queue_.end()) {
                queue_.erase(it);
            }
        }

        void update(const uint64_t &/*key*/) override {
            // FIFO doesn't reorder on access
        }

        uint64_t evict() override { // O(1)
            if (queue_.empty()) {
                return -1;
            }
            uint64_t key = queue_.front();
            queue_.pop_front();
            return key;
        }

    private:
        std::list<uint64_t> queue_;
};
