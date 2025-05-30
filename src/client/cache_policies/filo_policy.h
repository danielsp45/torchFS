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
        void insert(const std::string &key) override { // O(1)
            stack_.push_back(key);
        }

        void remove(const std::string &key) override { // O(N) (Not used often)
            auto it = std::find(stack_.begin(), stack_.end(), key);
            if (it != stack_.end()) {
                stack_.erase(it);
            }
        }
        
        void update(const std::string &/*key*/) override {
            // FILO doesn't need to update or reorder elements
        }
        
        std::string evict() override { // O(1)
            if (stack_.empty()) { 
                return "";
            }
            std::string key = stack_.back();
            stack_.pop_back();
            return key;
        }

    private:
        std::list<std::string> stack_;
};
