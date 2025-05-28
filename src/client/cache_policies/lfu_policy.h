#pragma once
#include "../eviction_policy.h"
#include <list>
#include <sys/types.h>
#include <unordered_map>
#include <string>

/**
 * LEAST FREQUENTLY USED eviction policy
 * Always evicts the key with the lowest access frequency
 */
class LFUEvictionPolicy : public IEvictionPolicy {
    public:
        LFUEvictionPolicy() : minFreq_(0) {}

        void insert(const std::string &key) override { // O(1)
            minFreq_ = 1;
            freqBuckets_[1].push_back(key);
            keyData_[key] = {1, --freqBuckets_[1].end()};
        }

        void remove(const std::string &key) override { // O(1)
            auto kd = keyData_.find(key);
            if (kd == keyData_.end()) return;

            uint old_f = kd->second.freq;
            auto &bucket = freqBuckets_[old_f];
            bucket.erase(kd->second.it);
            
            if (bucket.empty()) {
                freqBuckets_.erase(old_f);
                if (minFreq_ == old_f) ++minFreq_;
            }
            keyData_.erase(kd);
        }
    
        void update(const std::string &key) override { // O(1)
            auto kd = keyData_.find(key);
            if (kd == keyData_.end()) return;

            uint old_f = kd->second.freq;
            auto &oldBucket = freqBuckets_[old_f];
            oldBucket.erase(kd->second.it);

            if (oldBucket.empty()) {
                freqBuckets_.erase(old_f);
                if (minFreq_ == old_f) ++minFreq_;
            }

            uint new_f = old_f + 1;
            freqBuckets_[new_f].push_back(key);
            kd->second = {new_f, --freqBuckets_[new_f].end()};
        }

        std::string evict() override { // O(1)
            if (keyData_.empty()) {
                return "";
            }
            auto &bucket = freqBuckets_[minFreq_];
            const std::string victim = bucket.front();
            bucket.pop_front();
            keyData_.erase(victim);
            if (bucket.empty()) {
                freqBuckets_.erase(minFreq_);
            }
            return victim;
        }

    private:
        struct KeyInfo {
            uint freq;
            std::list<std::string>::iterator it;
        };
        std::unordered_map<int, std::list<std::string>> freqBuckets_;
        std::unordered_map<std::string, KeyInfo> keyData_;
        uint minFreq_;
};
