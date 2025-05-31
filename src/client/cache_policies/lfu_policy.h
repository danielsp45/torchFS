#pragma once
#include "../eviction_policy.h"
#include <list>
#include <sys/types.h>
#include <unordered_map>
#include <string>

/**
 * LEAST FREQUENTLY USED eviction policy
 * Evicts the element with the lowest access frequency
 */
class LFUEvictionPolicy : public IEvictionPolicy {
    public:
        LFUEvictionPolicy() : min_freq_(0) {}

        void insert(const uint64_t &key) override { // O(1)
            min_freq_ = 1;
            freq_buckets_[1].push_back(key);
            key_data_[key] = {1, --freq_buckets_[1].end()};
        }

        void remove(const uint64_t &key) override { // O(1)
            auto kd = key_data_.find(key);
            if (kd == key_data_.end()) return;

            uint old_f = kd->second.freq;
            auto &bucket = freq_buckets_[old_f];
            bucket.erase(kd->second.it);
            
            if (bucket.empty()) {
                freq_buckets_.erase(old_f);
                if (min_freq_ == old_f) ++min_freq_;
            }
            key_data_.erase(kd);
        }
    
        void update(const uint64_t &key) override { // O(1)
            auto kd = key_data_.find(key);
            if (kd == key_data_.end()) return;

            uint old_f = kd->second.freq;
            auto &oldBucket = freq_buckets_[old_f];
            oldBucket.erase(kd->second.it);

            if (oldBucket.empty()) {
                freq_buckets_.erase(old_f);
                if (min_freq_ == old_f) ++min_freq_;
            }

            uint new_f = old_f + 1;
            freq_buckets_[new_f].push_back(key);
            kd->second = {new_f, --freq_buckets_[new_f].end()};
        }

        uint64_t evict() override { // O(1)
            if (key_data_.empty()) {
                return -1;
            }
            auto &bucket = freq_buckets_[min_freq_];
            const uint64_t victim = bucket.front();
            bucket.pop_front();
            key_data_.erase(victim);
            if (bucket.empty()) {
                freq_buckets_.erase(min_freq_);
            }
            return victim;
        }

    private:
        struct KeyInfo {
            uint freq;
            std::list<uint64_t>::iterator it;
        };
        std::unordered_map<int, std::list<uint64_t>> freq_buckets_;
        std::unordered_map<uint64_t, KeyInfo> key_data_;
        uint min_freq_;
};
